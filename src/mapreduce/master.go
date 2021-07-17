package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"sync"

	common "github.com/drinkbeer/src/SimpleMapReduce/src/common"
)

const MasterName = "master"

/*
Master holds all the state that the master needs to keep track of.

sync.Mutex
https://gobyexample.com/mutexes
var mutex = &sync.Mutex{}
mutex.Lock()
mutex.Unlock()
*/
type Master struct {
	sync.Mutex

	address     string
	doneChannel chan bool

	// Protected by the mutex
	newCond *sync.Cond // Signals when Register() adds to workers[]
	workers []string   // Each worker's UNIX-domain socket name -- its RPC address

	// Per-task information
	jobName string   // Name of currently Register() adds to workers[]
	Files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

// newMaster initializes a new Map/Reduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.doneChannel = make(chan bool)
	mr.newCond = sync.NewCond(mr)
	mr.shutdown = make(chan struct{})
	return
}

/*
run executes a mapreduce job on the given number of mappers and reducers.

First, it divides up the input file among the given number of mappers, and
schedules each task on workers as they become available. Each map task bins
its output in a number of bins equal to the given number of reduce tasks.

When all tasks have been completed, the reducer outputs are merged,
statistics are collected, and the master is shut down.

Note that this implementation assumes a shared file system.
*/
func (mr *Master) run(jobName string, files []string, nReduce int, schedule func(phase common.JobPhase),
	finish func()) {
	mr.jobName = jobName
	mr.Files = files
	mr.nReduce = nReduce

	fmt.Printf("%s: Starting Map/Reduce task '%s'\n", mr.address, mr.jobName)

	schedule(common.MapPhase)
	schedule(common.ReducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed '%s'\n", mr.address, mr.jobName)

	mr.doneChannel <- true
}

// merge combines the results of the many reduce jobs into a single output file
// Implemented with merge sort
func (mr *Master) merge() {
	common.Debug("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		mergeName := common.MergeName(mr.jobName, i)
		fmt.Printf("merge: read %s \n", mergeName)
		mergeFile, err := os.Open(mergeName)
		if err != nil {
			log.Fatal("merge: ", err)
		}

		dec := json.NewDecoder(mergeFile)
		for {
			var kv common.KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		mergeFile.Close()
	}

	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mergeResultFile, err := os.Create(common.MergeResultName(mr.jobName))
	if err != nil {
		log.Fatal("merge: create ", err)
	}
	w := bufio.NewWriter(mergeResultFile)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	mergeResultFile.Close()
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// CleanupFiles cleans up all intermediate files produced by running mapreduce.
func (mr *Master) CleanupFiles() {
	for i := range mr.Files {
		for j := 0; j < mr.nReduce; j++ {
			common.RemoveFile(common.ReduceName(mr.jobName, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		common.RemoveFile(common.MergeName(mr.jobName, i))
	}
	common.RemoveFile(common.MergeResultName(mr.jobName))
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	nTasks := make([]int, 0, len(mr.workers))
	for _, wk := range mr.workers {
		common.Debug("Master: shutdown worker %s \n", wk)
		var reply common.ShutdownReply
		callResult := common.RPCCall(wk, "Worker.Shutdown", new(struct{}), &reply)
		if callResult == false {
			fmt.Printf("Master: RPC %s shutdown error \n", wk)
		} else {
			nTasks = append(nTasks, reply.Ntasks)
		}
	}
	return nTasks
}
