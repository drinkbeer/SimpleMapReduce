package mapreduce

import (
	"fmt"
	"github.com/drinkbeer/src/SimpleMapReduce/src/common"
	"log"
	"net"
	"sync"
	"time"
)

/*
Worker holds the state for a server waiting for DoTask or Shutdown RPCs.
*/
type Worker struct {
	sync.Mutex

	name        string
	Map         func(string, string) []common.KeyValue
	Reduce      func(string, []string) string
	nRPC        int // quit after this many RPCs; protected by mutex
	nTasks      int // total tasks executed; protected by mutex
	concurrent  int // number of parallel DoTasks in this worker; mutex
	l           net.Listener
	parallelism *Parallelism
}

/*
Parallelism is used to track whether workers executed in parallel.
*/
type Parallelism struct {
	mu  sync.Mutex
	now int32
	max int32
}

// DoTask is called by the master when a new task is being scheduled on this worker.
func (wk *Worker) DoTask(args *common.DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d) \n",
		wk.name, args.Phase, args.TaskNumber, args.File, args.NumOtherPhase)

	wk.Lock()
	wk.nTasks++
	wk.concurrent++
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	pause := false
	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now++
		if wk.parallelism.now > wk.parallelism.max {
			wk.parallelism.max = wk.parallelism.now
		}
		if wk.parallelism.max < 2 {
			pause = true
		}
		wk.parallelism.mu.Unlock()
	}

	if pause {
		// give other workers a chance to prove that they are executing in parallel
		time.Sleep(time.Second)
	}

	switch args.Phase {
	case common.MapPhase:
		common.DoMap(args.JobName, args.TaskNumber, args.File, args.NumOtherPhase, wk.Map)
	case common.ReducePhase:
		common.DoReduce(args.JobName, args.TaskNumber, common.MergeName(args.JobName, args.TaskNumber), args.NumOtherPhase, wk.Reduce)
	}

	wk.Lock()
	wk.concurrent--
	wk.Unlock()

	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now--
		wk.parallelism.mu.Unlock()
	}

	fmt.Printf("%s: %v task #%d done \n", wk.name, args.Phase, args.TaskNumber)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// We should response with the number of tasks this worker has processed.
func (wk *Worker) Shutdown(_ *struct{}, res *common.ShutdownReply) error {
	common.Debug("Shutdown %s \n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1
	return nil
}

// register tells the master we exist and ready to work
func (wk *Worker) register(master string) {
	args := new(common.RegisterArgs)
	args.Worker = wk.name
	callResult := common.RPCCall(master, "Master.Register", args, new(struct{}))
	if callResult == false {
		fmt.Printf("Register: RPC %s register error \n", master)
	}
}
