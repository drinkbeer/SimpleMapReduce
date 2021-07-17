package mapreduce

import (
	"bufio"
	"fmt"
	"github.com/drinkbeer/src/SimpleMapReduce/src/common"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

/*
This file includes common functions for unit tests.
*/

const (
	nNumber      = 100
	nMap         = 20
	nReduce      = 10
	jobName      = "test"
	masterSuffix = "master"
	workerSuffix = "worker"
)

// MapFunc splits in words
func MapFunc(file string, value string) (res []common.KeyValue) {
	common.Debug("Map %v \n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := common.KeyValue{
			Key:   w,
			Value: "",
		}
		res = append(res, kv)
	}
	return
}

// ReduceFunc just return key
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		common.Debug("Reduce %s %v \n", key, e)
	}
	return ""
}

// MakeInputs make input file
func MakeInputs(num int) (names []string) {
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("MakeInputs: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return
}

// Check checks input file against output file: each input number should show up
// in the output file in string sorted order
func Check(t *testing.T, files []string, jobName string) {
	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			log.Fatal("Check: ", err)
		}
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	output, err := os.Open(common.MergeResultName(jobName))
	if err != nil {
		log.Fatal("Check: ", err)
	}
	defer output.Close()
	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1) // returns the number of items successfully parsed
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d, err: %v \n", i, v1, v2, err)
		}
		i++
	}

	if i != nNumber {
		t.Fatalf("Expected %d lines in output \n", nNumber)
	}
}

// CheckWorker checks workers process at least 1 DoTask RPC when workers report back how many RPCs they have
// processed in the Shutdown reply.
func CheckWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("A worker didn't do any work \n")
		}
	}
}

// Cleanup cleans up the artifacts for testing
func Cleanup(mr *Master) {
	mr.CleanupFiles()
	for _, f := range mr.Files {
		common.RemoveFile(f)
	}
}

////////// Distributed Master Unit Test Suite //////////

// SetUpParallel sets up for Distributed Master
func SetUpParallel() *Master {
	files := MakeInputs(nMap)
	master := Port(masterSuffix)
	mr := Distributed(jobName, files, nReduce, master)
	return mr
}

// Port cooks up a unique-ish UNIX-domain socket name in /var/tmp. Cannot use current directory since AFS doesn't
// support UNIX-domain sockets. Usually we port for each master/worker.
func Port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

// RunWorker sets up a connection with the master, registers its address, and waits for tasks to be scheduled.
// It runs a single worker for testing.
func RunWorker(master string, worker string, MapFunc func(string, string) []common.KeyValue,
	ReduceFunc func(string, []string) string, nRPC int, parallelism *Parallelism) {
	common.Debug("RunWorker %s \n", worker)

	wk := new(Worker)
	wk.name = worker
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	wk.parallelism = parallelism

	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(worker) // only needed for "unix"
	l, err := net.Listen("unix", worker)
	if err != nil {
		log.Fatal("RunWorker: worker ", worker, " error: ", err)
	}
	wk.l = l
	wk.register(master)

	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()

		conn, err := wk.l.Accept()
		if err != nil {
			break
		}
		wk.Lock()
		wk.nRPC--
		wk.Unlock()
		go rpcs.ServeConn(conn)
	}
	wk.l.Close()
	common.Debug("RunWorker %s exit \n", worker)
}
