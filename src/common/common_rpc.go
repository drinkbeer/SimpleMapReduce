package common

import (
	"fmt"
	"net/rpc"
	"sync"
)

/*
This file includes RPC types and methods.
Field names must start with capital letters, otherwise RPC will break.
*/

// DoTaskArgs holds the arguments that are passed to a worker when a job is
// scheduled on it.
type DoTaskArgs struct {
	JobName    string
	File       string   // only for map, the input file
	Phase      JobPhase // are we in MapPhase or ReducePhase
	TaskNumber int      // this task's index in the current phase

	// NumOtherPhase is the total number of tasks in other phases; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int
}

// RegisterArgs is the argument passed when a worker registers with the master.
type RegisterArgs struct {
	Worker string // the worker's UNIT-domain socket name, e.g. its RPC address
}

// ShutdownReply is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	Ntasks int
}

// RPCCall sends an RPC to the rpcname handler on server 'srv' with arguments 'args',
// waits for the response, and leaves the response in 'reply'. The 'reply' should be
// the address of a 'reply' structure.
//
// RPCCall returns true if the server responded, and false if RPCCall received no
// reply from the server. reply's contents are valid if and only if RPCCall returned
// true.
//
// You should assume that RPCCall will timeout and return false after a while if it
// doesn't get a reply from the server.
//
// Please use RPCCall to send all RPCs.
func RPCCall(srv string, rpcname string, args interface{}, reply interface{}) bool {
	client, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err != nil {
		Debug("Faled to call, err: %s", err)
		return false
	}

	return true
}

/*
Schedule starts and waits for all tasks in the give phase (mapPhase or reducePhase).

The mapFiles argument holds the names of the files that are input to the map phase, one per map task.
The nReduce argument is the number of reduce tasks.
The workersRegisterChan argument yields a stream of registered workers; each item is the worker's RPC
address, suitable for passing to RPCCall. workersRegisterChan will yield all existing registered workers
(if any) and new ones as they register.

All nTasks tasks have to be scheduled on workers. Once all tasks have completed successfully, schedule()
should return.

		1. Get worker from workersRegisterChan
		2. Call "DoTask" service using RPC for each task
		3. Return the worker back to registerChan after using it
		4. Do not return until all the tasks are finished
*/
func Schedule(jobName string, mapFiles []string, nReduce int, phase JobPhase, workersRegisterChan chan string) {
	var nTasks int
	var nOther int
	switch phase {
	case MapPhase:
		nTasks = len(mapFiles)
		nOther = nReduce
	case ReducePhase:
		nTasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os) \n", nTasks, phase, nOther)

	var wg sync.WaitGroup
	for i := 0; i < nTasks; i++ {
		doTaskArgs := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nOther,
		}
		// Add adds delta, which may be negative, to the WaitGroup counter.
		// If the counter becomes zero, all goroutines blocked on Wait are released.
		// If the counter goes negative, Add panics.
		wg.Add(1)
		go func(doTaskArgs DoTaskArgs, workersRegisterChan chan string) {
			defer wg.Done()

			for {
				// get one worker from workersRegisterChan if available, block if worker not available
				worker := <-workersRegisterChan
				callResult := RPCCall(worker, "Worker.DoTask", doTaskArgs, nil)

				// put worker back after using it
				go func() {
					workersRegisterChan <- worker
				}()

				if callResult == true {
					break
				}

				fmt.Printf("Schedule: failed to call Worker %s DoTask in RPC \n", worker)
			}
		}(doTaskArgs, workersRegisterChan)
	}
	// Wait blocks until the WaitGroup counter is zero.
	wg.Wait()

	fmt.Printf("Schedule: %v done \n", phase)
}
