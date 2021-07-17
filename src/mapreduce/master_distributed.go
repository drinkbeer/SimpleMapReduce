package mapreduce

import (
	"fmt"
	"github.com/drinkbeer/src/SimpleMapReduce/src/common"
	"log"
	"net"
	"net/rpc"
	"os"
)

// Distributed schedules map and reduce tasks on workers that register with the master over RPC.
func Distributed(jobName string, files []string, nReduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()

	scheduleF := func(phase common.JobPhase) {
		ch := make(chan string)
		go mr.forwardRegistrations(ch)
		common.Schedule(mr.jobName, mr.Files, mr.nReduce, phase, ch)
	}

	finishF := func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	}

	go mr.run(jobName, files, nReduce, scheduleF, finishF)
	return
}

// startRPCServer starts the Master's RPC server. It continues accepting RPC calls (Register in particular) for as long
// as the worker is alive.
func (mr *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	err := rpcs.Register(mr)
	if err != nil {
		log.Fatal("RegistrationServer failed, error: ", err)
	}
	os.Remove(mr.address) // only needed for "unix"
	l, err := net.Listen("unix", mr.address)
	if err != nil {
		log.Fatal("RegistrationServer ", mr.address, " error: ", err)
	}
	mr.l = l

	// now that we are listening on the master address, can fork off accepting connections to another thread.
	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}

			conn, err := mr.l.Accept()
			if err != nil {
				common.Debug("RegistrationServer: accept error, %v", err)
				break
			}

			go func() {
				rpcs.ServeConn(conn)
				conn.Close()
			}()
		}
		common.Debug("RegistrationServer: done\n")
	}()
}

// stopRPCServer stops the master RPC server.
// This must be done through an RPC to avoid race conditions between the RPC server thread and the current threads.
func (mr *Master) stopRPCServer() {
	var reply common.ShutdownReply
	ok := common.RPCCall(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error \n", mr.address)
	}
	common.Debug("Cleanup Registration: done \n")
}

// forwardRegistrations is helper function that sends information about all existing and newly registered workers to
// channel ch. schedule() reads ch to learn about workers.
func (mr *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			// there's a worker that we haven't told Schedule() about.
			w := mr.workers[i]
			go func() { ch <- w }()		// send without holding the lock
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to a RPC from a new worker
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}

// Register is a RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
func (mr *Master) Register(args *common.RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	common.Debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)

	// tell forwardRegistrations() that there's a new workers[] entry
	mr.newCond.Broadcast()

	return nil
}

// Shutdown is a RPC method that shuts down the Master's RPC server
func (mr *Master) Shutdown(_, _ *string) error {
	common.Debug("Shutdown: registration server \n")
	close(mr.shutdown)
	mr.l.Close() // causes the Accept to fail
	return nil
}
