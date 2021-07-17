package mapreduce

import (
	"strconv"
	"testing"
	"time"
)

func TestParallelBasic(t *testing.T) {
	mr := SetUpParallel()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, Port(workerSuffix+strconv.Itoa(i)), MapFunc, ReduceFunc, -1, nil)
	}
	mr.Wait()
	Check(t, mr.Files, jobName)
	CheckWorker(t, mr.stats)
	Cleanup(mr)
}

func TestParallelCheck(t *testing.T) {
	mr := SetUpParallel()
	parallelism := &Parallelism{}
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, Port(workerSuffix+strconv.Itoa(i)), MapFunc, ReduceFunc, -1, parallelism)
	}
	mr.Wait()
	Check(t, mr.Files, jobName)
	CheckWorker(t, mr.stats)

	parallelism.mu.Lock()
	if parallelism.max < 2 {
		t.Fatalf("Workers did not execute in parallel")
	}
	parallelism.mu.Unlock()

	Cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := SetUpParallel()
	// Start 2 workers that fail after 10 tasks
	go RunWorker(mr.address, Port(workerSuffix+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10, nil)
	go RunWorker(mr.address, Port(workerSuffix+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1, nil)
	mr.Wait()
	Check(t, mr.Files, jobName)
	CheckWorker(t, mr.stats)
	Cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := SetUpParallel()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.doneChannel:
			Check(t, mr.Files, jobName)
			Cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := Port(workerSuffix + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10, nil)
			i++
			w = Port(workerSuffix + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10, nil)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}
