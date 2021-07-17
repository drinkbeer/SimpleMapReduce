package mapreduce

import (
	"strconv"
	"testing"
)

func TestParallelBasic(t *testing.T) {
	mr := SetUpParallel()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, Port(workerSuffix + strconv.Itoa(i)), MapFunc, ReduceFunc, -1, nil)
	}
	mr.Wait()
	Check(t, mr.Files, jobName)
	CheckWorker(t, mr.stats)
	Cleanup(mr)
}