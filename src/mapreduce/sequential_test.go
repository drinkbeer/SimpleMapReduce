package mapreduce

import (
	"testing"
)


func TestSequentialSingle(t *testing.T) {
	mr := Sequential(jobName, MakeInputs(1), 1, MapFunc, ReduceFunc)
	mr.Wait()
	Check(t, mr.Files, jobName)
	CheckWorker(t, mr.stats)
	Cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := Sequential(jobName, MakeInputs(5), 3, MapFunc, ReduceFunc)
	mr.Wait()
	Check(t, mr.Files, jobName)
	CheckWorker(t, mr.stats)
	Cleanup(mr)
}