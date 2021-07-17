package mapreduce

import "github.com/drinkbeer/src/SimpleMapReduce/src/common"

// Sequential runs map and reduce tasks sequentially, waiting for each task to complete before running the next.
func Sequential(jobName string, files []string, nReduce int, mapF func(string, string) []common.KeyValue,
	reduceF func(string, []string) string) (mr *Master) {
	mr = newMaster(MasterName)
	scheduleF := func(phase common.JobPhase) {
		switch phase {
		case common.MapPhase:
			for i, f := range mr.Files {
				common.DoMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case common.ReducePhase:
			for i := 0; i < mr.nReduce; i++ {
				common.DoReduce(mr.jobName, i, common.MergeName(mr.jobName, i), len(mr.Files), reduceF)
			}
		}
	}

	finishF := func() {
		mr.stats = []int{len(files) + nReduce}
	}

	go mr.run(jobName, files, nReduce, scheduleF, finishF)
	return
}
