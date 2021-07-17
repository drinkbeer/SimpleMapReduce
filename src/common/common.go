package common

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
)

// debugEnabled tells if debugging enabled
const debugEnabled = false

func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// JobPhase indicates whether a task is scheduled as a map or reduce task.
type JobPhase string

const (
	MapPhase    JobPhase = "mapPhase"
	ReducePhase          = "reducePhase"
)

type KeyValue struct {
	Key   string
	Value string
}

// RemoveFile removes a file
func RemoveFile(f string) {
	err := os.Remove(f)
	if err != nil {
		log.Fatal("RemoveFile: ", err)
	}
}

// ReduceName constructs the name of the intermediate file which map task
// <mapTaskIndex> produces for reduce task <reduceTaskIndex
// Format: "mrtmp.<job_name>-<map_task_index>-<reduce_task_index>.txt"
func ReduceName(jobName string, mapTaskIndex int, reduceTaskIndex int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTaskIndex) + "-" + strconv.Itoa(reduceTaskIndex) + ".txt"
}

// MergeName constructs the name of the output file of reduce task <reduceTaskIndex>
// Format: "mrtmp.<job_name>-res-<reduce_task_index>.txt"
func MergeName(jobName string, reduceTaskIndex int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTaskIndex) + ".txt"
}

// MergeResultName constructs the name of final merge result file
// Format: "mrtmp.<job_name>.txt"
func MergeResultName(jobName string) string {
	return "mrtmp." + jobName + ".txt"
}

// GetHash calculates the hash value based on input string
func GetHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
