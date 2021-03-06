package common

import (
	"encoding/json"
	"log"
	"os"
)

/*
DoReduce manages one reduce task: it should read the intermediate
files for the task, sort the intermediate key/value pairs by key,
call the user-defined reduce function (reduceF) for each key, and
write reduceF's output to disk.

You'll need to read one intermediate file from each map task;
reduceName(jobName, m, reduceTask) yields the file
name from map task m.

Your doMap() encoded the key/value pairs in the intermediate
files, so you will need to decode them. If you used JSON, you can
read and decode by creating a decoder and repeatedly calling
.Decode(&kv) on it until it returns an error.

You may find the first example in the golang sort package
documentation useful.

reduceF() is the application's reduce function. You should
call it once per distinct key, with a slice of all the values
for that key. reduceF() returns the reduced value for that key.

You should write the reduce output as JSON encoded KeyValue
objects to the file named outFile. We require you to use JSON
because that is what the merger than combines the output
from all the reduce tasks expects. There is nothing special about
JSON -- it is just the marshalling format we chose to use. Your
output code will look something like this:

enc := json.NewEncoder(file)
for key := ... {
	enc.Encode(KeyValue{key, reduceF(...)})
}
file.Close()

Your code here (Part I).


	1. Get the reduce file name through reduceName method
	2. Decode the reduce file to KV pairs
	3. Encode KV pairs to output file
*/
func DoReduce(
	jobName string,
	reduceTaskIndex int,
	outFileName string,
	nMap int,
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)
	for m := 0; m < nMap; m++ {
		reduceFileName := ReduceName(jobName, m, reduceTaskIndex)
		reduceFile, err := os.Open(reduceFileName)
		if err != nil {
			log.Fatal("DoReduce failed to open: ", err)
		}
		defer reduceFile.Close()

		dec := json.NewDecoder(reduceFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatal("DoReduce failed to create output file: ", err)
	}
	defer outFile.Close()
	enc := json.NewEncoder(outFile)
	for k, v := range kvs {
		enc.Encode(KeyValue{k, reduceF(k, v)})
	}
}
