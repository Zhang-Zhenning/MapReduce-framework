package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

// debuginfo function
var debugEnabled bool = true

func debuginfo(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// keyvalue for map reduce
type KeyValue struct {
	Key   string
	Value string
}

// task-related
type taskType string

const (
	MapTask    taskType = "MapTask"
	ReduceTask taskType = "ReduceTask"
	MaxWorkers int      = 10
)

type TaskDetail struct {
	TaskName      string
	File          string
	TaskIndex     int
	NumOtherPhase int
	TaskType      taskType
}

// map-reduce related
func MapResultName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func ReduceResultName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(tname string, mapidx int, num_reduces int, f string, mapf func(string, string) []KeyValue) {
	debuginfo("doMap: %s - %d - %s\n", tname, mapidx, f)

	// read the file
	data, err := ioutil.ReadFile(f)
	if err != nil {
		log.Fatal(err)
	}

	// call the map function
	kvps := mapf(f, string(data))

	// create nReduce files
	var outFiles []*os.File

	for i := 0; i < num_reduces; i++ {
		name := MapResultName(tname, mapidx, i)
		file, err := os.Create(name)
		if err != nil {
			log.Fatal(err)
		}
		outFiles = append(outFiles, file)
	}

	// write the key/value pairs to the files
	for _, kvp := range kvps {
		idx := ihash(kvp.Key) % num_reduces
		enc := json.NewEncoder(outFiles[idx])
		enc.Encode(kvp)
	}

	// close the files
	for _, file := range outFiles {
		file.Close()
	}

}

func doReduce(tname string, redidx int, num_map int, outf string, redf func(string, []string) string) {

	debuginfo("doReduce: %s - %d - %s\n", tname, redidx, outf)

	kvsmap := make(map[string][]string)

	// read all files belong to this reduce idx
	for i := 0; i < num_map; i++ {
		name := MapResultName(tname, i, redidx)
		file, err := os.Open(name)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kvp KeyValue
			if err := dec.Decode(&kvp); err != nil {
				break
			}
			// combine the key/value pairs
			kvsmap[kvp.Key] = append(kvsmap[kvp.Key], kvp.Value)
		}
		file.Close()
	}

	// generate the output file
	rfile, err := os.Create(outf)
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(rfile)

	//fill the output file
	for k, v := range kvsmap {
		data := redf(k, v)
		kvo := KeyValue{k, data}
		enc.Encode(kvo)

	}

	// close the file
	rfile.Close()
}
