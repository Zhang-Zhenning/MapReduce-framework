package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strconv"
)

// debuginfo function
var debugEnabled bool = false

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
	return "./results/" + "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func ReduceResultName(jobName string, reduceTask int) string {
	return "./results/" + "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func FinalResultName(jobName string) string {
	return "./results/" + "mrtmp." + jobName + "-res"
}

// hash each key to a shard
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func (mm *Master) CombineFiles() {
	var res_files []string
	for i := 0; i < mm.nReduce; i++ {
		res_files = append(res_files, ReduceResultName(mm.Task_name, i))
	}
	var final_file string = FinalResultName(mm.Task_name)
	newFile, err := os.Create(final_file)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range res_files {
		temp_file, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		_, err = io.Copy(newFile, temp_file)
		if err != nil {
			log.Fatal(err)
		}
		temp_file.Close()
	}
	newFile.Close()
}
