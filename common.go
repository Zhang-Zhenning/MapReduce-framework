package main

import "fmt"

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
	task_name  string
	file       string
	task_index int
	task_type  taskType
}

func doMap(tname string, tidx int, f string, mapf func(string, string) []KeyValue) {
	debuginfo("doMap: %s - %d - %s\n", tname, tidx, f)
}

func doReduce(tname string, tidx int, f string, redf func(string, []string) string) {
	debuginfo("doReduce: %s - %d - %s\n", tname, tidx, f)
}
