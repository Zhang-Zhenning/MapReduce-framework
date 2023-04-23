package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
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
	MapTask       taskType = "MapTask"
	ReduceTask    taskType = "ReduceTask"
	MaxWorkers    int      = 10
	RPCServerPath string   = "."
	NumReduceT    int      = 2
	DataFolder    string   = "./data"
	TheJobName    string   = "word_count"
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

// combine all the reduce result files into one file
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

// find all input files under the data directory
func FindFiles(dir string) []string {
	var filePaths []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal("Error accessing path %q: %v\n", path, err)
			return err
		}

		// Skip directories, only collect file paths
		if !info.IsDir() {
			filePaths = append(filePaths, path)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking the directory: %v\n", err)
		return nil
	}

	return filePaths

}

// create folder for unix socket files
func SetupUnixSocketFolder() string {
	s := RPCServerPath + "/uid-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.RemoveAll(s)
	os.Mkdir(s, 0777)
	return s
}

// clean up the unix socket folder
func CleanupUnixSocketFolder(ss string) {
	os.RemoveAll(ss)
}
