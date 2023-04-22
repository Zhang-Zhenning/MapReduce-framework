package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Worker struct {
	Mu sync.Mutex

	Worker_name        string // the Worker's unix domain socket Worker_name
	Num_done_tasks     int    // number of tasks Worker has done
	Listener           net.Listener
	Num_parallel_tasks int // number of concurrent tasks assigned to this Worker
	Map_func           func(string, string) []KeyValue
	Red_func           func(string, []string) string
	Shutdown_chan      chan struct{} // when this channel is closed then all the net threads should be shutdown
}

func doMap(tname string, mapidx int, num_reduces int, f string, mapf func(string, string) []KeyValue) {
	fmt.Printf("doMap: %s - %d - %s\n", tname, mapidx, f)

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

	fmt.Printf("doReduce: %s - %d - %s\n", tname, redidx, outf)

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

// ---------------------------rpc functions for Worker---------------------------

// this function should be called by the Worker to start the server, should be a coroutine
func (wk *Worker) WorkerStartServer() {

	// setup rpc server
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	//os.Remove(wk.Worker_name)

	// setup net Listener
	lis, er := net.Listen("unix", wk.Worker_name)
	if er != nil {
		log.Fatal("RunWorker: Worker ", wk.Worker_name, "error: ", er)
	}
	wk.Listener = lis

	fmt.Printf("Worker %s start server\n", wk.Worker_name)
	// infinite loop to accept new connection
	for {
		// if it is shutdown, then break the loop
		select {
		case <-wk.Shutdown_chan:
			break
		default:
		}
		// wait for connection
		conc, er := wk.Listener.Accept()
		if er != nil {
			debuginfo("RunWorker %s has network problem, can't accept conn\n", wk.Worker_name)
			break
		} else {
			// start a new coroutine to handle the connection
			go func() {
				rpcs.ServeConn(conc)
				conc.Close()
			}()
		}
	}
}

// this function should be called by the Master to shutdown the Worker
func (wk *Worker) WorkerShutdownServer(_ *struct{}, res *ShutdownWorkerReply) error {
	fmt.Printf("Worker %s shutdown server\n", wk.Worker_name)

	wk.Mu.Lock()
	res.Worker_name = wk.Worker_name
	res.Num_finished_task = wk.Num_done_tasks
	wk.Mu.Unlock()

	// close the shutdown channel
	wk.Listener.Close()
	close(wk.Shutdown_chan)
	os.Remove(wk.Worker_name)
	return nil
}

// this function should be called by the Worker to register the Worker to the Master
func (wk *Worker) WorkerRegisterToMaster(master_name string) {
	// register the Worker to the Master
	args := new(RegisterWorkerArgs)
	args.Worker_name = wk.Worker_name

	flag := call(master_name, "Master.RegisterWorker", args, new(struct{}))
	if flag == false {
		log.Fatal("RunWorker: Worker ", wk.Worker_name, "error: can't register to Master")
	}
}

// this function should be called by the Master to assign a task to the Worker
func (wk *Worker) WorkerAssignTask(task *TaskDetail, _ *struct{}) error {

	fmt.Printf("Worker %s assign task %s - %s - %d\n", wk.Worker_name, task.TaskName, task.TaskType, task.TaskIndex)

	// update the workder info
	wk.Mu.Lock()
	wk.Num_parallel_tasks++
	npt := wk.Num_parallel_tasks
	wk.Mu.Unlock()

	// one Worker should run at most 1 task at a time
	if npt > 1 {
		log.Fatal("Worker %s has more than 1 task running at the same time\n", wk.Worker_name)
	}

	switch task.TaskType {
	case MapTask:
		doMap(task.TaskName, task.TaskIndex, task.NumOtherPhase, task.File, wk.Map_func)
	case ReduceTask:
		doReduce(task.TaskName, task.TaskIndex, task.NumOtherPhase, task.File, wk.Red_func)
	}

	// update the workder info
	wk.Mu.Lock()
	wk.Num_parallel_tasks--
	wk.Num_done_tasks++
	wk.Mu.Unlock()

	fmt.Printf("Worker %s finish task %s - %s - %d\n", wk.Worker_name, task.TaskName, task.TaskType, task.TaskIndex)
	return nil

}

// run Worker to standby for upcomming tasks
func RunWorker(master_name string, worker_name string, map_func func(string, string) []KeyValue, red_func func(string, []string) string) {

	// create a new Worker
	wk := new(Worker)
	wk.Worker_name = worker_name
	wk.Map_func = map_func
	wk.Red_func = red_func
	wk.Shutdown_chan = make(chan struct{})

	fmt.Printf("Worker %s start to run\n", worker_name)

	// start the server
	go wk.WorkerStartServer()

	// register the Worker to the Master
	wk.WorkerRegisterToMaster(master_name)

	// wait for shutdown
	<-wk.Shutdown_chan

	fmt.Printf("Worker %s shutdown\n", worker_name)
}
