package main

import (
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

// ---------------------------rpc functions for Worker---------------------------

// this function should be called by the Worker to start the server, should be a coroutine
func (wk *Worker) Worker_start_server() {

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

	debuginfo("Worker %s start server\n", wk.Worker_name)
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
func (wk *Worker) Worker_shutdown_server(_ *struct{}, res *ShutdownWorkerReply) error {
	debuginfo("Worker %s shutdown server\n", wk.Worker_name)

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
func (wk *Worker) worker_register(master_name string) {
	// register the Worker to the Master
	args := new(RegisterWorkerArgs)
	args.Worker_name = wk.Worker_name

	flag := call(master_name, "Master.RegisterWorker", args, new(struct{}))
	if flag == false {
		log.Fatal("RunWorker: Worker ", wk.Worker_name, "error: can't register to Master")
	}
}

// this function should be called by the Master to assign a task to the Worker
func (wk *Worker) Worker_assign_task(task *TaskDetail, _ *struct{}) error {

	debuginfo("Worker %s assign task %s - %s - %d\n", wk.Worker_name, task.task_name, task.task_type, task.task_index)

	// update the workder info
	wk.Mu.Lock()
	wk.Num_parallel_tasks++
	npt := wk.Num_parallel_tasks
	wk.Mu.Unlock()

	// one Worker should run at most 1 task at a time
	if npt > 1 {
		log.Fatal("Worker %s has more than 1 task running at the same time\n", wk.Worker_name)
	}

	switch task.task_type {
	case MapTask:
		doMap(task.task_name, task.task_index, task.file, wk.Map_func)
	case ReduceTask:
		doReduce(task.task_name, task.task_index, task.file, wk.Red_func)
	}

	// update the workder info
	wk.Mu.Lock()
	wk.Num_parallel_tasks--
	wk.Num_done_tasks++
	wk.Mu.Unlock()

	debuginfo("Worker %s finish task %s - %s - %d\n", wk.Worker_name, task.task_name, task.task_type, task.task_index)
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

	debuginfo("Worker %s start to run\n", worker_name)

	// start the server
	go wk.Worker_start_server()

	// register the Worker to the Master
	wk.worker_register(master_name)

	// wait for shutdown
	<-wk.Shutdown_chan

	debuginfo("Worker %s shutdown\n", worker_name)
}
