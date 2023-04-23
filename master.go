package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	Mu sync.Mutex

	Master_name string

	Task_name      string
	Files          []string
	nReduce        int
	task_done_chan chan bool

	workers []string

	shutdown_chan  chan struct{}
	listener       net.Listener
	incomming_cond *sync.Cond
	workers_chan   chan string
}

// this function should be called by the Worker to register itself to the Master
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, _ *struct{}) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	fmt.Printf("Master %s: register Worker %s\n", m.Master_name, args.Worker_name)
	m.workers = append(m.workers, args.Worker_name)
	m.incomming_cond.Broadcast()
	return nil
}

// this function should be called by the Master to handle the newly registered Worker
// this function should be a coroutine
func (m *Master) HandleNewWorker() {
	idx := 0
	for {
		m.Mu.Lock()
		if len(m.workers) > idx {
			new_worker := m.workers[idx]
			idx++
			go func() { m.workers_chan <- new_worker }()
		} else {
			m.incomming_cond.Wait()
		}
		m.Mu.Unlock()
	}
}

// this function should be called by the Master to start the Master server
func (m *Master) MasterStartServer(sv chan bool) {
	rpcs := rpc.NewServer()
	err := rpcs.Register(m)
	os.Remove(m.Master_name)
	if err != nil {
		log.Fatal("RunMaster: Register error: ", err)
		return
	}
	lis, er := net.Listen("unix", m.Master_name)

	if er != nil {
		log.Fatal("RunMaster: Master ", m.Master_name, "error: ", er)
	}
	m.listener = lis

	fmt.Printf("Master %s: start server\n", m.Master_name)
	// tell workers they can go on
	close(sv)

	for {
		select {
		case <-m.shutdown_chan:
			break
		default:
		}
		conc, er := m.listener.Accept()
		if er != nil {
			debuginfo("RunMaster %s has network problem, can't accept conn\n", m.Master_name)
			break
		} else {
			go func() {
				rpcs.ServeConn(conc)
				conc.Close()
			}()
		}
	}
}

// this function should be called by the Master to shutdown the Master server
func (m *Master) MasterShutdownServer() {
	fmt.Printf("Master %s: shutdown server\n", m.Master_name)
	close(m.shutdown_chan)
	m.listener.Close()
}

// this function should be called by the Master to create a new Master
func MakeMaster(name string) *Master {
	m := new(Master)
	m.Master_name = name
	m.task_done_chan = make(chan bool, 1)
	m.shutdown_chan = make(chan struct{})
	m.workers_chan = make(chan string, MaxWorkers)
	m.incomming_cond = sync.NewCond(&m.Mu)
	return m
}

// kill all workers when master is about to shutdown
func (m *Master) KillAllWorkers() []int {
	worker_task_gather := make([]int, 0, len(m.workers))

	m.Mu.Lock()
	for _, worker := range m.workers {
		fmt.Printf("Master %s: Shutdown Worker %s\n", m.Master_name, worker)
		var reply ShutdownWorkerReply
		ok := call(worker, "Worker.WorkerShutdownServer", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master %s: Shutdown Worker %s error\n", m.Master_name, worker)
		} else {
			worker_task_gather = append(worker_task_gather, reply.Num_finished_task)
		}
	}
	m.Mu.Unlock()
	return worker_task_gather
}

// schedule the task to workers
func (m *Master) ScheduleJob(TaskPhase taskType) {
	fmt.Printf("Master %s: Schedule %s task %s\n", m.Master_name, TaskPhase, m.Task_name)

	var n_tasks int
	var n_others int
	var task_wait_group sync.WaitGroup

	if TaskPhase == MapTask {
		n_tasks = len(m.Files)
		n_others = m.nReduce
	} else {
		n_tasks = m.nReduce
		n_others = len(m.Files)
	}

	task_wait_group.Add(n_tasks)
	for i := 0; i < n_tasks; i++ {
		go func(sidx int) {
			var task_args TaskDetail
			task_args.TaskName = m.Task_name
			task_args.TaskType = TaskPhase
			task_args.TaskIndex = sidx
			task_args.NumOtherPhase = n_others
			if TaskPhase == MapTask {
				task_args.File = m.Files[sidx]
			} else {
				task_args.File = ReduceResultName(task_args.TaskName, task_args.TaskIndex)
			}

			done := false
			for !done {
				// get a free worker from the channel, all workers in channel should be free
				cur_worker := <-m.workers_chan
				done = call(cur_worker, "Worker.WorkerAssignTask", &task_args, new(struct{}))
				// the worker is available, put it back to the channel
				go func() { m.workers_chan <- cur_worker }()
			}

			task_wait_group.Done()

		}(i)
	}

	task_wait_group.Wait()
	fmt.Printf("Master %s: Schedule %s task %s done\n", m.Master_name, TaskPhase, m.Task_name)

}

// this function should be called by the Master to start a job and create a Master
func RunMaster(files []string, nReduce int, master_name string, job_name string, return_chan chan *Master, sev_chan chan bool) *Master {
	m := MakeMaster(master_name)
	m.Files = files
	m.nReduce = nReduce
	m.Task_name = job_name
	go m.HandleNewWorker()
	go m.MasterStartServer(sev_chan)

	// start schedule the map and reduce task
	fmt.Printf("Master %s: start to run\n", m.Master_name)
	fmt.Printf("Master %s: Starting Map/Reduce task %s\n", m.Master_name, m.Task_name)

	//time.Sleep(5 * time.Second)

	m.ScheduleJob(MapTask)
	m.ScheduleJob(ReduceTask)

	// combine files
	m.CombineFiles()

	//time.Sleep(5 * time.Second)

	// kill all workers
	worker_task_gather := m.KillAllWorkers()
	// print worker task gather
	for i, num := range worker_task_gather {
		fmt.Printf("Worker %s: finished %d tasks\n", m.workers[i], num)
	}

	// shutdown Master server
	m.MasterShutdownServer()

	fmt.Printf("Master %s: Map/Reduce task %s completed\n", m.Master_name, m.Task_name)

	// signal task finished
	m.task_done_chan <- true

	// return the Master
	return_chan <- m
	fmt.Printf("Master %s: shutdown\n", m.Master_name)
	return m
}
