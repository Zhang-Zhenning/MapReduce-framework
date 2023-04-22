package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
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
	debuginfo("Master %s register Worker %s\n", m.Master_name, args.Worker_name)
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
func (m *Master) MasterStartServer() {
	rpcs := rpc.NewServer()
	err := rpcs.Register(m)
	if err != nil {
		log.Fatal("RunMaster: Register error: ", err)
		return
	}
	lis, er := net.Listen("unix", m.Master_name)

	if er != nil {
		log.Fatal("RunMaster: Master ", m.Master_name, "error: ", er)
	}
	m.listener = lis

	fmt.Printf("Master %s start server\n", m.Master_name)

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

func (m *Master) MasterShutdownServer() {
	fmt.Printf("Master %s shutdown server\n", m.Master_name)
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

// this function should be called by the Master to start a job and create a Master
func RunMaster(files []string, nReduce int, master_name string, job_name string, return_chan chan *Master) *Master {
	m := MakeMaster(master_name)
	m.Files = files
	m.nReduce = nReduce
	m.Task_name = job_name
	go m.HandleNewWorker()
	go m.MasterStartServer()

	// start schedule the map and reduce task
	fmt.Printf("%s: Starting Map/Reduce task %s\n", m.Master_name, m.Task_name)
	//schedule(m.Task_name, m.Files, m.nReduce, m, mapTask)
	//schedule(m.Task_name, m.Files, m.nReduce, m, reduceTask)

	time.Sleep(14 * time.Second)

	// kill all workers
	worker_task_gather := m.KillAllWorkers()
	// print worker task gather
	for _, num := range worker_task_gather {
		fmt.Printf("%s has finished %d tasks\n", m.Master_name, num)
	}

	// shutdown Master server
	m.MasterShutdownServer()

	fmt.Printf("%s: Map/Reduce task %s completed\n", m.Master_name, m.Task_name)

	// signal task finished
	m.task_done_chan <- true

	// return the Master
	return_chan <- m
	fmt.Printf("Master %s shutdown\n", m.Master_name)
	return m
}

func (m *Master) WaitMaster() {
	<-m.task_done_chan
}

func (m *Master) KillAllWorkers() []int {
	worker_task_gather := make([]int, 0, len(m.workers))

	m.Mu.Lock()
	for _, worker := range m.workers {
		fmt.Printf("%s: Shutdown Worker %s\n", m.Master_name, worker)
		var reply ShutdownWorkerReply
		ok := call(worker, "Worker.WorkerShutdownServer", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("%s: Shutdown Worker %s error\n", m.Master_name, worker)
		} else {
			worker_task_gather = append(worker_task_gather, reply.Num_finished_task)
		}
	}
	m.Mu.Unlock()
	return worker_task_gather
}
