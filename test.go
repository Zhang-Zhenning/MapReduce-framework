package main

import (
	"fmt"
	"runtime"
	"strconv"
)

func main() {

	// clean up the socket files
	s := SetupUnixSocketFolder()

	// get all input files
	wc_files := FindFiles(DataFolder)

	// start running
	fmt.Println("Hello, playground")

	ret_c := make(chan *Master) // mark the end of the Master, the main thread can exit after this
	sv_c := make(chan bool)     // mark the end of master setting up servers, workers can start register after this

	go RunMaster(wc_files, NumReduceT, get_socket_name("Master"), TheJobName, ret_c, sv_c)

	for i := 0; i < runtime.NumCPU(); i++ {
		go RunWorker(get_socket_name("Master"), get_socket_name("Worker"+strconv.Itoa(i+1)), WcMapF, WcReduceF, sv_c)
	}

	// wait for the Master to finish, all workers should exit before the Master does
	<-ret_c

	// clean up the socket files
	CleanupUnixSocketFolder(s)
}
