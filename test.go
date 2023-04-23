package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
)

type TEST struct{}

func (t *TEST) HelloWorld(args *string, reply *string) error {
	*reply = "Hello, " + *args
	return nil
}

func main() {

	// clean up the socket files
	s := RPCServerPath + "/uid-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.RemoveAll(s)
	os.Mkdir(s, 0777)

	// master name
	var mname string = get_socket_name("Master")

	// get all input files
	var wc_files []string
	wc_files = FindFiles("./data")

	// start running
	fmt.Println("Hello, playground")

	ret_c := make(chan *Master)
	sv_c := make(chan bool)

	go RunMaster(wc_files, NumReduceT, mname, "word_count_job_test", ret_c, sv_c)

	for i := 0; i < runtime.NumCPU(); i++ {
		var cur_name string = get_socket_name("Worker" + strconv.Itoa(i+1))
		go RunWorker(mname, cur_name, WcMapF, WcReduceF, sv_c)
	}

	// wait for the Master to finish
	// all workers should exit before the Master does
	<-ret_c
	os.RemoveAll(s)

}
