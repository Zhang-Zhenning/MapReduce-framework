package main

import (
	"fmt"
	"time"
)

type TEST struct{}

func (t *TEST) HelloWorld(args *string, reply *string) error {
	*reply = "Hello, " + *args
	return nil
}

func main() {

	var mname string = get_socket_name("Master")
	var wname1 string = get_socket_name("worker1")
	var wname2 string = get_socket_name("worker2")

	var fake_files = []string{"a.txt", "b.txt", "c.txt"}

	fmt.Println("Hello, playground")

	ret_c := make(chan *Master)

	go RunMaster(fake_files, 3, mname, "test_job", ret_c)
	time.Sleep(2 * time.Second)
	go RunWorker(mname, wname1, nil, nil)
	go RunWorker(mname, wname2, nil, nil)

	// wait for the Master to finish
	// all workers should exit before the Master does
	<-ret_c

}
