package main

import (
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Cook up a unique-ish UNIX-domain socket Worker_name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func get_socket_name(suffix string) string {
	s := RPCServerPath + "/uid-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.RemoveAll(s)
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

// register args when register a Worker to Master
type RegisterWorkerArgs struct {
	Worker_name string
}

// shutdown args when shutdown a Worker
type ShutdownWorkerReply struct {
	Worker_name       string
	Num_finished_task int
}

// rpc call function
func call(srv_name string, func_name string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv_name)
	if errx != nil {
		log.Fatal("dialing:", errx)
		return false
	}
	defer c.Close()

	err := c.Call(func_name, args, reply)
	if err != nil {
		log.Fatal("call:", err)
		return false
	}

	return true
}
