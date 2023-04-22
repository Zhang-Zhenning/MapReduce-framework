package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type TEST struct{}

func (t *TEST) HelloWorld(args *string, reply *string) error {
	*reply = "Hello, " + *args
	return nil
}

// test map and reduce functions for word count
func WcMapF(filename string, contents string) []KeyValue {
	wordsKv := make(map[string]int)

	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, word := range words {
		if _, ok := wordsKv[word]; ok {
			wordsKv[word]++
		} else {
			wordsKv[word] = 1
		}
	}

	var rst []KeyValue
	for key, value := range wordsKv {
		kv := KeyValue{
			key,
			strconv.Itoa(value),
		}
		rst = append(rst, kv)
	}
	return rst
}

func WcReduceF(key string, values []string) string {
	cnt := 0
	for _, value := range values {
		num, err := strconv.Atoi(value)
		if err != nil {
			break
		}
		cnt += num
	}
	return strconv.Itoa(cnt)
}

func main() {

	var mname string = get_socket_name("Master")
	var wname1 string = get_socket_name("worker1")
	var wname2 string = get_socket_name("worker2")

	var wc_files = []string{"./data/wordsa.txt", "./data/wordsb.txt", "./data/wordsc.txt"}

	fmt.Println("Hello, playground")

	ret_c := make(chan *Master)

	go RunMaster(wc_files, 2, mname, "word_count_job_test1", ret_c)
	time.Sleep(2 * time.Second)
	go RunWorker(mname, wname1, WcMapF, WcReduceF)
	go RunWorker(mname, wname2, WcMapF, WcReduceF)

	// wait for the Master to finish
	// all workers should exit before the Master does
	<-ret_c

}
