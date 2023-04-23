package main

import (
	"strconv"
	"strings"
	"unicode"
)

// self-defined map function, can change depending on the task
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

// self-defined reduce function, can change depending on the task
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
