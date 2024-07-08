package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
)

type (
	Object struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	Result struct {
		batchStart int
		sum        int
	}
)

var (
	goroutNum int
	err       error
)

func main() {

	switch len(os.Args) {
	case 1:
		goroutNum = runtime.NumCPU()
	case 2:
		goroutNum, err = strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Println("usage: `go run . 4`, where 4 is the sample number of goroutines will be in use`")
	}

	data, err := readFile("../data.json")
	if err != nil {
		log.Printf("error reading json file: %s", err)
		return
	}

	totalSum := dataAsyncAction(data, goroutNum)

	fmt.Printf("number of goroutines: %d\n", goroutNum)
	fmt.Printf("total sum: %d\n", totalSum)
}

func readFile(filePath string) ([]Object, error) {
	byteValue, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var data = make([]Object, 0, 1000000)
	err = json.Unmarshal(byteValue, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func dataAsyncAction(data []Object, numGoroutines int) int {
	var batchSize = 1000000 / numGoroutines
	taskChan := make(chan int)
	resultChan := make(chan Result)
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go worker(data, taskChan, resultChan, batchSize, &wg)
	}

	go func() {
		for i := 0; i < len(data); i += batchSize {
			select {
			case <-done:
				close(taskChan)
				return
			case taskChan <- i:
			}
		}
		close(taskChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var totalSum int
	var minBatchStart = -1
	for r := range resultChan {
		totalSum += r.sum
		if minBatchStart == -1 || r.batchStart < minBatchStart {
			minBatchStart = r.batchStart
		}
	}

	close(done)
	return totalSum
}

func worker(data []Object, taskChan <-chan int, resultChan chan<- Result, batchSize int, wg *sync.WaitGroup) {
	defer wg.Done()
	for batchStart := range taskChan {
		sum := 0
		end := batchStart + batchSize
		if end > len(data) {
			end = len(data)
		}
		for i := batchStart; i < end; i++ {
			a, b := data[i].A, data[i].B
			if a > 0 && b > 0 && int64(a)+int64(b) > int64(0x7fffffff) {
				log.Printf("overflow detected at index %d: a=%d, b=%d", i, a, b)
				continue
			}
			sum += a + b
		}
		resultChan <- Result{batchStart: batchStart, sum: sum}
	}
}
