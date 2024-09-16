package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

var ChForSlot = make(chan struct{}, 1)

func main() {
	inputData := []int{0, 1, 1, 2, 3, 5, 8}

	var dataResult string
	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, number := range inputData {
				out <- number
			}

		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataResult = (<-in).(string)
		}),
	}

	start := time.Now()

	ExecutePipeline(hashSignJobs...)

	end := time.Since(start)

	fmt.Println(dataResult, end)
}

func workerCrc32Md5(in string, out chan interface{}, slot chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	slot <- struct{}{}
	resultMd5 := DataSignerMd5(in)
	<-slot
	out <- DataSignerCrc32(resultMd5)
}

func workerCrc32(in string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	out <- DataSignerCrc32(in)
}

func workerCrc32MH(in, index string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	out <- index + DataSignerCrc32(index+in)
}

func SingleHash(in, out chan interface{}) {
	wgSH := &sync.WaitGroup{}
	for x := range in {
		wgSH.Add(1)
		go func(inputData string, outputData chan interface{}, wgSH *sync.WaitGroup) {
			defer wgSH.Done()
			chOutCrc32 := make(chan interface{}, 1)
			chOutCrc32Md5 := make(chan interface{}, 1)

			wgSH.Add(1)
			go workerCrc32(inputData, chOutCrc32, wgSH)
			wgSH.Add(1)
			go workerCrc32Md5(inputData, chOutCrc32Md5, ChForSlot, wgSH)

			outputData <- (<-chOutCrc32).(string) + "~" + (<-chOutCrc32Md5).(string)

		}(strconv.Itoa(x.(int)), out, wgSH)
	}
	wgSH.Wait()
}

func MultiHash(in, out chan interface{}) {
	wgMH := &sync.WaitGroup{}
	for x := range in {
		wgMH.Add(1)
		go func(inputData string, outputData chan interface{}, wgMH *sync.WaitGroup) {
			chOutCrc32MH := make(chan interface{})
			result := []string{}
			totalResult := ""
			wg := &sync.WaitGroup{}
			for i := 0; i < 6; i++ {
				wg.Add(1)
				go workerCrc32MH(inputData, strconv.Itoa(i), chOutCrc32MH, wg)
			}

			wg.Add(1)
			go func(out chan interface{}, wg *sync.WaitGroup) {
				defer wg.Done()
				for x := range out {
					result = append(result, x.(string))
					if len(result) == 6 {
						close(out)
					}
				}
			}(chOutCrc32MH, wg)

			wg.Wait()

			sort.Slice(result, func(i, j int) bool {
				return result[i] < result[j]
			})

			for _, i := range result {
				totalResult += i[1:]
			}

			outputData <- totalResult

			wgMH.Done()

		}(x.(string), out, wgMH)
	}
	wgMH.Wait()
}

func CombineResults(in, out chan interface{}) {
	result := []string{}
	totalResult := ""
	wgCR := &sync.WaitGroup{}
	wgCR.Add(1)
	go func(in chan interface{}) {
		defer wgCR.Done()
		for x := range in {
			result = append(result, x.(string))
		}
	}(in)
	wgCR.Wait()

	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	for j, i := range result {
		if j == 0 {
			totalResult += i
		} else {
			totalResult += "_" + i
		}
	}
	out <- totalResult
}
func ExecutePipeline(flowJobs ...job) {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}
	for _, function := range flowJobs {
		out := make(chan interface{})
		wg.Add(1)
		go func(x func(in, out chan interface{}), chIn, chOut chan interface{}) {
			x(chIn, chOut)
			wg.Done()
			close(out)
		}(function, in, out)

		in = out
	}
	wg.Wait()
}
