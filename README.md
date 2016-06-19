# Octopus

Octopus is a golang library for managing a goroutine pool that can dynamic adjust the number of goroutine, the api is a bit like java concurrent pool api.

Octopus can new a pool to submit Callable job or Runnable job, Callable job is a function with a interface{} return value and no arguments, Runnable job is a function without arguments and return value, a job will be allocated to a worker when it becomes available.

## Features
1. dynamic adjust the number of goroutine according the idle of goroutine
2. support synchronous and asynchronous to get calculating result
3. support timeout to get calculating result
4. support to get status of a job
5. can drop jobs when pool is busy
6. automatic recovery from a job's  panic
7. can set a log function to record pool's log infos
8. the api is a bit like java concurrent pool and more easily to use

## Docs
https://godoc.org/github.com/Comdex/octopus

## Installation
```bash
go get github.com/Comdex/octopus
```

## Simple example for a cachedWorkerPool
```go
package main

import (
	"time"
	"fmt"
	"github.com/Comdex/octopus"
)

func main() {
	// the cachedpool will dynamic adjust the number of goroutine called worker according 
	// the timeout of workers process job and idle time of workers
	pool, err := octopus.NewCachedWorkerPool()
	if err != nil {
		fmt.Println(err)
	}
	// you can set a log func to get pool's  log info
	pool.SetLogFunc(func(msg string){
		fmt.Println(msg)
	})
	// the Runnable is a simple function
	var r Runnable = func() {
		fmt.Println("test runnable var")
	}
	pool.SubmitRunnable(r)
	// the Callable is a function with a return value
	var c Callable = func() interface{} {
		s := "test callable var"
		return 
	}
	pool.SubmitCallable(s)
	
	pool.SubmitRunnable(func(){
		fmt.Println("test1")
	})
	
	future, err2 := pool.SubmitCallable(func() interface{} {
		time.Sleep(2*time.Second)
		return "test2"
	})
	if err2 != nil {
		fmt.Println(err2)
	}
	// the Get method of future will wait for return value is prepared
	// Is it like a java concurrent pool api?
	value, err3 := future.Get()
	if err3 != nil {
		fmt.Println(err3)
	}
	fmt.Println("value: ", value)
	
	future2 , _ := pool.SubmitCallable(func() interface{} {
		time.Sleep(2*time.Second)
		return "test3"
	})
	
	//Get Value support timeout
	value2, timeoutErr := future2.GetTimed(1*time.Second)
	if timeoutErr != nil {
		fmt.Println(timeoutErr)
	}
	fmt.Println(value2)
	
	// close the pool and wait for all goroutines done
	pool.Shutdown()	
}
```

## Example for a dataprocess pool
```go
package main

import (
	"fmt"
	"github.com/Comdex/octopus"
)

func main() {
	pool, err := octopus.NewCachedDataProcessPool(func(object interface{}) interface{} {
		v := object.(int)
		return "data: " + strconv.Itoa(v)
	})
	if err != nil {
		fmt.Println(err)
	}
	
	pool.Submit(8)
	pool.Submit(29)
	
	future, err2 := pool.Submit(100)
	if err != nil {
		fmt.Println(err)
	}
	// the api is synchronous
	value, err3 := future.Get()
	if err3 != nil {
		fmt.Println(err3)
	}
	fmt.Println("100 value: ", value)
	
	future2, _ := pool.Submit(200)
	// Get method support timeout
	value2, _ := future2.GetTimed(2*time.Second)
	fmt.Println("200 value: ", value2)
	
	// close the pool and wait for all goroutine done
	pool.Shutdown()
}
```

## License
Apache License

more api usage please refer to docs

