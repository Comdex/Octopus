package octopus

import (
	"errors"
	"time"
)

const(
	JOBUNSTART uint32 = 0
	JOBDOING uint32 = 1
	JOBDONE uint32 = 2
	JOBCRASH uint32 = 3
)

const(
	poolOpen uint32 = 4
	poolClose uint32 = 5
)

var(
	ErrPoolShutdown     = errors.New("the pool is closed")
	ErrJobTimedOut        = errors.New("job request timed out")
	ErrRunnableNoResult = errors.New("runnable job has not a result value")
	ErrResultChannelClose = errors.New("result channel close")
	ErrInvalidArguments   = errors.New("Invalid Arguments")
	ErrKeepAliveTimeArguments = errors.New("KeepAliveTime must be greater than 1 second")
)

type Runnable func()

type Callable func() (interface{})

type LogFunc func(string)

type DataProcessFunc func(interface{}) interface{}

// a worker represents a goroutine
type worker interface {
	start()
	getStopChannel() chan bool
	getJobChannel() chan Future
}

// a Future represents the result of an asynchronous computation. 
type Future interface {
	// Wait for the job done, and then retrieves its result.
	Get() (interface{},error)
	// Wait for the result by setting timeout
	GetTimed(time.Duration) (interface{},error)
	// Get the execution status of the job, it may be JOBUNSTART, JOBDOING,JOBDONE or JOBCRASH
	Status() uint32
	// Return if the job is done
	IsDone() bool
	execute()
	setStatus(uint32)
	getData() interface{}
	getResultChannel() chan interface{}
}

type dataFuture struct {
	pool *DataProcessPool
	data interface{}
	resultChannel chan interface{}
	status uint32
}

func newDataFuture(dataprocesspool *DataProcessPool, jobData interface{}) Future {
	return &dataFuture {
		pool : dataprocesspool ,
		data : jobData ,
		resultChannel : make(chan interface{}, 1) ,
		status : JOBUNSTART,
	}
}

func (f *dataFuture) setStatus(s uint32) {
	f.status = s
}

func(f *dataFuture) getResultChannel() chan interface{} {
	return f.resultChannel
}

func (f *dataFuture) getData() interface{} {
	return f.data
}

func (f *dataFuture) execute() {
	
}

func (f *dataFuture) Get() (result interface{}, err error) {
	
	var ok bool
	result,ok  = <- f.resultChannel
	if !ok {
		err = ErrResultChannelClose
	}  else {
		close(f.resultChannel)
	}
	
	return
		
}

func (f *dataFuture) GetTimed(timeout time.Duration) (result interface{}, err error) {
	
	var ok bool
	select {
		case result,ok = <- f.resultChannel :
			if !ok {
				err = ErrResultChannelClose
			}  else {
				close(f.resultChannel)
			}
			return
		case <- time.After(timeout*time.Millisecond) :
			err = ErrJobTimedOut
			return 
	}
		
}
func (f *dataFuture) Status() uint32 {
	return f.status
}

func (f *dataFuture) IsDone() bool {
	return f.status == JOBDONE
}

type callableFuture struct {
	pool *WorkPool
	job Callable
	resultChannel chan interface{}
	status uint32
}
func newCallableFuture(workpool *WorkPool, fn Callable) Future {
	return &callableFuture{
			pool: workpool,
			job: fn,
			resultChannel: make(chan interface{}, 1),
			status: JOBUNSTART,
	}
}

func (f *callableFuture) setStatus(s uint32) {
	f.status = s
}

func(f *callableFuture) getResultChannel() chan interface{} {
	return f.resultChannel
}

func (f *callableFuture) getData() interface{} {
	return nil
}
func (f *callableFuture) execute() {
	defer func() {
		if err := recover(); err != nil {
			f.status = JOBCRASH
			f.pool.log("panic: " + err.(string))
			
		}	
	}()
	
	f.status = JOBDOING
			
	result := f.job()
	f.resultChannel <- result
	
	f.status = JOBDONE
}
func (f *callableFuture) Get() (result interface{}, err error) {
	
	var ok bool
	result,ok  = <- f.resultChannel
	if !ok {
		err = ErrResultChannelClose
	}  else {
		close(f.resultChannel)
	}
	return
		
}
func (f *callableFuture) GetTimed(timeout time.Duration) (result interface{}, err error) {
	
	var ok bool
	select {
		case result,ok = <- f.resultChannel :
			if !ok {
				err = ErrResultChannelClose
			} else {
				close(f.resultChannel)
			}
			return
		case <- time.After(timeout*time.Millisecond) :
			err = ErrJobTimedOut
			return 
	}
		
}
func (f *callableFuture) Status() uint32 {
	return f.status
}

func (f *callableFuture) IsDone() bool {
	return f.status == JOBDONE
}

type runnableFuture struct {
	pool *WorkPool
	job Runnable
	resultChannel chan interface{}
	status uint32
}
func newRunnableFuture(workpool *WorkPool, fn Runnable) Future {
	return &runnableFuture{
			pool: workpool,
			job: fn,
			resultChannel: make(chan interface{}, 1),
			status: JOBUNSTART,
	}
}

func (f *runnableFuture) setStatus(s uint32) {
	f.status = s
}

func (f *runnableFuture) getResultChannel() chan interface{} {
	return f.resultChannel
}

func (f *runnableFuture) getData() interface{} {
	return nil
}

func (f *runnableFuture) execute() {
	defer func() {
		if err := recover(); err != nil {
			f.status = JOBCRASH
			f.pool.log("panic: " + err.(string))
			
		}	
	}()
	
	f.status = JOBDOING
						
	f.job()
			
	f.status = JOBDONE
}
func (f *runnableFuture) Get() (result interface{}, err error) {
	
	err = ErrRunnableNoResult
	return
		
}
func (f *runnableFuture) GetTimed(timeout time.Duration) (result interface{}, err error) {
	
	err = ErrRunnableNoResult
	return
		
}
func (f *runnableFuture) Status() uint32 {
	return f.status
}

func (f *runnableFuture) IsDone() bool {
	return f.status == JOBDONE
}
