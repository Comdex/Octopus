// octopus project octopus.go
package octopus

import (
	"runtime"
	"errors"
	"time"
	"sync/atomic"
)

const(
	JOBUNSTART uint32 = 0
	JOBDOING uint32 = 1
	JOBDONE uint32 = 2
)

const(
	UNINTERRUPT uint32 = 3
	INTERRUPT uint32 = 4
)

const(
	WORKERFREE uint32 = 5
	WORKERRUNNING uint32 = 6
	WORKERSTOP uint32 = 7
)

const(
	POOLOPEN uint32 = 8
	POOLCLOSE uint32 = 9
)

type Runnable func()

type Callable func() (interface{})

type Future interface {
	// Cancel method will set a cancel tag attempt to cancel execute job before starting this job represented by Future.
	Cancel() error
	// Get method can get value from Callable , if not ready it will block.
	Get() (interface{},error)
	// GetTimed method can get value from Callable with setting timeout.
	GetTimed(time.Duration) (interface{},error)
	// IsCancelled will return whether the job was setting a cancel tag, but it does not mean that the job has been terminated.
	IsCancelled() bool//default 0 ,interrupt 1
	// IsDone will return whether the job was done.
	IsDone() bool
}

type futureTask struct {
	job interface{}
	resultChannel chan interface{}
	interrupt uint32
	status  uint32
}
func newfutureTask(fn interface{}) *futureTask {
	return &futureTask{
			job: fn,
			resultChannel: make(chan interface{}),
			interrupt: UNINTERRUPT,
			status: JOBUNSTART,
	}
}

// free worker will call execute method
func (f *futureTask) execute() {
	if f.IsCancelled() {
		return
	}
	switch j := f.job.(type) {
		case Runnable :
			atomic.SwapUint32(&f.status, JOBDOING)
			j()
			atomic.SwapUint32(&f.status, JOBDONE)
		case Callable :
			atomic.SwapUint32(&f.status,JOBDOING)
			result := j()
			f.resultChannel  <-  result
			atomic.SwapUint32(&f.status, JOBDONE)
		default :
	}
}
func (f *futureTask) Cancel() error {
	if f.Status() == JOBDONE {
		return errors.New("has done")
	}
	atomic.SwapUint32(&f.interrupt,INTERRUPT)
	return nil
}

func (f *futureTask) Get() (result interface{}, err error) {
	switch f.job.(type) {
		case Runnable :
			return
		case Callable :
			var ok bool
			result,ok  = <- f.resultChannel
			if !ok {
				return nil, errors.New("channel close")
			}
			return
	}
	return
}
func (f *futureTask) GetTimed(timeout time.Duration) (result interface{}, err error) {
	switch f.job.(type) {
		case Runnable :
			return
		case Callable :
			var ok bool
			select {
				case result,ok = <- f.resultChannel :
					if !ok {
						return nil, errors.New("channel close")
					}
					return
				case <- time.After(timeout*time.Millisecond) :
					return nil, errors.New("timeout")
			}
	}
	return
}
func (f *futureTask) IsCancelled() bool {
	return (atomic.LoadUint32(&f.interrupt) == INTERRUPT)
}
func (f *futureTask) Status() uint32 {
	return atomic.LoadUint32(&f.status)
}

type worker struct {
	jobChannel chan *futureTask
	stop chan bool
	status uint32
}
func newWorker( s chan bool) *worker {
	return &worker {
		jobChannel : make(chan *futureTask) ,
		stop : s ,
		status : WORKERSTOP ,
	}
}
func newWorkerForCachedPool() *worker {
	return &worker {
		jobChannel : make(chan *futureTask) ,
		stop : make(chan bool),
		status : WORKERSTOP ,
	}
}
func (w *worker) status() {
	return atomic.LoadUint32(&w.status) 
}
func (w *worker) start(pool WorkPool) {
	atomic.SwapUint32(&w.status, WORKERFREE)
	go func() {
		var job *futureTask
		for {
			// worker free, add it to pool
			//pool.workerChannel <- w
			pool.getWorkerChannel() <- w
			select {
				case job = <- w.jobChannel :
					atomic.SwapUint32(&w.status, WORKERRUNNING)
					job.execute()
					atomic.SwapUint32(&w.status,WORKERFREE)
				case stop := <- w.stop:
					if stop {
						//w.stop <- true
						atomic.SwapUint32(&w.status, WORKERSTOP)
						return
					}
				
			}
		}
	}()
}

type WorkPool interface {
	IsPoolOpen() bool
	Close()
	Release()
	SubmitRunnable(Runnable) (Future, error)
	SubmitCallable(job Callable) (Future, error)
	InvokeAllRunnable([]Runnable) ([]Future,error)
	InvokeAllCallable([]Callable) ([]Future,error)
	getWorkerChannel() chan *worker
}

type cachedWorkerPool struct {
	*fixWorkerPool
	workers []*worker
}
func NewCachedWorkerPool() WorkPool {
	num := runtime.NumCPU()*20
	wc := make(chan *worker,num )
	jc := make(chan *futureTask, num)
	sc := make(chan bool)
	var pool cachedWorkerPool
	pool.workerChannel = wc
	pool.jobChannel = jc
	pool.stop = sc
	pool.poolOpen = POOLOPEN
	ws := make([]*worker, num)
	pool.workers = ws
	for i:=0; i<num; i++ {
		pool.workers[i] = newWorkerForCachedPool()
		pool.workers[i].start(pool)
	}
	
	go pool.dispatch()
	return &pool
}

func newWorkerForChannel() chan *worker {
	w := newWorkerForCachedPool()
	wc := make(chan *worker)
	wc <- w
	return wc
}

func (pool *cachedWorkerPool) dispatch() {
	for {
		select {
		case w := <- pool.workerChannel :
			job := <- pool.jobChannel
			w.jobChannel <- job
		case w := <- newWorkerForChannel() :
			pool.workers = append(pool.workers, w)
			pool.workerChannel <- w
//		case stop := <- pool.stop :
//			if stop {
//				for i := 0; i < cap(pool.workerChannel); i++ {
//					w := <- pool.workerChannel

//					w.stop <- true
//					<-w.stop
//				}

//				pool.stop <- true
//				return
//			}
		}
	}
}

type fixWorkerPool struct {
	workerChannel chan *worker
	jobChannel chan *futureTask
	stop chan bool
	poolOpen  uint32 // 1 represent pool open to receive jobs,0 to close
}

// Create a goroutine pool that can be reused for a number of fixed goroutines
func newFixedWorkerPool(numWorkers int) WorkPool {
	wc := make(chan *worker, numWorkers)
	jc := make(chan *futureTask, numWorkers)
	sc := make(chan bool)
	pool := &fixWorkerPool{
		workerChannel : wc ,
		jobChannel : jc ,
		stop : sc,
		poolOpen : POOLOPEN ,
	}
	
	for i := 0; i < cap(pool.workerChannel); i++ {
		w := newWorker(jc, sc)
		w.start(pool)
	}
	
	go pool.dispatch()
	
	return pool
}

func (pool *fixWorkerPool) getWorkerChannel() chan *worker {
	return pool.workerChannel
}

func (pool *fixWorkerPool) dispatch() {
	for {
		select {
		case job := <- pool.jobChannel :
			w  := <- pool.workerChannel
			w.jobChannel <- job
		case stop := <- pool.stop :
			if stop {
				for i := 0; i < cap(pool.workerChannel); i++ {
					w := <- pool.workerChannel

					w.stop <- true
					<-w.stop
				}

				pool.stop <- true
				return
			}
		}
	}
}
func (pool *fixWorkerPool) IsPoolOpen() bool {
	return (atomic.LoadUint32(&pool.poolOpen) == POOLOPEN)
}
func (pool *fixWorkerPool) Close() {
	atomic.SwapUint32(&pool.poolOpen, POOLCLOSE)
}
func (pool *fixWorkerPool) Release() {
	if pool.IsPoolOpen() {
		pool.Close()
	}
	pool.stop <- true
	<- pool.stop
	close(pool.jobChannel)
	close(pool.workerChannel)
	close(pool.stop)
}
func (pool *fixWorkerPool) SubmitRunnable(job Runnable) (future Future, err error) {
	if !pool.IsPoolOpen() {
		return nil,errors.New("pool has closed")
	}
          f := newfutureTask(job)
	future = f
	pool.jobChannel <- f
	return 
}
func (pool *fixWorkerPool) SubmitCallable(job Callable) (future Future, err error) {
	if !pool.IsPoolOpen() {
		return nil,errors.New("pool has closed")
	}
          f := newfutureTask(job)
	future = f
	pool.jobChannel <- f
	return 
}
func (pool *fixWorkerPool) InvokeAllRunnable(jobs []Runnable) (futures []Future,err error) {
	if !pool.IsPoolOpen() {
		err = errors.New("pool has closed")
		return 
	}
	futures = make([]Future,len(jobs))
	for i,j := range jobs {
		f := newfutureTask(j)
		pool.jobChannel <- f
		futures[i] = f
	}
	return
}
func (pool *fixWorkerPool) InvokeAllCallable(jobs []Callable) (futures []Future,err error) {
	if !pool.IsPoolOpen() {
		err = errors.New("pool has closed")
		return 
	}
	futures = make([]Future,len(jobs))
	for i,j := range jobs {
		f := newfutureTask(j)
		pool.jobChannel <- f
		futures[i] = f
	}
	return
}
