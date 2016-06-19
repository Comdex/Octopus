package octopus

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type cachedWorker struct {
	pool *WorkPool
	jobChannel chan Future
	stop chan bool
}

func newCacheWorker(cachedpool *WorkPool) *cachedWorker {
	return &cachedWorker {
		pool : cachedpool ,
		jobChannel : make(chan Future) ,
		stop :  make(chan bool) ,
	}
}

func (w *cachedWorker) getStopChannel() chan bool {
	return w.stop
}

func (w *cachedWorker) getJobChannel() chan Future {
	return w.jobChannel
}

func (w *cachedWorker) start() {
	timer := time.NewTimer(w.pool.keepAliveTime)
	go func() {
		var job Future
		for {
			timer.Reset(w.pool.keepAliveTime)
			w.pool.workerChannel <- w
		
			select {
				case job = <- w.jobChannel :
					w.pool.activeCount = w.pool.activeCount + 1
					w.pool.wg.Add(1)
					job.execute()
					w.pool.wg.Done()
					w.pool.activeCount = w.pool.activeCount - 1
					w.pool.completedJobCount = w.pool.completedJobCount + 1
				// get job timeout	
				case <- timer.C :
					
					if w.pool.poolSize > w.pool.minPoolSize {
						w.pool.poolSize = w.pool.poolSize - 1
						return
					} else {
						
						select {
							case  job = <- w.jobChannel :
								w.pool.activeCount = w.pool.activeCount + 1
								w.pool.wg.Add(1)
								job.execute()
								w.pool.wg.Done()
								w.pool.activeCount = w.pool.activeCount - 1
								w.pool.completedJobCount = w.pool.completedJobCount + 1
							case  stop := <- w.stop :
								if stop {
									w.pool.poolSize = w.pool.poolSize - 1
									w.stop <- true
									return
								}
						}
						
					}
					
				case stop := <- w.stop :
					if stop {
						w.pool.poolSize = w.pool.poolSize - 1
						w.stop <- true
						return
					}
				
			}
		}
	}()
}


type WorkPool struct {
	workerChannel chan worker
	jobChannel chan Future
	stop chan bool
	isPoolOpen  uint32 
	isShutdownNow bool
	canDropJob bool
	wg *sync.WaitGroup
	minPoolSize uint64
	poolSize uint64 
	maxPoolSize uint64
	keepAliveTime time.Duration
	awaitWokerTime time.Duration
	activeCount uint64
	completedJobCount uint64
	logFunc LogFunc
}

// Creates a goroutine pool that reuses a fixed number of goroutines.
func NewFixWorkerPool(workerNum uint64) (workpool *WorkPool, err error) {
	workpool, err = NewBaseCachedWorkerPool(workerNum, workerNum, 60*time.Second, 1*time.Second)
	return
}

// Creates a goroutine pool that creates new goroutines as needed, but will reuse previously constructed goroutines when they are available.
func NewCachedWorkerPool() (workpool *WorkPool, err error) {
	workpool, err = NewBaseCachedWorkerPool(uint64(runtime.NumCPU()), math.MaxUint64, 60*time.Second, 1*time.Second)
	return
}

// Creates a goroutine pool with MinPoolSize , MaxPoolSize, the KeepAliveTime of a worker, the time of manager await worker.
// Please note that the KeepAliveTime must be greater than one second.
func NewBaseCachedWorkerPool(MinPoolSize uint64, MaxPoolSize uint64, KeepAliveTime time.Duration, AwaitWokerTime time.Duration) (workpool *WorkPool, err error) {
	if MaxPoolSize == 0 {
		err = ErrInvalidArguments
		return
	}
	
	if KeepAliveTime < 1*time.Second {
		err = ErrKeepAliveTimeArguments
		return
	} 
	
	wc := make(chan worker, MinPoolSize)
	jc := make(chan Future, MinPoolSize)
	sc := make(chan bool)
	
	pool := &WorkPool {
		workerChannel : wc ,
		jobChannel : jc ,
		stop : sc,
		isPoolOpen : poolOpen ,
		isShutdownNow : false ,
		canDropJob : false ,
		wg : &sync.WaitGroup{},
		minPoolSize : MinPoolSize, 
		poolSize : MinPoolSize, 
		maxPoolSize : MaxPoolSize,
		activeCount : 0,
		keepAliveTime : KeepAliveTime,
		awaitWokerTime : AwaitWokerTime, 
		completedJobCount : 0,

	}
	
	for i := 0; i < int(MinPoolSize); i++ {
		w := newCacheWorker(pool)
		w.start()
	}
	go pool.manager()
	workpool = pool
	return 
}

func (pool *WorkPool) manager() {
	pool.log("WorkPool Manager : started")
	timer := time.NewTimer(pool.awaitWokerTime)
	for {
		timer.Reset(pool.awaitWokerTime)
		select {
			
		case w  := <- pool.workerChannel :
			select {
				case job := <- pool.jobChannel :
					w.getJobChannel() <- job
				default:
					pool.workerChannel <- w
			}
		// if await worker timeout	
		case <-timer.C :
			if  !pool.canDropJob {
				if pool.GetPoolSize() < pool.GetMaxPoolSize() {
					pool.log("WorkPool Manager : add routine")
					worker := newCacheWorker(pool)
					worker.start()
					pool.poolSize = pool.poolSize + 1
				}
			} else {
				select {
					case <- pool.jobChannel :
						pool.log("WorkPool Manager : drop job")
					default:
				}
			}
			
			
		case stop := <- pool.stop :
			if stop {
				pool.log("WorkPool Manager : stop pool begin")
				close(pool.jobChannel)
				if len(pool.jobChannel) > 0 {
					for j := range pool.jobChannel {
						w  := <- pool.workerChannel
						
						w.getJobChannel() <- j
					}
				}
				if pool.isShutdownNow {
					// wait for all job done
					pool.wg.Wait()
				}
				
				// close all worker
				for pool.poolSize != 0 {
					worker := <- pool.workerChannel

					worker.getStopChannel() <- true
					<-worker.getStopChannel()
				}
				
				pool.stop <- true
				pool.log("WorkPool Manager : stop pool end")
				return
			}
		}
	}
}

// Set drop job if await worker timeout, it will drop jobs when manager appears awaitWorkerTime timeout .
func (pool *WorkPool) SetDropJob(ok bool) {
	pool.canDropJob = ok
}

// CanDropJob will return if the manager will drop jobs when pool is busy.
func (pool *WorkPool) CanDropJob() bool {
	return pool.canDropJob
}

func (pool *WorkPool) log(msg string) {
	if pool.logFunc != nil {
		pool.logFunc(msg)
	}
}

// Return approximate total number of goroutines in pool.
func (pool *WorkPool) GetPoolSize() uint64 {
	return pool.poolSize
}

// Return the approximate total number of woker executing a job in pool.
func (pool *WorkPool) GetActiveCount() uint64 {
	return pool.activeCount
}

// Return the approximate total number of jobs that have completed execution.
func (pool *WorkPool) GetCompletedJobCount() uint64 {
	return pool.completedJobCount
}

// if pool is close it will return true.
func (pool *WorkPool) IsShutDown() bool {
	return (atomic.LoadUint32(&pool.isPoolOpen) == poolClose)
}

// Set a log function to record log infos.
func (pool *WorkPool) SetLogFunc(function LogFunc) {
	pool.logFunc = function
}

// Set the minimum number of goroutines.
func (pool *WorkPool) SetMinPoolSize(minPoolSize uint64)  {
	pool.minPoolSize = minPoolSize
}

// Return the minimum number of goroutines.
func (pool *WorkPool) GetMinPoolSize() uint64 {
	return pool.minPoolSize
}

// Set the maximum allowed number of goroutines.
func (pool *WorkPool) SetMaxPoolSize(maxPoolSize uint64) {
	pool.maxPoolSize = maxPoolSize
}

// Return the maximum allowed number of goroutines.
func (pool *WorkPool) GetMaxPoolSize() uint64 {
	return pool.maxPoolSize
}

// Return the KeepAliveTime of a worker.
func (pool *WorkPool) GetKeepAliveTime() time.Duration {
	return pool.keepAliveTime
}

// Set the KeepAliveTime of a worker. Please note that it must be greater than one second.
func (pool *WorkPool) SetKeepAliveTime(keepAliveTime time.Duration) error  {
	if keepAliveTime < 1*time.Second {
		return ErrKeepAliveTimeArguments
	}
	pool.keepAliveTime = keepAliveTime
	return nil
}

// Return the awaitWorkerTime of pool manager.
func (pool *WorkPool) GetAwaitWorkerTime() time.Duration {
	return pool.awaitWokerTime
}

// Submit a Runnable job for execution and return a Future representing that job.
func (pool *WorkPool) SubmitRunnable(job Runnable) (future Future, err error) {
	if pool.IsShutDown() {
		err = ErrPoolShutdown
		return 
	}
          future = newRunnableFuture(pool, job)
	pool.jobChannel <- future
	return 
}

// Submit a Callable job for execution and return a Future representing that job.
func (pool *WorkPool) SubmitCallable(job Callable) (future Future, err error) {
	if pool.IsShutDown() {
		err = ErrPoolShutdown
		return 
	}
          future = newCallableFuture(pool, job)
	pool.jobChannel <- future
	return 
}

// Submit Runnable jobs for execution and return Futures representing those jobs.
func (pool *WorkPool) InvokeAllRunnable(jobs []Runnable) (futures []Future,err error) {
	if pool.IsShutDown() {
		err = ErrPoolShutdown
		return 
	}
	futures = make([]Future,len(jobs))
	for i,j := range jobs {
		f , _ := pool.SubmitRunnable(j)
		futures[i] = f
	}
	return
}

// Submit Callable jobs for execution and return Futures representing those jobs.
func (pool *WorkPool) InvokeAllCallable(jobs []Callable) (futures []Future,err error) {
	if pool.IsShutDown() {
		err = ErrPoolShutdown
		return 
	}
	futures = make([]Future,len(jobs))
	for i,j := range jobs {
		f , _ := pool.SubmitCallable(j)
		futures[i] = f
	}
	return
}

// Close the pool and wait for all goroutines done, it may be block.
func (pool *WorkPool) Shutdown() {
	if pool.IsShutDown() {
		return
	}
	atomic.SwapUint32(&pool.isPoolOpen, poolClose)
	
	pool.isShutdownNow = false
	pool.stop <- true
	<- pool.stop
	
	close(pool.stop)
}

// Close the pool but will not wait for all goroutines done, it will be never block.
func (pool *WorkPool) ShutdownNow() {
	if pool.IsShutDown() {
		return
	}
	atomic.SwapUint32(&pool.isPoolOpen, poolClose)
	
	pool.isShutdownNow = true
	pool.stop <- true
	close(pool.stop)
}
