package octopus

import(
	"time"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

type dataWorker struct {
	pool *DataProcessPool
	jobChannel chan Future
	stop chan bool
}

func (w *dataWorker) getStopChannel() chan bool {
	return w.stop
}

func (w *dataWorker) getJobChannel() chan Future {
	return w.jobChannel
}

func executeData(pool *DataProcessPool, future Future) interface{} {
	defer func() {
		if err := recover(); err != nil {
			future.setStatus(JOBCRASH)
			pool.log("panic: " + err.(string))
			
		}	
	}()
	result := pool.fn(future.getData())
	return result
}

func (w *dataWorker) start() {
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
					result :=executeData(w.pool, job)
					job.getResultChannel() <- result
					w.pool.wg.Done()
					w.pool.activeCount = w.pool.activeCount - 1
					w.pool.completedJobCount = w.pool.completedJobCount + 1
				// if get job timeout	
				case <- timer.C :
					
					if w.pool.poolSize > w.pool.minPoolSize {
						w.pool.poolSize = w.pool.poolSize - 1
						return
					} else {
						
						select {
							case  job = <- w.jobChannel :
								w.pool.activeCount = w.pool.activeCount + 1
								w.pool.wg.Add(1)
								result := w.pool.fn(job.getData())
								job.getResultChannel() <- result
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

func newDataWorker(dataprocesspool *DataProcessPool) *dataWorker {
	return &dataWorker {
		pool : dataprocesspool ,
		jobChannel : make(chan Future) ,
		stop :  make(chan bool) ,
	}
}

type DataProcessPool struct {
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
	fn DataProcessFunc
}

// Creates a goroutine pool for processing data by defining a DataProcessFunction that reuses a fixed number of goroutines.
func NewFixDataProcessPool(workerNum uint64, fn DataProcessFunc) (dataProcessPool *DataProcessPool, err error) {
	dataProcessPool, err = NewBaseDataProcessPool(workerNum, workerNum, 60*time.Second, 1*time.Second, fn)
	return
}

// Creates a goroutine pool that creates new goroutines as needed for processing data by defining a DataProcessFunction, but will reuse previously constructed goroutines when they are available.
func NewCachedDataProcessPool(fn DataProcessFunc) (dataProcessPool *DataProcessPool, err error) {
	dataProcessPool, err = NewBaseDataProcessPool(uint64(runtime.NumCPU()), math.MaxUint64, 60*time.Second, 1*time.Second, fn)
	return
}

// Creates a goroutine pool for processing data by defining a DataProcessFunction with MinPoolSize , MaxPoolSize, the KeepAliveTime of a worker, the time of manager await worker.
// Please note that the KeepAliveTime must be greater than one second.
func NewBaseDataProcessPool(MinPoolSize uint64, MaxPoolSize uint64, KeepAliveTime time.Duration, AwaitWokerTime time.Duration, ProcessFn DataProcessFunc) (dataProcessPool *DataProcessPool, err error) {
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
	
	pool := &DataProcessPool {
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
		fn : ProcessFn,
	}
	
	for i := 0; i < int(MinPoolSize); i++ {
		w := newDataWorker(pool)
		w.start()
	}
	
	go pool.manager()
	dataProcessPool = pool
	return 
}

func (pool *DataProcessPool) manager() {
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
			
		case <-timer.C :
			if  !pool.canDropJob {
				if pool.GetPoolSize() < pool.GetMaxPoolSize() {
					pool.log("WorkPool Manager : add routine")
					worker := newDataWorker(pool)
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
					pool.wg.Wait()
				}
				
				
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

// Set drop job data if await worker timeout, it will drop job datas when manager appears awaitWorkerTime timeout.
func (pool *DataProcessPool) SetDropJob(ok bool) {
	pool.canDropJob = ok
}

// CanDropJob will return if the manager will drop job datas when pool is busy.
func (pool *DataProcessPool) CanDropJob() bool {
	return pool.canDropJob
}


func (pool *DataProcessPool) log(msg string) {
	if pool.logFunc != nil {
		pool.logFunc(msg)
	}
}

// Return approximate total number of goroutines in pool.
func (pool *DataProcessPool) GetPoolSize() uint64 {
	return pool.poolSize
}

// Return the approximate total number of woker executing a job data in pool.
func (pool *DataProcessPool) GetActiveCount() uint64 {
	return pool.activeCount
}

// Return the approximate total number of jobs that have completed execution.
func (pool *DataProcessPool) GetCompletedJobCount() uint64 {
	return pool.completedJobCount
}

// if pool is close it will return true.
func (pool *DataProcessPool) IsShutDown() bool {
	return (atomic.LoadUint32(&pool.isPoolOpen) == poolClose)
}

// Set a log function to record log infos.
func (pool *DataProcessPool) SetLogFunc(function LogFunc) {
	pool.logFunc = function
}

// Set the minimum number of goroutines.
func (pool *DataProcessPool) SetMinPoolSize(minPoolSize uint64)  {
	pool.minPoolSize = minPoolSize
}

// Return the minimum number of goroutines.
func (pool *DataProcessPool) GetMinPoolSize() uint64 {
	return pool.minPoolSize
}

// Set the maximum allowed number of goroutines.
func (pool *DataProcessPool) SetMaxPoolSize(maxPoolSize uint64) {
	pool.maxPoolSize = maxPoolSize
}

// Return the maximum allowed number of goroutines.
func (pool *DataProcessPool) GetMaxPoolSize() uint64 {
	return pool.maxPoolSize
}

 // Return the KeepAliveTime of a worker.
func (pool *DataProcessPool) GetKeepAliveTime() time.Duration {
	return pool.keepAliveTime
}

// Set the KeepAliveTime of a worker. Please note that it must be greater than one second.
func (pool *DataProcessPool) SetKeepAliveTime(keepAliveTime time.Duration) error {
	if keepAliveTime < 1*time.Second {
		return ErrKeepAliveTimeArguments
	}
	pool.keepAliveTime = keepAliveTime
	return nil
}

// Return the awaitWorkerTime of pool manager.
func (pool *DataProcessPool) GetAwaitWorkerTime() time.Duration {
	return pool.awaitWokerTime
}

// Submit a job data for execution and return a Future representing the calculating result of  that job data.
func (pool *DataProcessPool) Submit(job interface{}) (future Future, err error) {
	if pool.IsShutDown() {
		err = ErrPoolShutdown
		return 
	}
          future = newDataFuture(pool, job)
	pool.jobChannel <- future
	return 
}

// Close the pool and wait for all goroutines done, it may be block.
func (pool *DataProcessPool) Shutdown() {
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
func (pool *DataProcessPool) ShutdownNow() {
	if pool.IsShutDown() {
		return
	}
	atomic.SwapUint32(&pool.isPoolOpen, poolClose)
	
	pool.isShutdownNow = true
	pool.stop <- true
	close(pool.stop)
}
