package octopus

import (
	"strconv"
	"testing"
)

func Test_CachedWorkerPool(t *testing.T) {
	pool, _ := NewCachedWorkerPool()
	f, err := pool.SubmitCallable(func () interface{} {
		t.Log("Hi")
		return "Hi, result"
	})
	
	if err != nil {
		t.Error(err)
	}
	
	v, _ := f.Get()
	t.Log(v)
	
	pool.SubmitRunnable(func () {
		t.Log("Hello")
	})
	
	var r Runnable = func() {
		t.Log("test Runnable var")
	}
	
	pool.SubmitRunnable(r)
	
	r2 := func() {
		t.Log("test Runnable var 2")
	}
	
	pool.SubmitRunnable(r2)
	
	for i:=0; i<30; i++ {
		pool.SubmitCallable(func() interface{} {
			t.Log("num: ", i)
			return i
		})
	}
	
	// wait for all goroutine done
	pool.Shutdown()
}

func Test_CachedDataProcessPool (t *testing.T) {
	pool, _  := NewCachedDataProcessPool(func (object interface{}) interface{} {
		v := object.(string)
		return v+" end"
	})
	
	f, err := pool.Submit("test1")
	if err != nil {
		t.Error(err)
	}
	
	v, _ := f.Get()
	t.Log(v)
	
	for i:=0; i<30; i++ {
		f, err := pool.Submit(strconv.Itoa(i))
		if err != nil {
			t.Error(err)
		}
		
		v, _ := f.Get()
		t.Log(v)
	}
	
	pool.Shutdown()
}