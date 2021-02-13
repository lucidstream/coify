package _struct

import "sync"

type StatusNotify struct{
	cond          *sync.Cond //全局条件
	status        int64 //全局状态
}



//status初始状态
func NewStatusNotify( status int64 ) StatusNotify {
	var notify StatusNotify
	notify.status = status
	notify.cond   = sync.NewCond(&sync.Mutex{})
	return notify
}



//当状态不再为指定值时触发
func (this *StatusNotify) OnChnage( old int64 ) <-chan int64 {
	channel := make(chan int64,1)
	go func() {
		this.cond.L.Lock()
		for this.status == old {
			this.cond.Wait()
		}
		this.cond.L.Unlock()
		channel <- this.status
		close(channel)
	}()
	return channel
}



//当状态为指定值时触发
func (this *StatusNotify) OnEqual( s int64 ) <-chan struct{} {
	channel := make(chan struct{},1)
	go func() {
		this.cond.L.Lock()
		for this.status != s {
			this.cond.Wait()
		}
		this.cond.L.Unlock()
		channel <- struct{}{}
		close(channel)
	}()
	return channel
}



//修改状态，多线程需使用广播模式
//存在唤起其中一个但条件并不成立的情况
func (this *StatusNotify) Change( new int64,broadcast bool ) {
	this.cond.L.Lock()
	this.status = new
	this.cond.L.Unlock()
	if broadcast == true {
		this.cond.Broadcast()
	} else {
		this.cond.Signal()
	}
}




