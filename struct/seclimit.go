package _struct

import (
	"errors"
	"golang.org/x/net/context"
	"sync"
	"time"
)


type SecLimit struct {
	Cap        int64//当前容量
	resum      int64//恢复容量
	_cap       int64//记录原始容量，用于更改后恢复
	lock       *sync.Mutex
	cond       *sync.Cond
	ctx        context.Context
	cancel     context.CancelFunc
}


func NewSecLimit( cap int64,lock *sync.Mutex,ctx context.Context ) *SecLimit {
	sec := SecLimit{
		Cap   : cap,
		resum : cap,
		_cap  : cap,
		lock  : lock,
	}
	sec.ctx,sec.cancel = context.WithCancel(ctx)
	sec.cond = sync.NewCond(sec.lock)
	return &sec
}



func (this *SecLimit) Reset() {
	this.cond.L.Lock()
	this.Cap = this.resum
	this.cond.Signal()//不能使用唤起所有，避免产生惊群
	this.cond.L.Unlock()
}



//临时更改cap
func (this *SecLimit) WeakCap( cap int64, after func(sec *SecLimit) ) {
	this.cond.L.Lock()
	this.Cap   = cap
	this.resum = cap
	this.cond.Signal()
	this.cond.L.Unlock()
	after(this)
}



//恢复原始cap值
func (this *SecLimit) ResumeOrgiCap() {
	this.cond.L.Lock()
	this.Cap   = this._cap
	this.resum = this._cap
	this.cond.Signal()
	this.cond.L.Unlock()
}




var (
	SecClosedErr   = errors.New("seclimit closed")
	NoEnoughCapErr = errors.New("no enough cap")
)



//go:noinline
func (this *SecLimit) Get( block bool ) error {
	this.cond.L.Lock()
	for this.Cap <= 0 {
		select{
		case <-this.ctx.Done():
			this.cond.Broadcast()
			this.cond.L.Unlock()
			return SecClosedErr
		default:
		}
		if ! block {
			this.cond.L.Unlock()
			return NoEnoughCapErr
		} else {
			this.cond.Wait()
		}
	}
	this.Cap--
	if this.Cap > 0  { this.cond.Signal() }
	this.cond.L.Unlock()
	//需要一个引起中断的调用保证分配的公平性
	//time.Sleep(1)
	//runtime.Gosched()
	select{
	case <-this.ctx.Done():
		this.cond.Broadcast()
		return SecClosedErr
	default:
	}
	return nil
}




//定时生产令牌函数
func (this *SecLimit) Produce( sec time.Duration ) {
	go func( sec time.Duration ) {
		timer := time.NewTicker(sec)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				this.Reset()
			case <-this.ctx.Done():
				this.cond.Broadcast()
				return
			}
		}
	}(sec)
}



//当调用.Wait()后，资源保护锁将解开
//也就是说要进入真正的wait状态，锁是必须解开的
//那么关闭方法必然可以有时机获得条件变量的资源锁
//获得资源锁后发关闭信号，可以防止在检查关闭信号前，进入沉睡
func (this *SecLimit) Close() {
	this.cond.L.Lock()
	this.cancel()
	//广播叫起来检查取消信号
	this.cond.Broadcast()
	this.cond.L.Unlock()
}
