package csync

import (
	_map "github.com/lucidStream/coify/shards/map"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)


const(
	NoRemove          = 0
	RemoveIng         = 1
	Removed           = 2
)


type QueryShare struct {
	Result         interface{}
	Err            error
}


type ProtectShared interface {
	AddRefer() error
	Subtract() error
	AfterPut() error
	GetRefer() (int64,error)
}


type wglock struct {
	wg             sync.WaitGroup
	version        int64
	waiter         int64
	hisWaiter      int64
	removed        int64
	shared         QueryShare
	ref_count      int64
}



type mulock struct {
	mutex          sync.Mutex
	version        int64
	removed        int64
	ref_count      int64
}



type SynchShare struct {
	waitGroupSync        *_map.ShardsMap
}



var (
	InterfaceNil  = (interface{})(nil)
	wgPool        = sync.Pool{
		New:func() interface{} {
			return &wglock{}
		},
	}
)



func NewSynchShare( chunkSize uint64,mpLen uint64 ) *SynchShare {
	x := _map.NewShardsMap(chunkSize,mpLen)
	var a SynchShare
	a.waitGroupSync = x
	return &a
}



func (this *SynchShare) GetWaitGroup( key string ) (*wglock,int64,bool) {
	var vwglock *wglock
	var isFirst bool = false

	chunk := this.waitGroupSync.MatchChunk(key)
	chunk.Lock.Lock()
	defer chunk.Lock.Unlock()

	if chunk.Mp[key] == nil {
		vwglock = wgPool.Get().(*wglock)
		if vwglock.ref_count != 0 {
			panic("refer count error")
		}
		if vwglock.waiter != 0 {
			panic("waiter count error")
		}
		vwglock.hisWaiter = 0
		vwglock.removed   = NoRemove
		vwglock.version   = int64(rand.Intn(int(math.MaxUint32)))
		//置空共享结果
		vwglock.shared.Result = nil
		vwglock.shared.Err    = nil
		chunk.Mp[key] = vwglock
		isFirst = true
	} else {
		vwglock = chunk.Mp[key].(*wglock)
	}

	vwglock.ref_count++
	vwglock.wg.Add(1)

	if false == isFirst {
		vwglock.hisWaiter++
		vwglock.waiter++
	}

	return vwglock,vwglock.version,isFirst
}




func (this *SynchShare) WGDone(vwglock *wglock, key string) {
	chunk := this.waitGroupSync.MatchChunk(key)
	chunk.Lock.Lock()
	defer chunk.Lock.Unlock()
	this.wgdone(vwglock,chunk,key)
}





//需外层锁保护
//go:inline
func (this *SynchShare) wgdone(vwglock *wglock,chunk *_map.SyncMap,key string) {
	vwglock.ref_count--
	//当计数为0意味着唤起所有等待者
	//在它们没有全部起来前不能再使用Add
	//因此需要先把waitGroup从Map上拿掉
	//新的请求申请新的waitGroup进行Add
	if vwglock.ref_count < 0 {
		panic("ref_count less than zero")
	}
	if vwglock.ref_count == 0 && vwglock.removed == NoRemove {
		vwglock.removed = RemoveIng
		delete(chunk.Mp,key)
		vwglock.removed = Removed
	}
	vwglock.wg.Done()
}




func (this *SynchShare) WGShared( vwglock *wglock,ret QueryShare ) {
	//增加当前对象引用计数防止共享对象被提前释放掉
	if nil != ret.Result {
		proshare,isok := ret.Result.(ProtectShared)
		if isok == true {
			_=proshare.AddRefer()
		}
	}
	vwglock.shared = ret
}




//需尽可能准确判断状态，尽可能走wait方法释放对象
func (this *SynchShare) WGUnlock(vwglock *wglock, key string,version int64) {
	chunk := this.waitGroupSync.MatchChunk(key)
	chunk.Lock.Lock()
	this.wgdone(vwglock,chunk,key)

	//Removed状态表示conter不会再增加，为0后也就永久为0了

	//conter为0有两种情况
	//并发数只有1，UnlockDone减去了自身计数后为0,从未有过waiter
	//并发数大于等于2,UnlockDone只是减去了其中一个计数，如果为0说明有waiter

	if  vwglock.removed != Removed || vwglock.hisWaiter > 0 || vwglock.ref_count > 0 {
		chunk.Lock.Unlock()
		return
	}

	chunk.Lock.Unlock()

	if vwglock.version != version {
		panic("version error")
	}
	vwglock.version    = -1
	vwglock.shared.Err = nil
	ret := vwglock.shared.Result
	vwglock.shared.Result = nil

	if nil != ret {
		proshare,isok := ret.(ProtectShared)
		if isok == true {
			_=proshare.Subtract()
		}
	}

	wgPool.Put(vwglock)
}





//需要先有add，否则直接调用waitj计数为0就退出了。这样做没有意义
func (this *SynchShare) WGWait(vwglock *wglock,key string,version int64) (res QueryShare) {
	vwglock.wg.Wait()

	res.Result = vwglock.shared.Result
	res.Err    = vwglock.shared.Err
	var proshare ProtectShared
	var isok     bool = false
	if nil != res.Result {
		proshare,isok = res.Result.(ProtectShared)
		if isok == true {
			_=proshare.AddRefer()
		}
	}

	//当唤起指令执行后，需要等所有等待者都起来后才能复用
	//vwglock.waiter--
	newwaiter := atomic.AddInt64(&vwglock.waiter,-1)

	//需要当waiter和counter都为0的时候才回收
	if  vwglock.removed != Removed || newwaiter > 0 || vwglock.ref_count > 0 {
		return
	}

	if vwglock.version != version {
		panic("version error")
	}

	//fmt.Println("hisWaiter",vwglock.hisWaiter)
	//os.Exit(0)

	vwglock.version  = -1
	vwglock.shared.Result = nil
	vwglock.shared.Err    = nil
	if isok == true && nil!=proshare {
		_=proshare.Subtract()
	}
	wgPool.Put(vwglock)
	return
}