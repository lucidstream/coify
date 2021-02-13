package csync

import (
	_map "github.com/lucidStream/coify/shards/map"
	"math"
	"math/rand"
	"sync"
)


//仅适用于单机环境

type KeyLock struct {
	MutexSync   *_map.ShardsMap
}



var(
	mutexPool     = sync.Pool{
		New:func() interface{} {
			return &mulock{}
		},
	}
)



func NewKeyLock( chunkSize uint64,mpLen uint64 ) KeyLock {
	x := _map.NewShardsMap(chunkSize,mpLen)
	var a KeyLock
	a.MutexSync = x
	return a
}



func (this *KeyLock) LockLocal( key string ) int64 {
	chunk := this.MutexSync.MatchChunk(key)
	chunk.Lock.Lock()

	var vmulock *mulock
	var version int64

	if chunk.Mp[key] == nil {
		vmulock = mutexPool.Get().(*mulock)
		if vmulock.ref_count != 0 {
			chunk.Lock.Unlock()
			panic("refer count error")
		}
		vmulock.removed = NoRemove
		vmulock.version = int64(rand.Intn(int(math.MaxUint32)))
		chunk.Mp[key] = vmulock
	} else {
		vmulock = chunk.Mp[key].(*mulock)
	}

	vmulock.ref_count++
	version = vmulock.version

	//+1完成后解锁，其他获取操作可以正常完成
	chunk.Lock.Unlock()

	//使用map上对应的key进行锁定
	//得不到的阻塞在map上对应的key，而不应该阻塞在保护获取操作的锁上
	//因为保护获取操作的锁资源有限，不适合处理锁定过程时间过长的业务
	vmulock.mutex.Lock()

	//得到锁的返回锁对象版本
	return version
}





//解锁本地
//不存在高频并发调用
func (this *KeyLock) UnLockLocal( key string,version int64 ) {
	chunk := this.MutexSync.MatchChunk(key)
	chunk.Lock.Lock()

	var vmulock *mulock = chunk.Mp[key].(*mulock)
	//检查锁版本是否一致
	if vmulock.version != version {
		chunk.Lock.Unlock()
		panic("version error")
	}

	vmulock.ref_count--
	if vmulock.ref_count < 0 {
		chunk.Lock.Unlock()
		panic("ref_count less than zero")
	}
	if vmulock.ref_count == 0 && vmulock.removed == NoRemove {
		vmulock.removed = RemoveIng
		delete(chunk.Mp,key)
		vmulock.removed = Removed
	}

	if vmulock.ref_count > 0 || vmulock.removed != Removed {
		vmulock.mutex.Unlock()
		chunk.Lock.Unlock()
		return
	}

	vmulock.mutex.Unlock()
	chunk.Lock.Unlock()

	vmulock.version = -1
	mutexPool.Put(vmulock)
}
