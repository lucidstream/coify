package _map

import (
	"github.com/lucidStream/coify/helper"
	"runtime"
	"strconv"
	"sync"
	"unsafe"
	"github.com/lucidStream/coify/spool"
)




type SyncMap struct {
	Mp       map[string]interface{}
	Lock     sync.RWMutex
}


type ShardsMap struct {
	chunk        []SyncMap
	chunk_size   uint64
	mp_len       uint64
	ref_count    int64
}


//通用清理map函数
func generalClear( i interface{} ) {
	x := i.(*ShardsMap)
	var range_count = 0
	for i:=0; i < int(x.chunk_size); i++ {
		//挨个delete是交给gc去回收，何不解除原map的引用
		//可以考虑创建一个垃圾桶，如果gc可以不扫描这个垃圾桶的话
		x.chunk[i]    = SyncMap{}
		x.chunk[i].Mp = make(map[string]interface{},x.mp_len)
		range_count++
		if range_count % 40960 == 0 {
			runtime.Gosched()
		}
	}
}



type ShardsMapPoolType struct {
	Ms      map[string]*spool.RefPool
	Lock    sync.Mutex
}


var ShardsMapPool ShardsMapPoolType


func init() { ShardsMapPool.Ms = make(map[string]*spool.RefPool) }


//为避免从对象池拿到尺寸不一的map，应该以chunk_size&mp_len作为条件建立对象池
func NewShardsMap( chunkSize uint64,mpLen uint64 ) *ShardsMap {
	x := strconv.FormatUint(chunkSize,10)
	y := strconv.FormatUint(mpLen,10)
	ShardsMapPool.Lock.Lock()
	if ShardsMapPool.Ms[x+"x"+y] == nil {
		ShardsMapPool.Ms[x+"x"+y] = newReferPool(chunkSize,mpLen)
	}
	ShardsMapPool.Lock.Unlock()

	return ShardsMapPool.Ms[x+"x"+y].Get(nil).(*ShardsMap)
}



func newReferPool( chunkSize uint64,mpLen uint64 ) *spool.RefPool {
	return &spool.RefPool{
		Roffset : unsafe.Offsetof(ShardsMap{}.ref_count),
		Size_t  : unsafe.Sizeof(ShardsMap{}),
		CreateFn: func() interface{} {
			mp           := ShardsMap{}
			mp.chunk_size = chunkSize
			mp.mp_len     = mpLen
			mp.chunk      = make([]SyncMap,mp.chunk_size)
			for i:=0; i<len(mp.chunk); i++ {
				mp.chunk[i].Mp = make(map[string]interface{},mp.mp_len)
			}
			return &mp
		},
		ClearFn: generalClear,
	}
}



func (this *ShardsMap) Destory() {
	x := strconv.FormatUint(this.chunk_size,10)
	y := strconv.FormatUint(this.mp_len,10)
	ShardsMapPool.Ms[x+"x"+y].Put(this)
}


//操作需先确定块
//map块始终会逃逸，因为回收不可能再挨个删除
//真正能复用的也就是数组而已
func (this *ShardsMap) MatchChunk( key string ) *SyncMap {
	var target_index = helper.BKDRHash(key) % this.chunk_size
	return &this.chunk[target_index]
}


func (this *ShardsMap) ChunkSize() uint64 { return this.chunk_size }


func (this *ShardsMap) ChunkIndex(i int) *SyncMap { return &this.chunk[i] }
