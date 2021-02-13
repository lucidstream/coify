package spool

import (
	"sync"
	"sync/atomic"
	"unsafe"
)




type RefPool struct {
	pool             sync.Pool
	Size_t           uintptr
	Roffset          uintptr
	CreateFn         func() interface{}
	ClearFn          func( interface{} )
}






//传入x增加其引用计数
//go:nosplit
func (this *RefPool) Get( x interface{} ) interface{} {
	if this.pool.New == nil {
		this.pool.New = this.CreateFn
	}

	var obj interface{}
	if x == nil { obj = this.pool.Get() } else {
		obj = x
	}

	interface_obj := (*struct{ Type uintptr ; Data uintptr })(
		unsafe.Pointer((*interface{})(unsafe.Pointer(&obj))) )

	ref_count := (*int64)(unsafe.Pointer( interface_obj.Data + this.Roffset ))

	//将计数字段+1
	//拿出的新对象不应该还存在引用
	if x == nil && atomic.LoadInt64(ref_count) != 0 {
		panic("the object reference count not zero")
	}

	//需要加计数的对象，如果计数已经是0了
	//说明已经被释放或正在释放的过程中，不可再加计数
	if x != nil && atomic.LoadInt64(ref_count) <= 0 {
		panic("useof released pointer")
	}

	//如果是已有对象直接加计数
	atomic.AddInt64(ref_count,1)

	return obj
}





//对于大数据集的复用，需要先清理，因此最好使用一个单独的goroutinue调用Put
//如果复用的速度太慢或成本过高，那么最好使用协程或跑队列慢慢复用
func (this *RefPool) Put( x interface{} ) {
	if x == nil {
		panic("unable nil pointer")
	}
	interface_obj := (*struct{ Type uintptr ; Data uintptr })(
		unsafe.Pointer((*interface{})(unsafe.Pointer(&x))) )
	//减去引用计数
	ref_count := (*int64)(unsafe.Pointer( interface_obj.Data + this.Roffset ))
	newcont := atomic.AddInt64(ref_count,-1)
	if newcont < 0 {
		panic("ref_count less than zero")
	}
	if newcont == 0 {
		this.ClearFn(x)
		this.pool.Put(x)
	}
}


