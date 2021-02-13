package csync

//简单封装的查询器
//每一个数据集的查询由用户保证key的唯一性
//可直接用来查DB，无需在redis构建缓存
//一般情况下，回调函数所调用的包+函数名+参数值可以形成一个唯一值
func (this *SynchShare) Get( key string,userfn func() QueryShare ) (interface{},error) {
	var (
		swg        *wglock
		version    int64
		first      bool
		sharedret  QueryShare
	)
	swg,version,first = this.GetWaitGroup(key)
	if ! first {
		//未得到查询权的请求↓↓↓↓↓↓↓↓↓
		this.WGDone(swg,key)
		sharedret = this.WGWait(swg,key,version)
		return sharedret.Result,sharedret.Err
	}

	//得到查询权限的请求↓↓↓↓↓↓↓↓↓
	//即使发现缓存不存在或过期，也只有一个请求能执行更新
	sharedret = userfn()
	this.WGShared(swg,sharedret)
	//解锁唤起所有等待请求
	this.WGUnlock(swg,key,version)
	return sharedret.Result,sharedret.Err
}
