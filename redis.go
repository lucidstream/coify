package coify

import (
	"errors"
	"github.com/go-ini/ini"
	"github.com/lucidStream/coify/redis"
	"stathat.com/c/consistent"
	"strings"
)



var (
	//缓存实例
	CacheRedis = CacheCoifyRedis{
		nil,
		make(map[string]*redis.CacheCoifyRedis),
	}
	//同步实例
	SyncRedis  = SyncCoifyRedis{
		nil,
		make(map[string]*redis.SyncCoifyRedis),
	}
	//关系实例
	//一个关系型实例需要由若干个索引实例（使用table名映射索引）以及若干个数据实例组成
	//拆分索引的成本太大了
	Relvance   = RelvanceRedis{
		nil,
		make(map[string]*redis.RelvanceRedis),
	}
)



//缓存类型客户端
type CacheCoifyRedis struct {
	*consistent.Consistent
	clients map[string]*redis.CacheCoifyRedis
}


func (this *CacheCoifyRedis) NewConsistent( exclude []*redis.CacheCoifyRedis ) {
	this.Consistent = consistent.New()
	var ( iter int; hasin bool )
	for k,v := range this.clients {
		iter,hasin = 0,false
	loop:
		if v == exclude[iter] {
			hasin = true
			goto block
		}
		if iter++; iter < len(exclude) {
			goto loop
		}
	block:
		if true == hasin {
			continue
		}
		this.Consistent.Add(k)
	}
}



//同步类型客户端
type SyncCoifyRedis struct {
	*consistent.Consistent
	clients map[string]*redis.SyncCoifyRedis
}


func (this *SyncCoifyRedis) NewConsistent( exclude []*redis.SyncCoifyRedis ) {
	this.Consistent = consistent.New()
	var ( iter int; hasin bool )
	for k,v := range this.clients {
		iter,hasin = 0,false
	loop:
		if v == exclude[iter] {
			hasin = true
			goto block
		}
		if iter++; iter < len(exclude) {
			goto loop
		}
	block:
		if true == hasin {
			continue
		}
		this.Consistent.Add(k)
	}
}



//关系存储类型客户端
type RelvanceRedis struct {
	*consistent.Consistent
	clients map[string]*redis.RelvanceRedis
}


func (this *RelvanceRedis) NewConsistent( exclude []*redis.RelvanceRedis ) {
	this.Consistent = consistent.New()
	var ( iter int;hasin bool )
	for k,v := range this.clients {
		iter,hasin = 0,false
loop:
		if v == exclude[iter] {
			hasin = true
			goto block
		}
		if iter++; iter < len(exclude) {
			goto loop
		}
block:
		if true == hasin {
			continue
		}
		this.Consistent.Add(k)
	}
}


func (this *RelvanceRedis) QuickGet( key string ) (*redis.RelvanceRedis,error) {
	name,err := this.Consistent.Get(key)
	if nil != err {
		return nil,err
	}
	client,ok := this.clients[name]
	if false == ok {
		return nil,errors.New("client not exists")
	}
	if nil == client {
		return nil,errors.New("client pointer is nil")
	}
	return client,nil
}



func (this *RelvanceRedis) GetN( key string,n int ) ([]*redis.RelvanceRedis,error) {
	names,err := this.Consistent.GetN(key,n)
	if nil != err {
		return nil,err
	}
	nameLen := len(names)
	clients := make([]*redis.RelvanceRedis,0,nameLen)
	for i:=0; i<nameLen; i++ {
		client,ok := this.clients[names[i]]
		if false == ok {
			return nil,errors.New("has not exists client")
		}
		if client == nil {
			return nil,errors.New("client pointer is nil")
		}
		clients = append(clients,client)
	}
	return clients,nil
}








//初始化缓存类型客户端
func NewCacheRedis( section *ini.Section ) (*redis.CacheCoifyRedis,error) {
	var client redis.CacheCoifyRedis
	name := strings.Trim(section.Name()," ")
	_,exists := CacheRedis.clients[name]
	if exists == true {
		return nil,errors.New("client name already exist")
	}
	var options redis.RedisOption
	err := section.MapTo(&options)
	if nil != err {
		return nil,err
	}
	client.CoifyRedis = redis.NewCoifyRedis(options,name)
	if err = client.LoadLuaScript();nil != err {
		return nil,err
	}
	CacheRedis.clients[name] = &client
	return &client,nil
}



//同步调度实例
func NewSyncRedis( section *ini.Section ) (*redis.SyncCoifyRedis,error) {
	var client redis.SyncCoifyRedis
	name := strings.Trim(section.Name()," ")
	_,exists := SyncRedis.clients[name]
	if exists == true {
		return nil,errors.New("client name already exist")
	}
	var options redis.RedisOption
	err := section.MapTo(&options)
	if nil != err {
		return nil,err
	}
	client.CoifyRedis = redis.NewCoifyRedis(options,name)
	if err = client.LoadLuaScript();nil != err {
		return nil,err
	}
	SyncRedis.clients[name] = &client
	return &client,nil
}



//关系型数据实例
func NewRelvanceRedis( section *ini.Section ) (*redis.RelvanceRedis,error) {
	var client redis.RelvanceRedis
	name := strings.Trim(section.Name()," ")
	_,exists := Relvance.clients[name]
	if exists == true {
		return nil,errors.New("client name already exist")
	}
	var options redis.RedisOption
	err := section.MapTo(&options)
	if nil != err {
		return nil,err
	}
	client.CoifyRedis = redis.NewCoifyRedis(options,name)
	if err = client.LoadLuaScript();nil != err {
		return nil,err
	}
	Relvance.clients[name] = &client
	return &client,nil
}