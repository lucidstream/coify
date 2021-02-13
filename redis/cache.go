package redis

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/lucidStream/coify/csync"
	"github.com/lucidStream/coify/helper"
	"strings"
	"time"
)


//缓存数据与直接使用redis作为第一源的数据应该在不同的库中存储
//至少需要是不同的No


type CacheControlClient struct{
	control    *CacheControl
	client     *CacheCoifyRedis
}


//缓存控制器开启同步
type EnableSynch struct {
	CacheControlClient
	Synch      *csync.SynchShare
}



//缓存层需要实现的方法
type CacheLayer interface {
	Serialize() (string,error) //按参数顺序拼接
	SoruceGet() (interface{},error) //获取原始对象
	Unmarshal( val interface{} ) (interface{},error)
}



var CacheManage = make(map[string]*CacheControl)


//缓存控制器绑定redis客户端
func NewCacheControlWithClient( cacheControl *CacheControl,client *CacheCoifyRedis ) CacheControlClient {
	if cacheControl.identifying == "" {
		panic("identifying empty")
	}
	if cacheControl.token == "" {
		panic("token empty")
	}
	if cacheControl.expires < 0 {
		panic("expires cannot less than zero")
	}
	return CacheControlClient{cacheControl, client,}
}


func (this *CacheControlClient) Client() *CacheCoifyRedis { return this.client }

func (this *CacheControlClient) Expires() time.Duration { return this.control.expires }





//缓存控制器需要有一个唯一名称，外部删除缓存需要用名字索引
type CacheControl struct{
	identifying      string //缓存名字
	token            string //获取器token
	expires          time.Duration //缓存有效期
	fullName         bool //是否保存完整键名
	CacheGet         func( export *CacheControlClient,key string ) (interface{},error)
	CacheSet         func( export *CacheControlClient,key string,val interface{} ) error
}




//identifying 缓存标识
//expires 过期时间，必须要有，不支持使用LRU淘汰
//fullName 是否保存完整键名，建议调试期间打开
func NewCacheControl( identifying string,expires time.Duration,fullName bool ) CacheControl {
	cacheControl := CacheControl{
		identifying : identifying,
		token       : helper.FileLineToken(2),
		expires     : expires,
		fullName    : fullName,
	}
	if cacheControl.identifying == "" {
		panic("identifying empty")
	}
	if cacheControl.token == "" {
		panic("token empty")
	}
	if cacheControl.expires < 0 {
		panic("expires cannot less than zero")
	}
	//缓存管理器标识需要是唯一的
	_,exists := CacheManage[cacheControl.identifying]
	if exists == true {
		errMsg := fmt.Sprintf("cache manage [%s] already exist",cacheControl.identifying)
		panic(errMsg)
	}
	CacheManage[cacheControl.identifying] = &cacheControl
	return cacheControl
}




//cache是在模型层实现的一个类型
func (this *CacheControl) newkey( instance CacheLayer ) (string,error) {
	seqAfter,err := instance.Serialize()
	if nil != err {
		return "",err
	}
	merge := []string{this.identifying,seqAfter,this.token}
	format_key := strings.Join(merge,":")
	//完整键名直接返回
	if true == this.fullName {
		return format_key,nil
	}
	//部署模式将存储sha256的base64
	sum := sha256.Sum256([]byte(format_key))
	b64 := base64.RawStdEncoding.EncodeToString(sum[:])
	return b64,nil
}







//受保护的获取过程
//可以避免缓存雪崩带来的影响
//但本地需要维护一个较大的hash表
func (this *EnableSynch) ProtectGet( instance CacheLayer ) (interface{},error) {
	format_key,err := this.control.newkey(instance)
	if nil != err {
		return nil,err
	}
	return this.Synch.Get(format_key, func() csync.QueryShare {
		result,err := this.CacheControlClient.get(format_key,instance)
		return csync.QueryShare{result,err}
	})
}





//直接获取，不考虑雪崩
func (this *CacheControlClient) Get( instance CacheLayer ) (interface{},error) {
	format_key,err := this.control.newkey(instance)
	if nil != err {
		return nil,err
	}
	return this.get(format_key,instance)
}





//数组更改前保存构成键名的原始参数，更改完成后使用更改前的参数调用Delete
//不作scan扫描与此缓存管理器相关的键，貌似匹配模式并不是前缀树
//批量操作就需要批量保存数据被修改前的状态，然后修改完成后再用该之前的参数来删除缓存
func (this *CacheControlClient) Delete( instance CacheLayer ) error {
	format_key,err := this.control.newkey(instance)
	if nil != err {
		return err
	}
	return this.client.Del(format_key).Err()
}





func (this *CacheControlClient) get( format_key string,instance CacheLayer ) (interface{},error) {
	result,err := this.control.CacheGet(this,format_key)
	if nil != err {
		//数据不存在统一返回nil
		//即使使用的命令没有nil结果，也需要判断一下手动写个nil返回
		if err != redis.Nil {
			return nil,err
		}
		//处理redis:nil错误
		result,err = instance.SoruceGet()
		if nil != err {
			return nil,err
		}
		err = this.control.CacheSet(this,format_key,result)
		if nil != err {
			return nil,err
		}
		//从源查询函数调用不需要再解析
	} else {
		if nil == result {
			return nil,errors.New("cacheGet detecet nil result")
		}
		result,err = instance.Unmarshal(result)
	}
	return result,err
}

