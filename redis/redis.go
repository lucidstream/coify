package redis

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"math"
	"math/rand"
	"strconv"
	"time"
)


var luaScriptList []string


func LuaScript( code string ) string {
	//code加入注册列表 客户端示例初始化时载入
	luaScriptList = append(luaScriptList,code)
	return fmt.Sprintf("%x",sha1.Sum([]byte(code)))
}


func (this* CoifyRedis) LoadLuaScript() error {
	var err error
	for i:=0; i<len(luaScriptList); i++ {
		err = this.ScriptLoad(luaScriptList[i]).Err()
		if nil != err {
			return err
		}
	}
	return nil
}



type CoifyRedis struct {
	*redis.Client
	Name         string
	Option       RedisOption
}



type CacheCoifyRedis struct{ *CoifyRedis }
type SyncCoifyRedis  struct{ *CoifyRedis }
type RelvanceRedis   struct{ *CoifyRedis }



type RedisOption struct {
	Addr             string        `ini:"addr"`
	Passwd           string        `ini:"pass"`
	Db               int           `ini:"dbno"`
	DialTimeout      time.Duration `ini:"dial_timeout"`
	ReadTimeout      time.Duration `ini:"read_timeout"`
	WriteTimeout     time.Duration `ini:"write_timeout"`
	PoolSize         int           `ini:"pool_size"`
	IdleTimeout      time.Duration `ini:"idle_timeout"`
}



func NewCoifyRedis( opts RedisOption,name string ) *CoifyRedis {
	return &CoifyRedis{
		NewRedisClient(opts),
		name,
		opts,
	}
}



func NewRedisClient( option RedisOption ) *redis.Client {
	var network = "tcp"
	if option.Addr[len(option.Addr)-5:] == ".sock" {
		network = "unix"
	}
	rption := redis.Options {
		Network      :  network,
		Addr         :  option.Addr,
		Password     :  option.Passwd, // no password set
		DB           :  option.Db,  // use default DB
		DialTimeout  :  option.DialTimeout,//连接超时
		PoolSize     :  option.PoolSize,
		IdleTimeout  :  option.IdleTimeout,
		ReadTimeout  :  option.ReadTimeout,
		WriteTimeout :  option.WriteTimeout,
	}
	rption.PoolTimeout = rption.ReadTimeout + 3*time.Second
	redis_cli := redis.NewClient(&rption)
	return redis_cli
}




//尝试对redis键加锁
func (this *CoifyRedis) LockRedis( key string,timeout time.Duration ) (string,error) {
	//生成随机数
	rnd   := int64(rand.Intn(int(math.MaxUint32)))
	value := strconv.FormatInt(rnd,10)
	lock_stat,err := this.SetNX(key,value,timeout).Result()
	if nil != err {
		return "",err
	}
	if lock_stat { return value,nil } else {
		return "",errors.New("")
	}
}





//解锁
func (this *CoifyRedis) UnLockRedis(key string,value string) error {
	check_value,err := this.Get(key).Result()
	if nil != err {
		if err == redis.Nil {
			//解锁的时候锁已经不在了
			//在保证他人无法操作锁的前提下
			//锁不在了说明操作太慢导致超时了
			//也可能是超时时间设置的不合理
			//应当返回错误提示处理
			return ProtectFlagTimeout
		}
		return err
	}
	if check_value == value {
		_,err = this.Del(key).Result()
		if nil != err {
			return err
		}
		return nil
	}
	return FalsifOrOtherRaceError
}

