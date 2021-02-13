package redis

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"math"
	"math/rand"
	"strconv"
	"time"
)

//依赖全局队列做执行计划
//分布式排队事件注册，回调，事件分类（相同的事件排队，不同的事件并发）


type SyncPlain struct{
	dispatch     *SyncCoifyRedis
	timeout      time.Duration
	queueTail    string
}





func (this *SyncPlain) GetDispatch() *SyncCoifyRedis {
	return this.dispatch
}





//创建一个lockPlain对象
//this即协作客户端
func (this *SyncCoifyRedis) SyncPlain( timeout time.Duration ) (splain SyncPlain,err error) {
	options := this.Options()
	//检查协作客户端的读写池操作时间
	if options.IdleTimeout < timeout {
		return splain,errors.New("client idletime not enough")
	}
	if options.ReadTimeout < timeout {
		return splain,errors.New("client readtime not enough")
	}
	if options.WriteTimeout < timeout {
		return splain,errors.New("client writetime not enough")
	}
	splain.timeout   = timeout
	splain.dispatch  = this
	splain.queueTail = ".queue"
	return splain,nil
}



func (this *SyncPlain) SetQueueTail( s string ){ this.queueTail=s }



//language=LUA
var ifSetNxProvideEmptyQueue = LuaScript(`
	local flagkey  = KEYS[1];
	local queuekey = KEYS[1]..KEYS[2];
	local value    = KEYS[3];
	local timeout  = KEYS[4];--秒级超时够用了
	local setnxret = redis.call("set",flagkey,value,"EX",timeout,"NX");
	if false == setnxret then
		return 0;--key已存在返回nil
	end
	if setnxret["ok"] == "OK" then
		redis.call("del",queuekey);
		return 1;
	end
	return 0;
`)


func formatSec(dur time.Duration) (int64,error) {
	var err error
	if dur > 0 && dur < time.Second {
		err = errors.New(fmt.Sprintf(
			"specified duration is %s, but minimal supported value is %s",
			dur, time.Second,
		))
		return 0,err
	}
	return int64(dur / time.Second),nil
}



var QueueWaitTimeoutError = errors.New("queue wait timeout")


//尝试对redis键加锁
func (this *SyncPlain) Lock( key string ) (string,error) {
	//生成随机数
	var (
		rnd,second  int64
		value       string
		lock_stat   interface{}
		err         error
		KEYS        [4]string
	)
	KEYS[0] = key
	KEYS[1] = this.queueTail
	second,err = formatSec(this.timeout)
	if nil != err {
		return "",err
	}
	KEYS[3] = strconv.FormatInt(second,10)
	queue_key := key+this.queueTail
contend:
	rnd     = int64(rand.Intn(int(math.MaxUint32)))
	value   = strconv.FormatInt(rnd,10)
	KEYS[2] = value

	lock_stat,err = this.dispatch.EvalSha(ifSetNxProvideEmptyQueue,KEYS[:]).Result()
	if nil != err {
		return "",err
	}

	//得到锁的继续处理业务，待业务处理处理完后回传随机数解锁
	if lock_stat.(int64) == 1 { return value,nil } else {
		//未得到锁的到协作实例等待调度
		err = this.dispatch.BRPop(this.timeout,queue_key).Err()
		if nil != err {
			if err == redis.Nil {
				return "",QueueWaitTimeoutError
			}
			return "",err
		}
		//得到调度后重新竞争锁
		goto contend
	}
}




//language=LUA
var ifEqualUnlock = LuaScript(`
	local key       = KEYS[1];
	local queueTail = KEYS[2];
	local value     = KEYS[3];
	local check_value = redis.call("get",key);
	if false == check_value then
		return redis.error_reply("1");
	end
	
	if check_value == value then
		redis.call("del",key);
		redis.call("lpush",key..queueTail,"1");
		return "OK";
	end
	
	return redis.error_reply("2");
`)


//保护标识超时
var ProtectFlagTimeout  = errors.New("protect flag timeout")

//篡改或已被他人抢占错误
var FalsifOrOtherRaceError = errors.New("falsified or other race covered")

//解锁
func (this *SyncPlain) UnLock(key string,value string) error {
	var KEYS = []string{
		key,this.queueTail,value,
	}
	err := this.dispatch.EvalSha(ifEqualUnlock,KEYS).Err()
	if nil == err {
		return nil
	}
	errMsg := err.Error()
	switch errMsg {
	case "1":
		return ProtectFlagTimeout
	case "2":
		return FalsifOrOtherRaceError
	default:
		return err
	}
}
