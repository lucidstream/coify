package timer

import (
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
	"github.com/go-redis/redis"
	"github.com/lucidStream/coify/helper"
	coifyRedis "github.com/lucidStream/coify/redis"
	"github.com/sirupsen/logrus"
)


//检查是否影响了头部元素
//language=LUA
var socreSmallerScript = coifyRedis.LuaScript(`
	local key    = KEYS[1];
	local member = KEYS[2];
	local score  = KEYS[3];
	local XXNX   = KEYS[4];
	local CH     = KEYS[5];
	local INCR   = KEYS[6];
	local affectFront = "no";
	--组装可变参数调用
	local function mutableCall(...)
		local args = {...};
		local arg  = {};
		--移除空参数
		for k, v in pairs(args) do
			if v ~= nil and v ~= "" then
				table.insert(arg,v);
			end
		end
		local paramLen = #arg;
		if paramLen < 4 or paramLen > 7 then return -1000; end;
		--不带附加参数调用
		if paramLen == 4 then
			return redis.call(arg[1],arg[2],arg[3],arg[4]);
		elseif paramLen == 5 then
			return redis.call(arg[1],arg[2],arg[3],arg[4],arg[5]);
		elseif paramLen == 6 then
			return redis.call(arg[1],arg[2],arg[3],arg[4],arg[5],arg[6]);
		else if paramLen == 7 then
			return redis.call(arg[1],arg[2],arg[3],arg[4],arg[5],arg[6],arg[7]);
		end
		end
	end
	--拉取头部元素
	local front = redis.call("zrange",key,"0","0","WITHSCORES");
	if front ~= false then
		if #front==2 and score < front[2] then
			affectFront = "yes";
		end
	end
	local zaddRet = mutableCall("zadd",key,XXNX,CH,INCR,score,member);
	if false == zaddRet then
		return redis.error_reply("zadd member failed");
	elseif zaddRet == -1000 then
		return redis.error_reply("error param length");
	end
	return {zaddRet,affectFront};
`)




func milliSecond(time *time.Time) int64 { return time.UnixNano() / 1000000 }

type PushCommand uint64

const (
	Default = PushCommand(1) << iota
	XX
	NX
	CH
	INCR
)

type TimerChain struct {
	tigger_time int64
	value       map[string]string
	command     [3]string
	timer       *TimeAfter
}






//用法
//.Command(Default).NowAdd() //不管成员是否存在照常覆盖
//.Command(NX|CH).NowAdd() //不更新存在的成员，只添加新成员，通常用作防止覆盖已存在的成员，也就是如果不存在才添加
//.Command(XX|CH).NowAdd() //仅更新存在的成员，不添加新成员，通常用作判断更新是否成功（确定新旧分数不同的情况下）
//大多数情况下影响行数可以忽略，不作判断
func (this *TimeAfter) Command(c PushCommand) (*TimerChain, error) {
	var chain TimerChain
	if c&Default == Default {
		//不能包含NX或XX
		if c&NX == NX {
			return &chain, errors.New("default cannot container NX")
		}
		if c&XX == XX {
			return &chain, errors.New("default cannot container XX")
		}
		//zadd默认是不带CH选项的，zadd只返回添加的成员数
		//如果zadd操作的是一个已存在的成员，不带CH返回值为0
	}
	//XX和NX只能选其一
	if c&XX == XX {
		if c&NX == NX {
			return &chain, errors.New("XX|NX pick one")
		}
		chain.command[0] = "XX"
	}
	if c&NX == NX {
		if c&XX == XX {
			return &chain, errors.New("NX|XX pick one")
		}
		chain.command[0] = "NX"
	}
	if c&CH == CH {
		chain.command[1] = "CH"
	}
	if c&INCR == INCR {
		chain.command[2] = "INCR"
	}
	chain.timer = this
	return &chain, nil
}

//从毫秒时间戳中提取毫秒部分值
func GetMillSecond(x int64) int64 { return x - x/1000*1000 }

//指定执行时间
func (this *TimerChain) TimeStamp(sec int64, nses int64) *TimerChain {
	timestamp := time.Unix(sec, nses)
	this.tigger_time = milliSecond(&timestamp)
	return this
}

//当前时间延后多少秒指定执行时间
func (this *TimerChain) NowAdd(d time.Duration) *TimerChain {
	timestamp := time.Now().Add(d)
	this.tigger_time = milliSecond(&timestamp)
	return this
}

//指定到指定的时间执行
func (this *TimerChain) At(target time.Time) *TimerChain {
	this.tigger_time = milliSecond(&target)
	return this
}




func (this *TimeAfter) RabStract(value map[string]string, rkeys *RabStract) {
	var key_arr = make([]string, 0, len(value))
	for k, _ := range value {
		key_arr = append(key_arr, k)
	}
	sort.Strings(key_arr)
	this.keysJoin(key_arr, value, rkeys)
}




//根据值的hash256结果放入不同的redis队列
func (this *TimerChain) Push(value map[string]string, rkeys *RabStract) (int64, error) {
	var (
		err        error
		key_arr    []string
		fields     map[string]interface{}
		push_sync  *sync.Mutex
		cmder      []redis.Cmder
		tigger_str string
	)
	valLen   := len(value)
	tigger_str = strconv.FormatInt(this.tigger_time, 10)
	key_arr = make([]string, 0, valLen)
	fields = make(map[string]interface{}, valLen)
	for k, v := range value {
		key_arr = append(key_arr, k)
		fields[k] = v
	}
	sort.Strings(key_arr)
	this.timer.keysJoin(key_arr, value, rkeys)

	//以队列名映射同步锁
	sort_queue_hash := helper.BKDRHash(rkeys.SortQueue)
	if this.timer.pushsync {
		mutexindex := sort_queue_hash % PushQueueMapSize
		mutex_inter, _ := pushQueuedSync[mutexindex].Load(rkeys.SortQueue)
		push_sync = mutex_inter.(*sync.Mutex)
		push_sync.Lock()
	}

	pipeline := this.timer.redis_cli.TxPipeline()
	defer func() {
		err := pipeline.Close()
		if nil != err {
			logrus.Error(err)
			return
		}
	}()

	pipeline.HMSet(rkeys.Datakey, fields)
	pipeline.Expire(rkeys.Datakey, this.timer.data_expires) //防止未删除到
	KEYS := [6]string{
		rkeys.SortQueue, rkeys.Datakey, tigger_str,
		this.command[0], this.command[1], this.command[2],
	}
	pipeline.EvalSha(socreSmallerScript, KEYS[:])
	if cmder, err = pipeline.Exec(); nil != err {
		if this.timer.pushsync {
			push_sync.Unlock()
		}
		return 0, err
	}
	//执行完就可以提前解锁了
	//提前解锁不能用defer
	if this.timer.pushsync { push_sync.Unlock() }

	//处理执行结果ing
	cmderLen := len(cmder)
	if cmderLen != 3 {
		return 0, errors.New("lack response cmder")
	}

	eval, err := cmder[2].(*redis.Cmd).Result()
	if nil != err {
		//不存在未能更改执行时间但修改了数据字段值的问题
		//因为只有字段值没有变化才能索引到key
		//一个定时事件能修改的只有执行时间而已
		return 0, err
	}
	evalr := eval.([]interface{})
	affectFront := evalr[1].(string)
	affectCount := evalr[0].(int64)

	//以队列名映射条件变量
	cond_index := sort_queue_hash % EmptyCondMapSize
	cond_inter, exists := emptyCondSync[cond_index].Load(rkeys.SortQueue)
	if false == exists {
		panic("cond not exists")
	}
	cond_accert := cond_inter.(*sync.Cond)
	cond_accert.L.Lock()
	cond_accert.Signal()
	cond_accert.L.Unlock()
	//发送frontChange通知
	if affectFront == "yes" {
		timer_index := sort_queue_hash % WaitTimerMapSize
		timer_inter, exists := waitTimerSync[timer_index].Load(rkeys.SortQueue)
		if false == exists {
			panic("timer not exists")
		}
		timer_inter.(*waitTimer).Cancel()
	}
	return affectCount, nil
}





//若有需要删除，用户需保存已投递的事件副本
//或能根据某种参数索引
//假设删掉的是队列头元素，比如123,456
//可以不用唤醒peeker，123都还没到时间，唤起123等456还是要等
func (this *TimeAfter) Remove(abkeys *RabStract) error {
	pipeline := this.redis_cli.TxPipeline()
	defer func() {
		err := pipeline.Close()
		if nil != err {
			logrus.Error(err)
			return
		}
	}()
	pipeline.ZRem(abkeys.SortQueue, abkeys.Datakey)
	pipeline.Del(abkeys.Datakey)
	if _, err := pipeline.Exec(); nil != err {
		return err
	}
	shards_index := helper.BKDRHash(abkeys.SortQueue) % EmptyCondMapSize
	cond_inter, _ := emptyCondSync[shards_index].Load(abkeys.SortQueue)
	cond_accert := cond_inter.(*sync.Cond)
	cond_accert.L.Lock()
	cond_accert.Signal()
	cond_accert.L.Unlock()
	return nil
}




//检查事件是否在队列中
//member不存在返回redis.Nil
func (this *TimeAfter) Exists(abkeys *RabStract) (float64, error) {
	score, err := this.redis_cli.ZScore(abkeys.SortQueue, abkeys.Datakey).Result()
	return score, err
}
