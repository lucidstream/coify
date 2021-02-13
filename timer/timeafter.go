package timer

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/lucidStream/coify/helper"
	coifyRedis "github.com/lucidStream/coify/redis"
	"github.com/lucidStream/coify/serv"
	"github.com/lucidStream/coify/spool"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)



const (
	EmptyCondMapSize   = 1000//空队列等待条件变量哈希表容量
	WaitTimerMapSize   = 1000//peekWait定时器哈希表容量
	PushQueueMapSize   = 1000//同步推送锁哈希表容量
	DefaultPuller      = 1000
	DefaultConcurrent  = 10000
	DefaultDataExpires = 24*time.Hour*7
)


//member与score一致性检查脚本
//language=LUA
var consisScript string = coifyRedis.LuaScript(`
		local ret;
		local scores=redis.call('zscore',KEYS[1],KEYS[2]);
		--检查member的分数是否发生变化
		if scores~=false and scores==KEYS[3] then
			--重命名datakey
			redis.call('rename',KEYS[2],KEYS[4]);
			--从队列中删除确认一致的member
			ret = redis.call('zrem',KEYS[1],KEYS[2]);
		else
			ret=-1;
		end
		return ret;`)



type TimeAfter struct {
	sort_queue     string
	redis_cli      *coifyRedis.CoifyRedis
	tigger_fn      func( cli *coifyRedis.CoifyRedis,event_key string,tigger_time int64 )//任务触发执行回调
	concurrent     int64            //允许同时存在的任务数
	task_count     sync.WaitGroup   //当前进行中的任务数
	task_cond      *sync.Cond
	data_expires   time.Duration    //数据过期时间
	stop_notify    context.Context
	stop_call      context.CancelFunc
	puller         int              //任务拉取者数量
	puller_count   sync.WaitGroup   //运行中的拉取者计数
	pushsync       bool
	ref_count      int64
}



type waitTimer struct{
	timer   *time.Timer
	cancel  bool
	mutex   sync.Mutex
}


//读取取消状态，如果已被取消自动恢复
func (this *waitTimer) IfCanceldRecover() bool {
	this.mutex.Lock()
	canceld := this.cancel
	this.cancel = false
	this.mutex.Unlock()
	return canceld
}


//设置取消状态并唤起等待者
func (this *waitTimer) Cancel() {
	this.mutex.Lock()
	this.cancel = true
	this.timer.Reset(0)//将由定时器线程向管道写入数据
	this.mutex.Unlock()
}


//清理消息队列残留消息，重置定时器
func (this *waitTimer) ResetTimer( d time.Duration ) bool {
	this.mutex.Lock()
	select{
	case <-this.timer.C:
	default:
	}
	ret := this.timer.Reset(d)
	this.mutex.Unlock()
	return ret
}


//需要定时器唤起后再调用put
func (this *waitTimer) Put() {
	this.mutex.Lock()
	//清理残留消息
	select{
	case <-this.timer.C:
	default:
	}
	this.timer.Stop()
	this.cancel = false
	waitTimerPool.Put(this)
	this.mutex.Unlock()
}


var (
	emptyCondSync  [EmptyCondMapSize]sync.Map
	pushQueuedSync [PushQueueMapSize]sync.Map
	waitTimerSync  [WaitTimerMapSize]sync.Map
	condPool = sync.Pool{
		New: func() interface{} {
			return sync.NewCond(&sync.Mutex{})
		},
	}
	pushSyncPool = sync.Pool{
		New: func() interface{} {
			return new(sync.Mutex)
		},
	}
	waitTimerPool = sync.Pool{
		New: func() interface{} {
			timer := time.NewTimer(1*time.Hour)
			timer.Stop()
			return &waitTimer{
				timer:timer,
				cancel:false,
			}
		},
	}
)



var TimeafterPool = spool.RefPool{
	Roffset : unsafe.Offsetof(TimeAfter{}.ref_count),
	Size_t  : unsafe.Sizeof(TimeAfter{}),
	CreateFn: func() interface{} {
		return &TimeAfter{}
	},
	ClearFn: func(i interface{}) {
		x := i.(*TimeAfter)
		x.sort_queue   = ""
		x.redis_cli    = nil
		x.tigger_fn    = nil
		x.concurrent   = DefaultConcurrent
		x.data_expires = DefaultDataExpires
		x.stop_notify  = nil
		x.stop_call    = nil
		x.puller       = DefaultPuller
		x.task_cond    = nil
		x.pushsync     = false
	},
}



type Client   = coifyRedis.CoifyRedis
type OnTigger = func( cli *coifyRedis.CoifyRedis,event_key string,tigger_time int64)


func NewTimeAfter( key string,client *Client,ontigger OnTigger ) (*TimeAfter,error) {
	timeafter := TimeafterPool.Get(nil).(*TimeAfter)
	//timeafter:client=CLIENTNAME:sortqueue=KEY
	timeafter.sort_queue = fmt.Sprintf("timeafter:client=%s:sortqueue=%s",client.Name,key)
	timeafter.redis_cli    = client
	timeafter.tigger_fn    = ontigger
	timeafter.concurrent   = DefaultConcurrent
	timeafter.data_expires = DefaultDataExpires
	ctx,cancel := context.WithCancel(context.Background())
	timeafter.stop_notify,timeafter.stop_call = ctx,cancel
	timeafter.puller       = DefaultPuller
	timeafter.task_cond    = condPool.Get().(*sync.Cond)
	timeafter.pushsync     = false
	conter,waiter := helper.ReadWaitgroup(&timeafter.task_count)
	if conter != 0 || waiter != 0 {
		errmsg :=  fmt.Sprintf("timeafter check task_count: conter=%d,waiter=%d",conter,waiter)
		return nil,errors.New(errmsg)
	}
	conter,waiter = helper.ReadWaitgroup(&timeafter.puller_count)
	if conter != 0 || waiter != 0 {
		errmsg := fmt.Sprintf("timeafter check puller_count: conter=%d,waiter=%d",conter,waiter)
		return nil,errors.New(errmsg)
	}
	return timeafter,nil
}



func (this *TimeAfter) SetDataExpires( expires time.Duration ) { this.data_expires = expires }

func (this *TimeAfter) SetPuller( puller int ) { this.puller = puller }

func (this *TimeAfter) SetConcurrent( concurrent int64 ) { this.concurrent = concurrent }

func (this *TimeAfter) SetSyncPush( sync bool ) { this.pushsync = sync }




//唤起所有空条件变量
func (this *TimeAfter) emptyCondWakeup() {
	var(
		sortqueue    string
		sortquehash  uint64
		accert_cond  *sync.Cond
		cond_inter   interface{}
		cond_index uint64; i=0; isok bool
	)
	joinbuf := bytes.NewBuffer([]byte{})
	joinbuf.Grow(260)
loop:
	sortqueue   = this.format_sort_queue(joinbuf,strconv.Itoa(i))
	sortquehash = helper.BKDRHash(sortqueue)
	cond_index  = sortquehash % EmptyCondMapSize
	cond_inter,isok = emptyCondSync[cond_index].Load(sortqueue)
	if false == isok {
		return
	}
	accert_cond = cond_inter.(*sync.Cond)
	accert_cond.L.Lock()
	accert_cond.Broadcast()
	accert_cond.L.Unlock()
	if i++; i<int(this.puller) { goto loop }
}




//唤起等待定时器
func (this *TimeAfter) waitTimerWakeup() {
	var (
		sortqueue     string
		sortquehash   uint64
		accert_timer  *waitTimer
		timer_inter   interface{}
		i=0; isok bool; shards_index uint64
	)
	joinbuf := bytes.NewBuffer([]byte{})
	joinbuf.Grow(260)
loop:
	sortqueue    = this.format_sort_queue(joinbuf,strconv.Itoa(i))
	sortquehash  = helper.BKDRHash(sortqueue)
	shards_index = sortquehash % WaitTimerMapSize
	timer_inter,isok = waitTimerSync[shards_index].Load(sortqueue)
	if false == isok {
		return
	}
	accert_timer = timer_inter.(*waitTimer)
	accert_timer.Cancel()
	if i++; i<int(this.puller) { goto loop }
}



//删除创建的依赖资源
func (this *TimeAfter) deldynamic() {
	var(
		sortqueue     string
		sortquehash   uint64
		accert_cond   *sync.Cond
		isok          bool
		mutex_index   uint64
		cond_index    uint64
		timer_index   uint64
		psync_inter   interface{}
		cond_inter    interface{}
		timer_inter   interface{}
		psync_accert  *sync.Mutex
		timer_accert  *waitTimer
		i int = 0
	)
	joinbuf := bytes.NewBuffer([]byte{})
	joinbuf.Grow(260)
loop:
	sortqueue   = this.format_sort_queue(joinbuf,strconv.Itoa(i))
	sortquehash = helper.BKDRHash(sortqueue)
	//删除空队列条件变量
	cond_index  = sortquehash % EmptyCondMapSize
	cond_inter,isok = emptyCondSync[cond_index].Load(sortqueue)
	if false == isok {
		goto delPushSync
	}
	accert_cond = cond_inter.(*sync.Cond)
	emptyCondSync[cond_index].Delete(sortqueue)
	condPool.Put(accert_cond)
delPushSync:
	//删除同步推送互斥量
	mutex_index = sortquehash % PushQueueMapSize
	psync_inter,isok = pushQueuedSync[mutex_index].Load(sortqueue)
	if false == isok {
		goto delPeekTimer
	}
	psync_accert = psync_inter.(*sync.Mutex)
	pushQueuedSync[mutex_index].Delete(sortqueue)
	pushSyncPool.Put(psync_accert)
delPeekTimer:
	//删除peek等待定时器
	timer_index = sortquehash % WaitTimerMapSize
	timer_inter,isok = waitTimerSync[timer_index].Load(sortqueue)
	if false == isok {
		return
	}
	timer_accert = timer_inter.(*waitTimer)
	waitTimerSync[timer_index].Delete(sortqueue)
	timer_accert.Put()
	if i++; i<int(this.puller) { goto loop }
}






//对象二次封装的情况可能多次调用Destory
//销毁过程中不允许再修改或查询任务
func (this *TimeAfter) Destory() {
	//发送停止通知
	(this.stop_call)()
	//唤起空队列条件变量
	this.emptyCondWakeup()
	//唤起等待定时器
	this.waitTimerWakeup()
	//唤起任务数超限条件变量
	task_cond := this.task_cond
	if nil == task_cond {
		return
	}
	//----------------------------------------------
	task_cond.L.Lock()
	//第一个请求进入临界区获取对象操作权
	//检查指针是否已被清空，如果被清空说明有其他请求已经处理了
	if this.task_cond == nil {
		task_cond.Broadcast()
		task_cond.L.Unlock()
		return
	}
	this.task_cond = nil//清空对象指针
	task_cond.Broadcast()
	task_cond.L.Unlock()
	//----------------------------------------------
	donech := make(chan struct{},2)
	defer close(donech)
	go func() {
		//等待已投递的任务退出
		this.task_count.Wait()
		donech <- struct{}{}
		//等待puller退出
		this.puller_count.Wait()
		donech <- struct{}{}
	}()
	for i:=0; i<2; i++ {
		select{
		case <-donech:
		case <-time.After(10*time.Second):
			//如果超时可能存在死锁
			logrus.Error("wait counter done timeout")
			return
		}
	}

	this.deldynamic()
	condPool.Put(task_cond)
	TimeafterPool.Put(this)
}








func (this *TimeAfter) Run() {
	var (
		sortqueue    string
		sortquehash  uint64
		shards_index uint64
		isload       bool
		i int = 0
	)
	joinbuf := bytes.NewBuffer([]byte{})
	joinbuf.Grow(260)
loop:
	//sortqueue = this.sort_queue+".remainder:"+strconv.Itoa(i)
	sortqueue    = this.format_sort_queue(joinbuf,strconv.Itoa(i))
	sortquehash  = helper.BKDRHash(sortqueue)
	//创建同步所需条件变量
	empty_cond   := condPool.Get().(*sync.Cond)
	shards_index = sortquehash % EmptyCondMapSize
	_,isload  = emptyCondSync[shards_index].LoadOrStore(sortqueue,empty_cond)
	if isload == true {
		panic("cond location occupied")
	}
	//创建peekwait所需定时器
	wTimer := waitTimerPool.Get().(*waitTimer)
	shards_index = sortquehash % WaitTimerMapSize
	_,isload = waitTimerSync[shards_index].LoadOrStore(sortqueue,wTimer)
	if isload == true {
		panic("waitTimer location occupied")
	}
	//创建同步所需互斥量
	if this.pushsync == true {
		push_sync    := pushSyncPool.Get().(*sync.Mutex)
		shards_index  =  sortquehash % PushQueueMapSize
		_,isload  = pushQueuedSync[shards_index].LoadOrStore(sortqueue,push_sync)
		if isload == true {
			panic("mutex location occupied")
		}
	}
	go this.pullerRun(sortqueue,empty_cond,wTimer)

	if i++;i < int(this.puller) { goto loop }

	//监听服务器关闭指令
	go func() {
		defer func() { this.Destory() }()
		select{
		case <-this.stop_notify.Done():
			//当实例关闭时也要能退出，不能一直等服务器关闭
			return
		case <-serv.ServerCtx.Done():
			//转到定时器关闭流程
			return
		}
	}()
}




const dataRenamePrefix = "timeafter:execed:datakey="

var contextDoneErr = errors.New("context done")


func (this *TimeAfter) peekWait( sortqueue string,empty_cond *sync.Cond,wTimer *waitTimer ) ([]redis.Z,error) {
	var (
		err          error
		block        []redis.Z
		wait_second  time.Duration
		hasSmaller   bool
		score        int64
		tiggerOffset int64
		nowtime      int64
	)
rePeek:
	empty_cond.L.Lock()
empty_retry:
	select{
	case <-(this.stop_notify).Done():
		empty_cond.L.Unlock()
		return nil,contextDoneErr
	default:
	}
	//需要先读取头元素才知道需要等待多长时间
	block,err = this.redis_cli.ZRangeWithScores(sortqueue,0,0).Result()
	if nil != err {
		logrus.Error("timeAfter peek wait err:",err)
		select{
		case <-(this.stop_notify).Done():
			empty_cond.L.Unlock()
			return nil,contextDoneErr
		case <-time.After(10*time.Second):
		}
		goto empty_retry
	}
	if len(block) == 0 {
		//进入阻塞状态，等待有事件写入再唤起
		empty_cond.Wait()
		goto empty_retry
	}
	empty_cond.L.Unlock()

	//计算当前时间距离最早取消还有多久，单位毫秒
	score   = int64(block[0].Score)
	nowtime = time.Now().UnixNano() / 1000000
	tiggerOffset = score - nowtime
	wait_second  = time.Duration(tiggerOffset)*time.Millisecond
	//时间已经到了直接返回
	if tiggerOffset <= 0 {
		return block,err
	}
	//检查是否已有smaller通知
	hasSmaller = wTimer.IfCanceldRecover()
	//有更小的头元素转去拉取最小的等待
	//当被取消时，定时器倒计时为0
	if hasSmaller == true {
		goto rePeek
	}
	wTimer.ResetTimer(wait_second)
	select{
	case <-(this.stop_notify).Done():
		return nil,contextDoneErr
	case <-wTimer.timer.C:
		//唤醒后需要检查定时是否被取消
		hasSmaller = wTimer.IfCanceldRecover()
		if hasSmaller == true {
			goto rePeek
		}
	}
	return block,err
}




func (this *TimeAfter) pullerRun( sortqueue string,empty_cond *sync.Cond,wTimer *waitTimer ) {
	this.puller_count.Add(1)
	defer this.puller_count.Done()
	var (
		err          error
		front        []redis.Z
		member       string
		score        int64
		result       interface{}
		newcount     uint32
		md5sum       [16]byte
		newrnd       int
		newname      string
		str2byte     reflect.SliceHeader
	)
	enBuffer := make([]byte,base64.RawStdEncoding.EncodedLen(16))
	joinbuf := bytes.NewBuffer([]byte{})
	joinbuf.Grow(120)
	var consisKeys   []string
	for {
		front,err = this.peekWait(sortqueue,empty_cond,wTimer)
		if nil != err {
			if err == contextDoneErr {
				return
			}
			logrus.Error("timeAfter peek wait err: ",err)
			select{
			case <-(this.stop_notify).Done():
				return
			case <-time.After(10*time.Second):
			}
			continue
		}
		score  = int64(front[0].Score)
		member = front[0].Member.(string)

		//完成等待后检查关闭指令，不进行处理
		select{
		case <-(this.stop_notify).Done():
			return
		default:
		}

		//验证是否允许投递任务
		this.task_cond.L.Lock()
concurrent_retry:
		select{
		case <-(this.stop_notify).Done():
			this.task_cond.L.Unlock()
			return
		default:
		}
		this.task_count.Add(1)
		newcount,_ = helper.ReadWaitgroup(&this.task_count)
		if uint64(newcount) > uint64(this.concurrent) {
			this.task_count.Done()
			//等待有任务退出后再继续
			//可以是自己投递的任务，也可以是其他puller投递的任务完成后，唤起
			this.task_cond.Wait()
			goto concurrent_retry
		}
		this.task_cond.L.Unlock()

		//准备datakey的新名称
		str2byte = *(*reflect.SliceHeader)(unsafe.Pointer(&member))
		str2byte.Cap = str2byte.Len
		md5sum  = md5.Sum(*(*[]byte)(unsafe.Pointer(&str2byte)))
		//helper.Hash16String(md5sum,encodebuf)
		base64.RawStdEncoding.Encode(enBuffer,md5sum[:])
		newrnd  = rand.Intn(int(math.MaxUint32))
		joinbuf.Truncate(0)
		joinbuf.WriteString(dataRenamePrefix)
		joinbuf.WriteString(*(*string)(unsafe.Pointer(&enBuffer)))
		joinbuf.WriteString(strconv.FormatInt(int64(newrnd),36))
		newname = joinbuf.String()
		joinbuf.Truncate(0)
		
		//验证头元素数据一致性，如一致则拿掉头元素开始任务处理过程
		consisKeys = []string{
			sortqueue,
			member,
			strconv.FormatInt(score,10),
			newname,
		}
		result,err =this.redis_cli.EvalSha(consisScript,consisKeys).Result()
		if nil != err {
			//发生错误提前结束任务计数
			this.task_cond.L.Lock()
			this.task_count.Done()
			this.task_cond.Signal()
			this.task_cond.L.Unlock()
			//datakey已过期，将datakey重命名为新名称时会发现key不存在
			//这个错误可以忽略
			if true == strings.Contains(err.Error(),"ERR no such key") {
				logrus.Warn(err)
			} else {
				logrus.Error(err)
			}
			select{
			case <-(this.stop_notify).Done():
				return
			case <-time.After(10*time.Second):
			}
			continue
		}
		if result.(int64) == -1 {
			//不一致重新从头元素开始拉取
			this.task_cond.L.Lock()
			this.task_count.Done()
			this.task_cond.Signal()
			this.task_cond.L.Unlock()
			continue
		}

		//调用用户函数
		serv.GoCounter.Add(1)
		go func(event_key string,tigger_time int64 ) {
			defer serv.GoCounter.Done()
			this.tigger_fn(this.redis_cli,event_key,tigger_time)
			//完成调用后删除与事件绑定的数据
			err := this.redis_cli.Del(event_key).Err()
			if nil != err {
				logrus.Error(err)
			}
			//无条件唤起一个被阻塞的请求，防止死锁
			//不会产生压栈问题，没有等待者就不会有唤起
			//待有了等待者，不会因为之前已经通知过不阻塞
			this.task_cond.L.Lock()
			this.task_count.Done()
			this.task_cond.Signal()
			this.task_cond.L.Unlock()
		}(newname,score)
	}
}


