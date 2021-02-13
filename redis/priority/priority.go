package priority

import (
	"errors"
	"github.com/go-redis/redis"
	"github.com/lucidStream/coify/serv"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"sync"
	"time"
	coifyRedis "github.com/lucidStream/coify/redis"
)



type RedisCli coifyRedis.CoifyRedis


//redis优先级队列
type PriorityRedisQueue struct {
	key           string
	redis_cli     *RedisCli
	stop_notify   context.Context
	stop_call     context.CancelFunc
	cond          *sync.Cond
	puller_count  *sync.WaitGroup   //运行中的拉取者计数
}



//实例初始化
func NewPriorityRedisQueue( key string,cli *RedisCli,mutex *sync.Mutex ) (PriorityRedisQueue) {
	var queue PriorityRedisQueue
	queue.key = key
	queue.redis_cli = cli
	ctx,cancel := context.WithCancel(context.Background())
	queue.stop_notify,queue.stop_call = ctx,cancel
	queue.cond = sync.NewCond(mutex)
	queue.puller_count = &sync.WaitGroup{}
	//监听服务器关闭
	go func() {
		defer func() { queue.Destory() }()
		select{
		case <-queue.stop_notify.Done():
			//当实例关闭时也要能退出，不能一直等服务器关闭
			return
		case <-serv.ServerCtx.Done():
			//转到定时器关闭流程
			return
		}
	}()
	return queue
}




func (this *PriorityRedisQueue) ShareContext() (context.Context,context.CancelFunc) {
	return this.stop_notify,this.stop_call
}





//在封装对象的情况下可能多次调用
func (this *PriorityRedisQueue) Destory() {
	//发送停止通知
	(this.stop_call)()
	cond_lock := this.cond
	if nil == cond_lock {
		return
	}
	cond_lock.L.Lock()
	if this.cond == nil {
		cond_lock.Broadcast()
		cond_lock.L.Unlock()
		return
	}
	this.cond = nil
	cond_lock.Broadcast()
	cond_lock.L.Unlock()
	doneCh := make(chan struct{},2)
	defer close(doneCh)
	go func() {
		//等待puller退出
		this.puller_count.Wait()
		doneCh <- struct{}{}
	}()
	for i:=0; i<1; i++ {
		select{
		case <-doneCh:
		case <-time.After(10*time.Second):
			//如果超时可能存在死锁
			logrus.Error("wait puller_count done timeout")
			return
		}
	}
}







type PushCommand uint64

const(
	Default = PushCommand(1) << iota
	XX
	NX
	CH
	INCR
)



type QueueChain struct {
	command    PushCommand
	option1    PushCommand
	members    []redis.Z
	queue      *PriorityRedisQueue
}



func (this *PriorityRedisQueue) Command( c PushCommand ) (*QueueChain,error) {
	var chain QueueChain
	if c & Default == Default {
		//不能包含NX或XX
		if c & NX == NX {
			return &chain,errors.New("default cannot container NX")
		}
		if c & XX == XX {
			return &chain,errors.New("default cannot container XX")
		}
		//zadd默认是不带CH选项的，zadd只返回添加的成员数
		//如果zadd操作的是一个已存在的成员，不带CH返回值为0
		chain.option1 = Default
	}
	//XX和NX只能选其一
	if c & XX == XX {
		if c & NX == NX {
			return &chain,errors.New("XX|NX pick one")
		}
		chain.option1 = XX
	}
	if c & NX == NX {
		if c & XX == XX {
			return &chain,errors.New("NX|XX pick one")
		}
		chain.option1 = NX
	}
	chain.command = c
	chain.queue   = this
	return &chain,nil
}



func (this *QueueChain) Push( members []redis.Z ) (int64,error) {
	var ( affectCount int64;err error )
	switch this.option1 {
	case Default://default
		switch this.command {
		case Default:
			affectCount,err = this.queue.redis_cli.ZAdd(this.queue.key,members...).Result()
		case Default|CH:
			affectCount,err = this.queue.redis_cli.ZAddCh(this.queue.key,members...).Result()
		case Default|INCR:
			//err = this.queue.redis_cli.ZIncr()
			//redis驱动ZIncr只能传一个成员，暂不实现
			panic("no implement: Default|INCR")
		case Default|CH|INCR:
			//没有对应命令
			panic("no implement: Default|CH|INCR")
		default:
			return 0, errors.New("unrecognized pattern")
		}
	case NX:
		switch this.command {
		case NX:
			affectCount,err = this.queue.redis_cli.ZAddNX(this.queue.key,members...).Result()
		case NX|CH:
			affectCount,err = this.queue.redis_cli.ZAddNXCh(this.queue.key,members...).Result()
		case NX|INCR:
			//this.queue.redis_cli.ZIncrNX()
			//只能传一个成员
			panic("no implement: NX|INCR")
		case NX|CH|INCR:
			//没有对应命令
			panic("no implement: NX|CH|INCR")
		default:
			return 0, errors.New("unrecognized pattern")
		}
	case XX:
		switch this.command {
		case XX:
			affectCount,err = this.queue.redis_cli.ZAddXX(this.queue.key,members...).Result()
		case XX|CH:
			affectCount,err = this.queue.redis_cli.ZAddXXCh(this.queue.key,members...).Result()
		case XX|INCR:
			//this.queue.redis_cli.ZIncrXX()
			panic("no implement: XX|INCR")
		case XX|CH|INCR:
			//没有对应命令
			panic("no implement: XX|CH|INC")
		default:
			return 0, errors.New("unrecognized pattern")
		}
	default:
		return 0,errors.New("unknown command")
	}
	if nil != err {
		return 0,err
	}
	this.queue.cond.L.Lock()
	this.queue.cond.Signal()
	this.queue.cond.L.Unlock()
	return affectCount,nil
}







var(
	QueueDoneErr   = errors.New("queue instance already done")
	ErrorDirection = errors.New("error direction")
)

type PopDirection int

const (
	Min = PopDirection(1)
	MAX = PopDirection(2)
)

func (this *PriorityRedisQueue) Pop( direction PopDirection ) ([]byte,float64,error) {
	this.puller_count.Add(1)
	defer this.puller_count.Done()
	var result    []byte
	var score     float64
	var err       error
	var front     []redis.Z
	for {
		this.cond.L.Lock()
empty_retry:
		select{
		case <-(this.stop_notify).Done():
			this.cond.L.Unlock()
			return result,score,QueueDoneErr
		default:
		}

		switch direction {
		case Min:
			front,err = this.redis_cli.ZPopMin(this.key,1).Result()
		case MAX:
			front,err = this.redis_cli.ZPopMax(this.key,1).Result()
		default:
			this.cond.L.Unlock()
			return nil,0,ErrorDirection
		}

		if nil != err {
			logrus.Error(err)
			select {
			case <-this.stop_notify.Done():
				this.cond.L.Unlock()
				return result,score,QueueDoneErr
			case <-time.After(10*time.Second):
			}
			goto empty_retry
		}
		if len(front) == 0 {
			//进入阻塞状态，等待有事件写入再唤起
			this.cond.Wait()
			goto empty_retry
		}
		this.cond.L.Unlock()

		score  = front[0].Score
		result = []byte(front[0].Member.(string))
		return result,score,err
	}
}




