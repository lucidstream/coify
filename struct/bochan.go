package _struct

import (
	"container/list"
	"errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"sync"
	"time"
)

//广播channel


//广播消息结构
type BroadcastMessage struct{
	Id         int64 //可选
	Type       int //消息类型0默认 1排除 2指定
	Keys       []string//排除或指定的键
	Content    interface{}//消息内容
}



type BroadcastProduce struct{
	producers        *list.List
	offlineProduce   map[string]*list.Element//离线生产者
	changeSync       *sync.RWMutex
}



type ConsumerKeepAlive struct{
	element     *list.Element
	clearTimer  *time.Timer
	cancelTimer chan struct{}
	deadline    time.Duration//失效倒计时
}


type BroadcastConsumer struct{
	consumers        *list.List
	offlineConsumer  map[string]*ConsumerKeepAlive//离线消费者
	changeSync       *sync.RWMutex
}


type BroadcastChanel struct{
	Produce        BroadcastProduce
	Consumer       BroadcastConsumer
}


//新实例
func NewBroadcastChanel( channel *BroadcastChanel ) *BroadcastChanel {
	channel.Produce.producers  = new(list.List)
	channel.Consumer.consumers = new(list.List)
	channel.Produce.offlineProduce   = make(map[string]*list.Element)
	channel.Consumer.offlineConsumer = make(map[string]*ConsumerKeepAlive)
	channel.Produce.changeSync  = &sync.RWMutex{}
	channel.Consumer.changeSync = &sync.RWMutex{}
	return channel
}



//新生产者
func (this *BroadcastProduce) New( size int ) (string) {
	channel := make(chan BroadcastMessage,size)
	u1 := uuid.Must(uuid.NewV4())
	key := u1.String()

	this.changeSync.Lock()
	element := this.producers.PushBack(channel)
	this.offlineProduce[key] = element
	this.changeSync.Unlock()

	return key
}



func (this *BroadcastProduce) Remove( key string ) error {
	this.changeSync.Lock()
	defer this.changeSync.Unlock()
	element,ok := this.offlineProduce[key]
	if false == ok {
		return errors.New("Not found produce")
	}
	_=this.producers.Remove(element)
	delete(this.offlineProduce,key)
	channel := element.Value.(chan BroadcastMessage)
	close(channel)
	return nil
}





func (this *BroadcastProduce) Push( key string,message *BroadcastMessage ) error {
	this.changeSync.RLock()
	defer this.changeSync.RUnlock()
	element,ok := this.offlineProduce[key]
	if false == ok {
		return errors.New("Not found produce")
	}
	channel := element.Value.(chan BroadcastMessage)
	//往管道写数据需要保证管道未关闭
	//否则将引发panic错误
	select{
	case channel <- *message:
		return nil
	default:
		return errors.New("cannot write to produce channel")
	}
}





//消费者过久无消费则清除该消费者
func (this *BroadcastConsumer) New( size int,deadline time.Duration ) (string) {
	channel := make(chan BroadcastMessage,size)
	u1 := uuid.Must(uuid.NewV4())
	key := u1.String()
	clearTimer   := time.NewTimer(deadline)
	cancelNotify := make(chan struct{},1)
	consumer_k := &ConsumerKeepAlive{
		nil,
		clearTimer,
		cancelNotify,
		deadline,
	}

	this.changeSync.Lock()
	element := this.consumers.PushBack(channel)
	consumer_k.element = element
	this.offlineConsumer[key] = consumer_k
	this.changeSync.Unlock()

	//监听倒计时事件
	go func() {
		select{
		case <-cancelNotify:
		case <-clearTimer.C:
			_=this.Remove(key)
		}
	}()

	return key
}




//移除消费者，停止倒计时
func (this *BroadcastConsumer) Remove( key string ) error {
	this.changeSync.Lock()
	defer this.changeSync.Unlock()
	element,ok := this.offlineConsumer[key]
	if false == ok {
		return errors.New("Not found consumer")
	}
	element.clearTimer.Stop()
	select{
	case element.cancelTimer <- struct{}{}:
	default:
	}
	_=this.consumers.Remove(element.element)
	delete(this.offlineConsumer,key)
	channel := element.element.Value.(chan BroadcastMessage)
	close(channel)
	return nil
}



func (this *BroadcastConsumer) RemoveAll() error {
	for k,_ := range this.offlineConsumer {
		err := this.Remove(k)
		if nil != err { return err }
	}
	return nil
}



var (
	ReadTimeoutErr = errors.New("timeout")
	ReadCtxDoneErr = errors.New("context done")
)
type ConsumerMessage struct{Id int64;Content interface{}}

type ReadMessageWithContext struct{
	*BroadcastConsumer
	ctx context.Context
}


func (this *BroadcastConsumer) Context( ctx context.Context ) *ReadMessageWithContext {
	return &ReadMessageWithContext{
		this,
		ctx,
	}
}


func (this *ReadMessageWithContext) ReadMessage( key string,timeout time.Duration ) (*ConsumerMessage,error) {
	return this.readMessage(key,timeout,this.ctx)
}




func (this *BroadcastConsumer) ReadMessage( key string,timeout time.Duration ) (*ConsumerMessage,error) {
	return this.readMessage(key,timeout,nil)
}





//重置消费者失效倒计时
func (this *BroadcastConsumer) ResetDeadline( key string ) (*ConsumerKeepAlive,error) {
	this.changeSync.RLock()
	element,ok := this.offlineConsumer[key]
	if false == ok {
		this.changeSync.RUnlock()
		return nil,errors.New("Not found consumer")
	}
	element.clearTimer.Reset(element.deadline)
	this.changeSync.RUnlock()
	return element,nil
}




//消费者读取消息
//调用一次读取，延后失效倒计时
func (this *BroadcastConsumer) readMessage( key string,timeout time.Duration,ctx context.Context ) (*ConsumerMessage,error) {
	element,err := this.ResetDeadline(key)
	if nil != err { return nil,err }
	channel := element.element.Value.(chan BroadcastMessage)
	var ( result BroadcastMessage; ok bool )
	if nil != ctx {
		select{
		case <-ctx.Done():
			return nil,ReadCtxDoneErr
		default:
			select{
			case <-ctx.Done():
				return nil,ReadCtxDoneErr
			case result,ok = <-channel:
				if false == ok {
					return nil,errors.New("consumer closed.")
				}
				return &ConsumerMessage{
					Id: result.Id,
					Content: result.Content,
				},nil
			case <-time.After(timeout):
				return nil,ReadTimeoutErr
			}
		}
	} else {
		select{
		case result,ok = <-channel:
			if false == ok {
				return nil,errors.New("consumer closed.")
			}
			return &ConsumerMessage{
				Id: result.Id,
				Content: result.Content,
			},nil
		case <-time.After(timeout):
			return nil,ReadTimeoutErr
		}
	}
}




var (
	InvalidMessageError = errors.New("list element not valid broadcast message")
)


//监听生产者消息，转发给可用消费者
//不作路由匹配等复杂功能，仅支持排除某些消费者，或仅指定某些消费者
//分组等功能可通过上层再次封装实现
func (this *BroadcastChanel) Run() (int,error) {
	var reciverCount = 0
	this.Produce.changeSync.RLock()
	defer this.Produce.changeSync.RUnlock()
	element := this.Produce.producers.Front()
	for element != nil {
		channel,ok := element.Value.(chan BroadcastMessage)
		if false == ok {
			return 0,InvalidMessageError
		}
		go this.reciverProduce(channel)
		element = element.Next()
		reciverCount++
	}
	return reciverCount,nil
}



//接收生产者消息
func (this *BroadcastChanel) reciverProduce( channel chan BroadcastMessage ) {
	var (
		message      BroadcastMessage
		ok           bool
		forwardCount int
		err          error
	)
	for {
		message,ok = <-channel
		if false == ok { break }
		switch message.Type {
		case 1://排除
		case 2://指定
		default:
			//默认全部转发
			forwardCount,err = this.forwardingFullConsumer(&message)
			if nil != err {
				switch err {
				case NotFullForwardErr:
					logrus.Warn(err,"forwardCount:",forwardCount)
				default:
					logrus.Error(err,"forwardCount:",forwardCount)
					time.Sleep(10*time.Second)
				}
			}
		}
	}
}




var NotFullForwardErr = errors.New("Not all forwarding")


func (this *BroadcastChanel) forwardingFullConsumer( message *BroadcastMessage ) (int,error) {
	this.Consumer.changeSync.RLock()
	defer this.Consumer.changeSync.RUnlock()
	front := this.Consumer.consumers.Front()
	var (
		channel       chan BroadcastMessage
		ok            bool
		forwardCount  int
		consumerCount int
	)
	for front != nil {
		channel,ok = front.Value.(chan BroadcastMessage)
		if false == ok {
			return 0,InvalidMessageError
		}
		select{
		case channel <- *message:
			forwardCount++
		default:
			//无法写入消息，不能因为无法向一个消费者转发消息，就卡住了
		}
		front = front.Next()
		consumerCount++
	}
	if forwardCount != consumerCount {
		return forwardCount,NotFullForwardErr
	}
	return forwardCount,nil
}