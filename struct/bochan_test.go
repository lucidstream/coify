package _struct

import (
	"fmt"
	"strconv"
	"time"
)

func test() {
	channel := NewBroadcastChanel(&BroadcastChanel{})
	produce_key := channel.Produce.New(100)
	consumer1_key := channel.Consumer.New(100,30*time.Second)
	consumer2_key := channel.Consumer.New(100,30*time.Second)

	go func() {
		for {
			message,err := channel.Consumer.ReadMessage(consumer1_key,10*time.Second)
			if nil != err {
				if err == ReadTimeoutErr { continue }
				panic(err)
			}
			fmt.Println("consumer1:",message)
		}
	}()

	go func() {
		for {
			message,err := channel.Consumer.ReadMessage(consumer2_key,10*time.Second)
			if nil != err {
				if err == ReadTimeoutErr { continue }
				panic(err)
			}
			fmt.Println("consumer2:",message)
		}
	}()


	//动态加入消费者
	go func() {
		time.Sleep(10*time.Second)
		consumer3_key := channel.Consumer.New(100,30*time.Second)
		for {
			message,err := channel.Consumer.ReadMessage(consumer3_key,10*time.Second)
			if nil != err {
				if err == ReadTimeoutErr { continue }
				panic(err)
			}
			fmt.Println("consumer3:",message)
		}
	}()


	//读取消息迟缓的消费者
	//将出现WARN[0181] Not all forwardingforwardCount警告
	go func() {
		time.Sleep(12*time.Second)
		consumer4_key := channel.Consumer.New(100,30*time.Second)
		for {
			message,err := channel.Consumer.ReadMessage(consumer4_key,10*time.Second)
			if nil != err {
				if err == ReadTimeoutErr { continue }
				panic(err)
			}
			fmt.Println("consumer4:",message)
			time.Sleep(28*time.Second)
		}
	}()



	//被强制注销的消费者
	//被注销后将找不到该消费者
	go func() {
		consumer5_key := channel.Consumer.New(100,30*time.Second)
		for {
			message,err := channel.Consumer.ReadMessage(consumer5_key,10*time.Second)
			if nil != err {
				if err == ReadTimeoutErr { continue }
				if err.Error() == "Not found consumer" {
					fmt.Println(err.Error())
					return
				}
				panic(err)
			}
			fmt.Println("consumer5:",message)
			time.Sleep(33*time.Second)
		}
	}()



	runing,err := channel.Run()
	if nil != err { panic(err) }
	if runing == 0 {
		panic("no worker runing")
	}

	for i:=0; i<10000; i++ {
		err := channel.Produce.Push(produce_key,&BroadcastMessage{
			Id : int64(i),//可选
			Type : 0, //消息类型0默认 1排除 2指定
			Keys : nil,//排除或指定的键
			Content : strconv.Itoa(i)+"_message" ,//消息内容
		})
		if nil != err { panic(err) }
		time.Sleep(1*time.Second)
	}
}