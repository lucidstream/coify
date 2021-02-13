package serv

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/lucidStream/coify/helper"
	"github.com/sirupsen/logrus"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)


var (
	graceRestart = flag.Uint64("grace_restart",0,"grace father process id")
	Listeners        [3]net.Listener
	GoCounter        sync.WaitGroup
	ServerCtx,stopfn = context.WithCancel(context.Background())
)



//启动后的子进程第一件事就是要等父进程把事情处理完
func GraceWaitReady() {
	flag.Parse()
	if false == GraceRestart() {
		return
	}
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGUSR2)
	fmt.Println("father pid: ",*graceRestart)
	fmt.Println("my pid: ",os.Getpid())
	err := syscall.Kill(int(*graceRestart),syscall.SIGUSR2)
	if nil != err {
		panic("send signal usr2 to father err")
	} else {
		fmt.Println("telled father i ready")
	}
	fmt.Println("wait signal usr2...")
	for {
		s := <-c
		if "user defined signal 2" == s.String() {
			break
		}
	}
	fmt.Println("grace starting...")
}



func GraceRestart() bool { return *graceRestart > 0 }




var osArgsExclude = []string{
	"-grace_restart",
}


func AddExcludeArgs( key string ) error {
	key = strings.Trim(key," ")
	key = strings.TrimLeft(key,"-")
	if len(key) < 2 {
		return errors.New("key too short")
	}
	osArgsExclude = append(osArgsExclude,"-"+key)
	return nil
}



func GetOsArgsExclude() []string {
	osArgs := make([]string,0,len(os.Args))
	osArgs = append(osArgs,os.Args[0])
	var key_trim string
	for i:=1; i<len(os.Args); i+=2 {
		key_trim = strings.Trim(os.Args[i]," ")
		if helper.StringArrayIn(key_trim,osArgsExclude) {
			continue
		}
		osArgs = append(osArgs,key_trim)
		osArgs = append(osArgs,os.Args[i+1])
	}
	return osArgs
}





var childPidNotify = make(chan int,1)


func Reload( osArgs []string ) error {
	//保证子进程启动前没有已就绪信号
	//可能因为错误的命令行操作导致队列中有无效的就绪信号
clearReady:	 for {
		select{
		case <-readyNotify:
		default:
			break clearReady
		}
	}
	//启动子进程（子进程暂时不启动任务服务）
	fdfiles := []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd()}
	//Listeners需要是数组保证顺序
	for _,v := range Listeners {
		if nil == v {
			//空位用maxuint占位
			fdfiles = append(fdfiles, math.MaxUint64)
			continue
		}
		lnfile,err := v.(*net.TCPListener).File()
		if nil != err {
			return err
		}
		fdfiles = append(fdfiles, lnfile.Fd())
	}
	execSpec := &syscall.ProcAttr{
		Env   :   os.Environ(),
		Files :   fdfiles,
	}
	myPid := strconv.Itoa(os.Getpid())
	osArgs = append(osArgs,"-grace_restart",myPid)
	fmt.Println("restart command: ",os.Args[0],osArgs,fdfiles)
	//需要启动成功再停止服务
	//内存不足的情况下很有可能启动失败
	pid, err := syscall.ForkExec(os.Args[0], osArgs, execSpec)
	if nil != err {
		logrus.Errorf("fork err: ",err)
		return err
	}
	select {
	case childPidNotify <- pid:
	default:
		logrus.Error("unable write pid notify")
	}
	//父进程停止接受新连接（父进程停止服务后，再给子进程发命令注册服务）
	return Stop()
}



func Stop() error {
	//父进程停止接受新连接
	for _,v := range Listeners {
		if v == nil {
			continue
		}
		if err := v.Close();nil != err {
			logrus.Errorf("close listener err: ",err)
			return err
		}
	}
	//父进程全局发送停止信号
	stopfn()
	return nil
}



var readyNotify = make(chan struct{},1)


//信号处理
func SignalHandler() {
	c := make(chan os.Signal)
	sigs := []os.Signal{
		syscall.SIGUSR1,
		syscall.SIGUSR2,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	}
	signal.Notify(c, sigs...)
	for {
		s := <-c
		if "user defined signal 1" == s.String() {
			osArgs := GetOsArgsExclude()
			if err := Reload(osArgs) ;nil != err {
				logrus.Error(err)
			}
			logrus.Info("signal reload now")
		} else if "user defined signal 2" == s.String() {
			//把子进程启动起来后，会给父进程发sig2信号
			//父进程收到后给子进程发启动信号
			//无法发送就绪信号不应该阻塞信号循环
			select{
			case readyNotify <- struct{}{}:
				logrus.Info("write child process ready notify")
			default:
				logrus.Error("unable write ready notify")
			}
		} else {
			if err := Stop(); nil != err {
				logrus.Error(err)
			}
			logrus.Info("signal stop now")
		}
	}
}




func WaitComplete( onShutdown func() error ) error {
	//服务关闭回调
	defer func() {
		if nil == onShutdown {
			return
		}
		if err := onShutdown();nil != err {
			logrus.Error(err)
		}
		outputHandle := logrus.StandardLogger().Out
		switch outputHandle.(type) {
		case *os.File:
		case *bufio.Writer:
			_=outputHandle.(*bufio.Writer).Flush()
		default:
		}
	}()
	wait_all_done := make(chan struct{},1)
	go func() {
		GoCounter.Wait()
		wait_all_done <- struct{}{}
	}()
	select {
	case <-wait_all_done:
	case <-time.After(60*time.Second):
		return errors.New("wait counter done timeout")
	}
	//父进程即将推出再给子进程发送启动信号
	select {
	case pid:=<-childPidNotify:
		if pid <= 0 {
			err := errors.New("find a invalid pid number")
			logrus.Error(err)
			return err
		}
		//等待子进程就绪信号
		select {
		case <-readyNotify:
			fmt.Println("send usr2 signal to child pid: ",pid)
			err := syscall.Kill(pid,syscall.SIGUSR2)
			if nil != err {
				logrus.Error(err)
				return err
			}
		case <-time.After(30*time.Second):
			err := errors.New("wait child process ready timeout")
			return err
		}
	default:
	}
	return nil
}
