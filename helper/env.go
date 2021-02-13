package helper

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"unsafe"
)

const TimeLayout string = "2006-01-02 15:04:05"


type Environ string
func (this Environ) Value() string {
	return os.Getenv(string(this))
}
func (this Environ) TrimSpaceVal() string {
	return strings.Trim(this.Value()," ")
}



//检查端口监听
//http服务的启动顺序可以直接用定时器，当A启动失败直接panic就行了，启动B的定时器即得不到执行
func CheckPortListen( port int64 ) error {
	cmdStr := fmt.Sprintf("lsof -i :%d",port)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	outbuf, err := cmd.Output()
	if err != nil {
		return err
	}
	outstr  := string(outbuf)
	outline := strings.Split(outstr,"\n")
	var not_empty_line_count int64
	for _,v := range outline {
		if len(strings.Trim(v," ")) > 0 {
			not_empty_line_count++
		}
	}
	if not_empty_line_count >= 2 {
		return nil
	}
	return errors.New("unlisten state")
}



type noCopy struct{}

type WaitGroup struct {
	noCopy noCopy

	// 64-bit value: high 32 bits are counter, low 32 bits are waiter count.
	// 64-bit atomic operations require 64-bit alignment, but 32-bit
	// compilers do not ensure it. So we allocate 12 bytes and then use
	// the aligned 8 bytes in them as state.
	state1 [12]byte
	sema   uint32
}




func ReadWaitgroup( wg *sync.WaitGroup ) (uint32,uint32) {
	gocounter := (*WaitGroup)(unsafe.Pointer(wg))
	var state_pointer *uint64
	if uintptr(unsafe.Pointer(&gocounter.state1))%8 == 0 {
		state_pointer = (*uint64)(unsafe.Pointer(&gocounter.state1))
	} else {
		state_pointer = (*uint64)(unsafe.Pointer(&gocounter.state1[4]))
	}
	waiter  := (*uint32)(unsafe.Pointer(state_pointer))
	counter := (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(state_pointer)) + 4 ))
	return *counter,*waiter
}