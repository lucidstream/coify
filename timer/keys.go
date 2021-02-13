package timer

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"github.com/lucidStream/coify/helper"
	"strconv"
	"unsafe"
)




type RabStract struct {
	Datakey       string `json:"data_key"`
	SortQueue     string `json:"sort_queue"`
	DatakeyHash   uint64 `json:"datakey_hash"`
}




func (this *TimeAfter) keysJoin ( sortkeys []string,value map[string]string,rkeys *RabStract ) {
	//根据排序结果拼接字符串
	joinbuf := bytes.NewBuffer([]byte{})
	joinbuf.Grow(260)
	keysLen := len(sortkeys)
	for i:=0; i<keysLen; i++ {
		joinbuf.WriteString(sortkeys[i])
		joinbuf.WriteString(":")
		joinbuf.WriteString(value[sortkeys[i]])
		joinbuf.WriteString(";")
	}
	enBuffer := make([]byte,base64.RawStdEncoding.EncodedLen(32))
	sum256 := sha256.Sum256(joinbuf.Bytes())
	//fmt.Println(base64.StdEncoding.EncodeToString(sum256[:]))
	base64.RawStdEncoding.Encode(enBuffer,sum256[:])
	sha256_value := *(*string)(unsafe.Pointer(&enBuffer))
	joinbuf.Truncate(0)
	//拼接datakey
	datakey := this.format_data_key(joinbuf,&sha256_value)
	//根据hash值分发到不同的队列
	datakeyhash := helper.BKDRHash(datakey)
	remainder   := strconv.FormatUint(datakeyhash%uint64(this.puller),10)
	sortqueue   := this.format_sort_queue(joinbuf,remainder)
	rkeys.Datakey      = datakey
	rkeys.SortQueue    = sortqueue
	rkeys.DatakeyHash  = datakeyhash
}





func (this *TimeAfter) format_data_key(joinbuf *bytes.Buffer,sha256 *string) string {
	if nil == joinbuf {
		joinbuf = bytes.NewBuffer([]byte{})
		joinbuf.Grow(260)
	}
	joinbuf.Truncate(0)
	joinbuf.WriteString("timeafter:clinet=")
	//多个客户端上创建的定时器实例，即使事件值完全相同，不同客户端也需隔离
	//避免多个客户端操作同一个实例的datakey
	joinbuf.WriteString(this.redis_cli.Name)
	joinbuf.WriteString(":sha256=")
	joinbuf.WriteString(*sha256)
	datakey := joinbuf.String()
	joinbuf.Truncate(0)
	return datakey
}






func (this *TimeAfter) format_sort_queue( joinbuf *bytes.Buffer,remainder string) string {
	if nil == joinbuf {
		joinbuf = bytes.NewBuffer([]byte{})
		joinbuf.Grow(260)
	}
	joinbuf.Truncate(0)
	joinbuf.WriteString(this.sort_queue)
	joinbuf.WriteString(":remainder=")
	joinbuf.WriteString(remainder)
	sortqueue := joinbuf.String()
	joinbuf.Truncate(0)
	return sortqueue
}
