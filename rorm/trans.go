package rorm

import (
	"crypto/sha256"
	"encoding/base64"
	"github.com/lucidStream/coify/helper"
	"strings"
)

//严格的事务应该做到多个数据查询/写指令为一组，生成一组随机键名，为每一条数据添加一个指针指向一个事务key
//然而这样做的前提是要能准确分析出一个事务这个逻辑组合中操作的每一条数据ID
//在一些lua语句中，往往只是一些查询的必要条件，要知道这些查询或操作语句具体影响到哪些具体ID数据，只有在语句真正执行后才能知道
//无法在语句执行前确定指令影响的数据ID，就无法提前创建好排他键
//因此rorm的实现使用token+自定义事务键的方式实现，这样既保证了灵活性，又避免了不相关的业务映射到相同的键上


type Transkey struct {
	token            string
	fullName         bool //是否保存完整键名
}


func NewTranskey( fullName bool ) Transkey {
	token := helper.FileLineToken(2)
	return Transkey{ token,fullName }
}



func (this *Transkey) Sum( a ...string ) string {
	a = append(a,this.token)
	key := strings.Join(a,":")
	if true == this.fullName {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	b64 := base64.RawStdEncoding.EncodeToString(sum[:])
	return b64
}



