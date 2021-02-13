package filter

import (
	"errors"
	"strings"
)



type Helper struct {
	extra map[string]interface{}
	full  map[string]interface{}
	Field string
}

type Uint64 struct {
	Helper
	That uint64
}

type Int64 struct {
	Helper
	That int64
}

type String struct {
	Helper
	That string
}

type Float64 struct {
	Helper
	That float64
}

type Bool struct {
	Helper
	That bool
}



type Array struct {
	Helper
	That []interface{}
}


type Json struct {
	Helper
	That map[string]interface{}
}



func (this* Helper) SetFullValue( full map[string]interface{} ) {
	this.full = full
}


func (this* Helper) SetExtra( extra map[string]interface{} ) {
	this.extra = extra
}




//确定浮点数精度
func (this* Float64) Prec2() float64 {
	this.That = float64(int64(this.That*100))/100
	return this.That
}


func (this* Float64) Prec4() float64 {
	this.That = float64(int64(this.That*10000))/10000
	return this.That
}







//获取全部值
func (this* Uint64) GetFullValue() map[string]interface{} { return this.full }

func (this* Int64) GetFullValue() map[string]interface{} { return this.full }

func (this* String) GetFullValue() map[string]interface{} { return this.full }

func (this* Float64) GetFullValue() map[string]interface{} { return this.full }

func (this* Bool) GetFullValue() map[string]interface{} { return this.full }

func (this* Array) GetFullValue() map[string]interface{} { return this.full }

func (this* Json) GetFullValue() map[string]interface{} { return this.full }



//获取指定字段值
func (this* Uint64) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this* Int64) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this* String) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this* Float64) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this* Bool) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this* Array) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this* Json) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}




//去除左边指定符号
func (this *String) TrimLeft( s string ) string {
	return strings.TrimLeft(this.That,s)
}

//去除右边指定符号
func (this *String) TrimRight( s string ) string {
	return strings.TrimRight(this.That,s)
}

//去除两边指定符号
func (this *String) Trim( s string ) string {
	return strings.Trim(this.That,s)
}

func (this *String) TrimLRSpace() string {
	return strings.Trim(this.That," ")
}

//todo 去除html标签
func (this *String) RemoveHtmlTags() string {
	return this.That
}




//当number超出范围时，取最接近的边界的值
func (this *Uint64) IfOverflowBoundary( left uint64,right uint64 ) (uint64,error) {
	if left >= right {
		return 0,errors.New("boundary error")
	}
	if this.That < left || this.That > right {
		middle := (right + left) / 2
		if this.That > middle {
			return right,nil
		} else {
			return left,nil
		}
	}
	return this.That,nil
}




func (this *Int64) IfOverflowBoundary( left int64,right int64 ) (int64,error) {
	if left >= right {
		return 0,errors.New("boundary error")
	}
	if this.That < left || this.That > right {
		middle := (right + left) / 2
		if this.That > middle {
			return right,nil
		} else {
			return left,nil
		}
	}
	return this.That,nil
}




func (this *Float64) IfOverflowBoundary( left float64,right float64,retain float64 ) (float64,error) {
	that   := int64(this.That * retain)
	_left  := int64(left * retain)
	_right := int64(right * retain)
	if _left >= _right {
		return 0,errors.New("boundary error")
	}
	if that < _left || that > _right {
		middle := (_right + _left) / 2
		if that > middle {
			return float64(_right)/retain,nil
		} else {
			return float64(_left)/retain,nil
		}
	}
	return float64(that)/retain,nil
}





//访问外部字段实现
func accessFieldValue( full map[string]interface{},key string ) (interface{},error) {
	if nil == full[key] {
		return nil,errors.New("full[key] is nil")
	}
	inter,ok := full[key]
	if false == ok {
		return nil,errors.New("cannot load outer field")
	}
	if inter == nil {
		return nil,errors.New("load after interface value is nil")
	}
	return inter,nil
}