package _default

import (
	"errors"
	"strings"
)




var(
	UseDefaultValue = errors.New("User set default value")
)



//默认值助手对象
type Helper struct {
	full      map[string]interface{}
	extra     map[string]interface{}
	Field     string
	Undefind  bool
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


type Array struct{
	Helper
	That []interface{}
}


type Json struct{
	Helper
	That map[string]interface{}
}




func (this* Helper) SetFullValue( full map[string]interface{} ) {
	this.full = full
}



func (this* Helper) SetExtra( extra map[string]interface{} ) {
	this.extra = extra
}



//获取当前字段值
func (this* String) GetTrimValue( s string ) string { return strings.Trim(this.That,s) }




//获取全部值
func (this* Uint64) GetFullValue() map[string]interface{} { return this.full }

func (this* Int64) GetFullValue() map[string]interface{} { return this.full }

func (this* String) GetFullValue() map[string]interface{} { return this.full }

func (this* Float64) GetFullValue() map[string]interface{} { return this.full }

func (this *Bool) GetFullValue() map[string]interface{} { return this.full }

func (this *Array) GetFullValue() map[string]interface{} { return this.full }

func (this *Json) GetFullValue() map[string]interface{} { return this.full }



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

func (this *Bool) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this *Array) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
}

func (this *Json) GetFieldValue( key string ) (interface{},error) {
	return accessFieldValue(this.full,key)
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