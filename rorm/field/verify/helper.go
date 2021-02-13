package verify

import (
	"errors"
	"fmt"
	"github.com/lucidStream/coify/helper"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)





//验证助手对象，这是一个强大的助手，可以省去用户很多繁琐操作
type Helper struct {
	extra map[string]interface{}
	full  map[string]interface{}
	Field string
}

type Uint64 struct {
	Helper
	That  uint64
}

type Int64 struct {
	Helper
	That  int64
}

type String struct {
	Helper
	That  string
}

type Float64 struct {
	Helper
	That  float64
}

type Bool struct {
	Helper
	That  bool
}

type Array struct {
	Helper
	That  []interface{}
}


type Json struct {
	Helper
	That  map[string]interface{}
}




func (this* Helper) SetFullValue( full map[string]interface{} ) {
	this.full = full
}



func (this* Helper) SetExtra( extra map[string]interface{} ) {
	this.extra = extra
}




//获取全部字段值
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




//必填错误
//为空为0不一定就是require，需要自己判断
func (this *Uint64) ErrorRequire() error {
	return errors.New("require parameter: "+this.Field)
}

func (this *Int64) ErrorRequire() error {
	return errors.New("require parameter: "+this.Field)
}

func (this *String) ErrorRequire() error {
	return errors.New("require parameter: "+this.Field)
}

func (this *Float64) ErrorRequire() error {
	return errors.New("require parameter: "+this.Field)
}

func (this *Bool) ErrorRequire() error {
	return errors.New("require parameter: "+this.Field)
}

func (this *Array) ErrorRequire() error {
	return errors.New("require parameter: "+this.Field)
}

func (this *Json) ErrorRequire() error {
	return errors.New("require parameter: "+this.Field)
}




//无效选项错误
func (this *Uint64) MustOptionsIn ( opts []uint64 ) error {
	if true == helper.Uint64ArrayIn(this.That,opts) {
		return nil
	}
	optsLen := len(opts)
	optstr := make([]string,optsLen)
	for i:=0; i<optsLen; i++ {
		optstr = append(optstr,strconv.FormatUint(opts[i],10))
	}
	msg := fmt.Sprintf("%s has invalid option,options is %s",
		this.Field,
		strings.Join(optstr,","),
	)
	return errors.New(msg)
}


func (this *Int64) MustOptionsIn ( opts []int64 ) error {
	if true == helper.Int64ArrayIn(this.That,opts) {
		return nil
	}
	optsLen := len(opts)
	optstr := make([]string,optsLen)
	for i:=0; i<optsLen; i++ {
		optstr = append(optstr,strconv.FormatInt(opts[i],10))
	}
	msg := fmt.Sprintf("%s has invalid option,options is %s",
		this.Field,
		strings.Join(optstr,","),
	)
	return errors.New(msg)
}



//字符需提供拆分值的方法(如果需要)
func (this *String) MustOptionsIn ( opts []string,split func( v *string ) ([]string,error) ) error {
	if split != nil {
		values,err := split(&this.That)
		if nil != err {
			return err
		}
		vLen := len(values)
		if vLen == 0 {
			//说明值就是空，直接把空值到opts里查找
			if false == helper.StringArrayIn("",opts) {
				goto ErrBlock
			}
		}
		for i:=0; i<vLen; i++ {
			if false == helper.StringArrayIn(values[i],opts) {
				goto ErrBlock
			}
		}
		return nil
	} else {
		if false == helper.StringArrayIn(this.That,opts) {
			goto ErrBlock
		}
		return nil
	}
ErrBlock:
	msg := fmt.Sprintf("%s has invalid option,options is %s",
		this.Field,
		strings.Join(opts,","),
	)
	return errors.New(msg)
}



func (this *Float64) MustOptionsIn ( opts []float64,retain float64 ) error {
	if true == helper.Float64ArrayIn(this.That,opts,retain) {
		return nil
	}
	optsLen := len(opts)
	optstr := make([]string,optsLen)
	for i:=0; i<optsLen; i++ {
		optstr = append(optstr,strconv.FormatFloat(opts[i],'f',-1,64))
	}
	msg := fmt.Sprintf("%s has invalid option,options is %s",
		this.Field,
		strings.Join(optstr,","),
	)
	return errors.New(msg)
}




//无效的值
func (this *Uint64) ErrorInvalidValue() error {
	return errors.New("invalid value for field: "+this.Field)
}

func (this *Int64) ErrorInvalidValue() error {
	return errors.New("invalid value for field: "+this.Field)
}

func (this *String) ErrorInvalidValue() error {
	return errors.New("invalid value for field: "+this.Field)
}

func (this *Float64) ErrorInvalidValue() error {
	return errors.New("invalid value for field: "+this.Field)
}




//包前不包后
//超出范围错误，适用于范围值
func (this *Uint64) MustBetween( left uint64,right uint64 ) error {
	if this.That >= left && this.That < right {
		return nil
	}
	msg := fmt.Sprintf("%s outof range,the range is %d to %d",
		this.Field,
		left,
		right,
	)
	return errors.New(msg)
}


func (this *Int64) MustBetween( left int64,right int64 ) error {
	if this.That >= left && this.That < right {
		return nil
	}
	msg := fmt.Sprintf("%s outof range,the range is %d to %d",
		this.Field,
		left,
		right,
	)
	return errors.New(msg)
}


func (this *Float64) MustBetween( left float64,right float64,retain float64 ) error {
	left  = float64(int64(left*retain))/retain
	right = float64(int64(right*retain))/retain
	that := float64(int64(this.That*retain))/retain
	if that >= left && that < right {
		return nil
	}
	msg := fmt.Sprintf("%s outof range,the range is %f to %f",
		this.Field,
		left,
		right,
	)
	return errors.New(msg)
}




//验证值是否为年月日格式
func (this *String) MustYearMonthDay() error {
	matched,err := regexp.MatchString(`^\d+-\d+-\d+$`,this.That)
	if nil != err {
		return err
	}
	if false == matched {
		return errors.New("wrong format for field: "+this.Field)
	}
	return nil
}



//字符长度必须等于x
func (this *String) MustStrLenEqual( _len int ) error {
	if len(this.That) != _len {
		msg := fmt.Sprintf("%s length must: %d",
			this.Field,
			_len,
		)
		return errors.New(msg)
	}
	return nil
}



//必须是完整url
func (this *String) MustUrl() error {
	_,err := url.Parse(this.That)
	if nil != err {
		return err
	}
	return nil
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