package field

import (
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	_default "github.com/lucidStream/coify/rorm/field/default"
	"github.com/lucidStream/coify/rorm/field/filter"
	"github.com/lucidStream/coify/rorm/field/verify"
	"strconv"
	"time"
	"unsafe"
)




//根据传入val决定是否需要填充默认值，如果需要，返回填充值
//val==nil说明字段未定义 !=nil需要判断该字段为该指针指向值时是否需要填充默认值
type Uint64DefaultVal  = func( helper _default.Uint64 ) (uint64,error)
type Int64DefaultVal   = func( helper _default.Int64 ) (int64,error)
type StringDefaultVal  = func( helper _default.String ) (string,error)
type Float64DefaultVal = func( helper _default.Float64 ) (float64,error)
type BoolDefaultVal    = func( helper _default.Bool ) (bool,error)
type ArrayDefaultVal   = func( helper _default.Array ) ([]interface{},error)
type JsonDefaultVal    = func( helper _default.Json ) (map[string]interface{},error)


//字段过滤方法，只有string需要过滤，数值类型验证值是否合法即可
type StringFilterVal  = func( helper filter.String ) (string,error)
type Uint64FilterVal  = func( helper filter.Uint64 ) (uint64,error)
type Int64FilterVal   = func( helper filter.Int64 ) (int64,error)
//常见在此方法实现中确定数值精度
//orm模型并不负责数值的计算，也不负责浮点数的逻辑比较
//因此float数据类型的使用需用户知晓风险
type Float64FilterVal = func( helper filter.Float64 ) (float64,error)
type ArrayFilterVal   = func( helper filter.Array ) ([]interface{},error)
type JsonFilterVal    = func( helper filter.Json ) (map[string]interface{},error)



//字段值验证方法，验证是否合法
type Uint64Validation  = func( helper verify.Uint64 ) error
type Int64Validation   = func( helper verify.Int64 ) error
type StringValidation  = func( helper verify.String ) error
type Float64Validation = func( helper verify.Float64 ) error
type BoolValidation    = func( helper verify.Bool ) error
type ArrayValidation   = func( helper verify.Array ) error
type JsonValidation    = func( helper verify.Json ) error






//parse
func ParseString( p string ) (i interface{}, e error) {
	return p,nil
}


func ParseUint64( p string ) (i interface{}, e error) {
	return strconv.ParseUint(p,10,64)
}


func ParseInt64( p string ) (i interface{}, e error) {
	return strconv.ParseInt(p,10,64)
}


func ParseFloat64( p string ) (i interface{}, e error) {
	return strconv.ParseFloat(p,10)
}


func ParseBool( p string ) (i interface{},e error) {
	switch p {
	case "yes":
		return true,nil
	case "no":
		return false,nil
	case "true":
		return true,nil
	case "false":
		return false,nil
	case "0":
		return false,nil
	case "1":
		return true,nil
	default:
		return nil,errors.New("Unrecognized bool value")
	}
}





func ParseArray( p string ) (i interface{},e error) {
	var result []interface{}
	err := jsoniter.UnmarshalFromString(p,&result)
	return result,err
}





func ParseJson( p string ) (i interface{},e error) {
	result := make(map[string]interface{},32)
	err := jsoniter.UnmarshalFromString(p,&result)
	return result,err
}









//format
func FormatUint64( p interface{} ) (s string, e error) {
	//各种数据类型转uint64后再format
	var err error
	switch p.(type) {
	case uint64:
		p = uint64(p.(uint64))
	case int64:
		p = uint64(p.(int64))
	case uint32:
		p = uint64(p.(uint32))
	case int32:
		p = uint64(p.(int32))
	case int:
		p = uint64(p.(int))
	case uint16:
		p = uint64(p.(uint16))
	case int16:
		p = uint64(p.(int16))
	case uint8:
		p = uint64(p.(uint8))
	case int8:
		p = uint64(p.(int8))
	case float32:
		p = uint64(p.(float32))
	case float64:
		p = uint64(p.(float64))
	case string:
		p,err = strconv.ParseUint(p.(string),10,64)
		if nil != err {
			return "",err
		}
	case bool:
		if p.(bool) {
			p = uint64(1)
		} else {
			p = uint64(0)
		}
	default:
		return "",errors.New("cannot format to uint64")
	}
	return strconv.FormatUint(p.(uint64),10),nil
}





func FormatInt64( p interface{} ) (s string, e error) {
	//各种数据类型转int64后再format
	var err error
	switch p.(type) {
	case uint64:
		p = int64(p.(uint64))
	case int64:
		p = int64(p.(int64))
	case uint32:
		p = int64(p.(uint32))
	case int32:
		p = int64(p.(int32))
	case int:
		p = int64(p.(int))
	case uint16:
		p = int64(p.(uint16))
	case int16:
		p = int64(p.(int16))
	case uint8:
		p = int64(p.(uint8))
	case int8:
		p = int64(p.(int8))
	case float32:
		p = int64(p.(float32))
	case float64:
		p = int64(p.(float64))
	case string:
		p,err = strconv.ParseInt(p.(string),10,64)
		if nil != err {
			return "",err
		}
	case bool:
		if p.(bool) {
			p = int64(1)
		} else {
			p = int64(0)
		}
	default:
		return "",errors.New("cannot format to int64")
	}
	return strconv.FormatInt(p.(int64),10),nil
}





func FormatString( p interface{} ) (s string, e error) {
	switch p.(type) {
	case uint64:
		return strconv.FormatUint(p.(uint64),10),nil
	case int64:
		return strconv.FormatInt(p.(int64),10),nil
	case uint32:
		return strconv.FormatUint(uint64(p.(uint32)),10),nil
	case int32:
		return strconv.FormatInt(int64(p.(int32)),10),nil
	case int:
		return strconv.Itoa(p.(int)),nil
	case uint16:
		return strconv.FormatUint(uint64(p.(uint16)),10),nil
	case int16:
		return strconv.FormatInt(int64(p.(int16)),10),nil
	case uint8:
		return strconv.FormatUint(uint64(p.(uint8)),10),nil
	case int8:
		return strconv.FormatInt(int64(p.(int8)),10),nil
	case float32:
		return strconv.FormatFloat(float64(p.(float32)),'f',-1,64),nil
	case float64:
		return strconv.FormatFloat(p.(float64),'f',-1,64),nil
	case string:
		return p.(string),nil
	case bool:
		if p.(bool) {
			return "1",nil
		}
		return "0",nil
	default:
		return "",errors.New("cannot format to string")
	}
}





func FormatFloat64( p interface{} ) (s string, e error) {
	//各种基本数据类型转float64后再format
	var err error
	switch p.(type) {
	case uint64:
		p = float64(p.(uint64))
	case int64:
		p = float64(p.(int64))
	case uint32:
		p = float64(p.(uint32))
	case int32:
		p = float64(p.(int32))
	case int:
		p = float64(p.(int))
	case uint16:
		p = float64(p.(uint16))
	case int16:
		p = float64(p.(int16))
	case uint8:
		p = float64(p.(uint8))
	case int8:
		p = float64(p.(int8))
	case float32:
		p = float64(p.(float32))
	case float64:
		p = float64(p.(float64))
	case string:
		p,err = strconv.ParseFloat(p.(string),64)
		if nil != err {
			return "",err
		}
	case bool:
		if p.(bool) {
			p = float64(1)
		} else {
			p = float64(0)
		}
	default:
		return "",errors.New("cannot format to float64")
	}
	return strconv.FormatFloat(p.(float64),'f',-1,64),nil
}






func FormatBool( p interface{} ) (s string, e error) {
	//各种基本数据类型转uint64后再format
	switch p.(type) {
	case uint64:
		p = uint64(p.(uint64)&0x1)
	case int64:
		p = uint64(p.(int64)&0x1)
	case uint32:
		p = uint64(p.(uint32)&0x1)
	case int32:
		p = uint64(p.(int32)&0x1)
	case int:
		p = uint64(p.(int)&0x1)
	case uint16:
		p = uint64(p.(uint16)&0x1)
	case int16:
		p = uint64(p.(int16)&0x1)
	case uint8:
		p = uint64(p.(uint8)&0x1)
	case int8:
		p = uint64(p.(int8)&0x1)
	case float32:
		p = uint64(p.(float32))&0x1
	case float64:
		p = uint64(p.(float64))&0x1
	case string:
		switch p.(string) {
		case "yes":
			p = uint64(1)
		case "no":
			p = uint64(0)
		case "true":
			p = uint64(1)
		case "false":
			p = uint64(0)
		case "0":
			p = uint64(0)
		case "1":
			p = uint64(1)
		default:
			return "",errors.New("Unrecognized bool value")
		}
	case bool:
		if p.(bool) {
			p = uint64(1)
		} else {
			p = uint64(0)
		}
	default:
		return "",errors.New("cannot format to bool")
	}
	if p.(uint64) == 1 {
		return "1",nil
	}
	return "0",nil
}





func FormatArray( p interface{} ) (s string, e error) {
	pString,ok := p.(string)
	if ok {
		//如果传入string则认为是已经序列化的
		return pString,nil
	}
	return jsoniter.MarshalToString(p)
}




func FormatJson( p interface{} ) (s string, e error) {
	pString,ok := p.(string)
	if ok {
		//如果传入string则认为是已经序列化的
		return pString,nil
	}
	interface_obj := (*struct{ Type uintptr ; Data uintptr })(unsafe.Pointer(&p))
	if interface_obj.Data == 0 {
		return "null",nil
	}
	return jsoniter.MarshalToString(p)
}








//查询结果类型转换
func (this *RormField) HInterfaceConverToHString( from map[string]interface{},to map[string]string ) error {
	var (
		err       error
		ok        bool
		k         string
		v         interface{}
		errMsg    string
		convFunc ConverToString
	)
	for k,v = range from {
		convFunc,ok = this.ConverToString[k]
		if !ok {
			errMsg = fmt.Sprintf("does exists conver func: %s",k)
			return errors.New(errMsg)
		}
		to[k],err = convFunc(v)
		if nil != err {
			errMsg = fmt.Sprintf("%s conver err: %s",k,err.Error())
			return errors.New(errMsg)
		}
	}
	return nil
}






type RormValue struct{
	Dict map[string]interface{}
	ormField *RormField
}




func (this *RormValue) SetKeyValue( key string,val interface{} ) {
	this.Dict[key] = val
}



func (this *RormValue) SetPrimaryKey( id uint64 ) {
	this.Dict[this.ormField.IdFieldName] = id
}



func (this *RormValue) SetCreateTime() {
	this.Dict[this.ormField.CreateField] = float64(time.Now().Unix())
	this.Dict[this.ormField.UpdateField] = float64(0)
}



func (this *RormValue) SetUpdateTime() {
	this.Dict[this.ormField.UpdateField] = float64(time.Now().Unix())
	delete(this.Dict,this.ormField.CreateField)
}


func (this *RormValue) GetPrimaryKey() (uint64,error) {
	id,ok := this.Dict[this.ormField.IdFieldName]
	if !ok {
		errMsg := fmt.Sprintf("undefined primaryKey: %s",this.ormField.IdFieldName)
		return 0,errors.New(errMsg)
	}
	ui64,ok := id.(uint64)
	if !ok {
		return 0,errors.New("bad primaryKey type")
	}
	return ui64,nil
}








func (this *RormField) NewRormValue( val map[string]interface{} ) (*RormValue,error) {
	toString := make(map[string]string,len(val))
	err := this.HInterfaceConverToHString(val,toString)
	if nil != err {
		return nil,err
	}
	//转回interface
	var (
		ok       bool
		errMsg   string
		k,v      string
		convFunc StringParse
	)
	result := new(RormValue)
	result.ormField = this
	result.Dict = make(map[string]interface{},len(toString))
	for k,v = range toString {
		convFunc,ok = this.StringParse[k]
		if !ok {
			errMsg = fmt.Sprintf("does exists conver func: %s",k)
			return nil,errors.New(errMsg)
		}
		result.Dict[k],err = convFunc(v)
		if nil != err {
			errMsg = fmt.Sprintf("%s conver err: %s",k,err.Error())
			return nil,errors.New(errMsg)
		}
	}
	return result,nil
}




