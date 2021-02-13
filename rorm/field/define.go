package field

import (
	"errors"
	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	"unsafe"
)


type TypePair [2]interface{}


var(
	Uint64Pair   = TypePair{ParseUint64,FormatUint64}
	Int64Pair    = TypePair{ParseInt64,FormatInt64}
	StringPair   = TypePair{ParseString,FormatString}
	Float64Pair  = TypePair{ParseFloat64,FormatFloat64}
	BoolPair     = TypePair{ParseBool,FormatBool}
	ArrayPair    = TypePair{ParseArray,FormatArray}
	JsonPair     = TypePair{ParseJson,FormatJson}
	XMLPair      = TypePair{}
)





type Setting struct{
	Name           string //字段名
	Type           *TypePair//数据类型指针
	Rorm           interface{}
	Field          *reflect2.StructField
	DataId         string     //数据主键，决定字段值是否在模型中进行实际操作
	Default        interface{}//默认值(nil代表无默认值)
	Filter         interface{}//过滤方法
	Verify         interface{}//验证方法(nil代表无验证)
}






type StringParse    = func(p string) (interface{},error)
type ConverToString = func(p interface{}) (string,error)
type SettingExpr map[string]*Setting





type RormField struct {
	IdFieldName         string
	CreateField         string
	UpdateField         string
	StringParse         map[string]StringParse
	ConverToString      map[string]ConverToString
	TypeExpr            SettingExpr
	FieldList           []string
}





//根据字段设定解析为对象
type myInterface struct { typ uintptr;word unsafe.Pointer }

func (this *SettingExpr) HashTableParse( mmp map[string]interface{},objc interface{} ) error {
	var (
		stringType  string
		ok          bool
		err         error
		exists      bool
		key         string
		value       interface{}
		valInter    *myInterface
		settingExpr *Setting
		field       *reflect2.StructField
	)
	www := (*myInterface)(unsafe.Pointer(&objc))
	for key,value = range mmp {
		settingExpr,exists = (*this)[key]
		if false == exists {
			return errors.New("field not exists")
		}
		field = settingExpr.Field
		switch settingExpr.Type {
		case &JsonPair,&ArrayPair:
			stringType,ok = value.(string)
			if false == ok {
				stringType,err = jsoniter.MarshalToString(value)
				if nil != err {
					return err
				}
			}
			if len(stringType) > 0 {
				//不知道对象具体类型，无法赋空值，只能是nil，后续使用要注意
				//判断结构体成员是否只指针类型
				err = jsoniter.UnmarshalFromString(stringType,(*field).Get(objc))
				if nil != err {
					return err
				}
			}
		case &XMLPair:
		default:
			valInter = (*myInterface)(unsafe.Pointer(&value))
			(*field).UnsafeSet(www.word,valInter.word)
		}
	}
	return nil
}