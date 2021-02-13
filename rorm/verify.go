package rorm

import (
	"errors"
	"fmt"
	_field "github.com/lucidStream/coify/rorm/field"
	_default "github.com/lucidStream/coify/rorm/field/default"
	"github.com/lucidStream/coify/rorm/field/filter"
	"github.com/lucidStream/coify/rorm/field/verify"
)






//一个事务中处理多个表的数据提交应该要先检查数据是否能够正常写入
//而不是开启了事务再写又发生逻辑错误
//多余的字段可能需要剔除
//多余的字段最好是控制器中处理，比如填充一个定义字段的hash表，而不是直接使用json解析结果
func (this *RedisOrm) IntegrityProcess( val,extra map[string]interface{} ) error {
	var (
		typePair *_field.TypePair; err error; interval interface{};
		exists bool; fieldSetting *_field.Setting;
	)
	//各种用户函数调用后的新值定义
	var (
		uint64New uint64;int64New int64; stringNew string;float64New float64;
		boolNew bool; arrayNew []interface{}; jsonNew interface{}; fieldName string
	)
	//验证助手定义
	var (
		uint64VHelper verify.Uint64; int64VHelper verify.Int64
		stringVHelper verify.String; float64VHelper verify.Float64
		boolVHelper verify.Bool; arrayVHelper verify.Array; jsonVHelper verify.Json
	)
	//默认值助手定义
	var (
		uint64DHelper _default.Uint64;int64DHelper _default.Int64;
		stringDHelper _default.String;float64DHelper _default.Float64
		boolDHelper _default.Bool; arrayDHelper _default.Array; jsonDHelper _default.Json
	)
	//过滤助手定义
	var (
		uint64FHelper filter.Uint64; int64FHelper filter.Int64;
		stringFHelper filter.String; float64FHelper filter.Float64
		arrayFHelper  filter.Array ; jsonFHelper  filter.Json
	)



	fieldListLen := len(this.FieldList)
	//第一个循环填充默认值
	for i:=0; i<fieldListLen; i++ {
		fieldName       = this.FieldList[i]
		interval,exists = val[fieldName]
		fieldSetting    = this.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		typePair = fieldSetting.Type
		if fieldSetting.Default == nil { continue }
		switch typePair {
		case &_field.Uint64Pair:
			uint64DHelper.Field = fieldName
			uint64DHelper.SetFullValue(val)
			uint64DHelper.SetExtra(extra)
			//字段值==nil说明该字段也是需要填充默认值
			if false == exists || interval == nil {
				uint64DHelper.Undefind = true
				uint64DHelper.That     = 0
			} else {
				uint64DHelper.Undefind = false
				uint64DHelper.That     = interval.(uint64)
			}
			uint64New,err = fieldSetting.Default.(_field.Uint64DefaultVal)(uint64DHelper)
			if nil != err {
				if err == _default.UseDefaultValue { val[fieldName] = uint64New } else {
					return err
				}
			}
		case &_field.Int64Pair:
			int64DHelper.Field = fieldName
			int64DHelper.SetFullValue(val)
			int64DHelper.SetExtra(extra)
			if false == exists || interval == nil {
				int64DHelper.Undefind = true
				int64DHelper.That     = 0
			} else {
				int64DHelper.Undefind = false
				int64DHelper.That     = interval.(int64)
			}
			int64New,err = fieldSetting.Default.(_field.Int64DefaultVal)(int64DHelper)
			if nil != err {
				if err == _default.UseDefaultValue { val[fieldName] = int64New } else {
					return err
				}
			}
		case &_field.StringPair:
			stringDHelper.Field = fieldName
			stringDHelper.SetFullValue(val)
			stringDHelper.SetExtra(extra)
			if false == exists || interval == nil {
				stringDHelper.Undefind = true
				stringDHelper.That     = ""
			} else {
				stringDHelper.Undefind = false
				stringDHelper.That     = interval.(string)
			}
			stringNew,err = fieldSetting.Default.(_field.StringDefaultVal)(stringDHelper)
			if nil != err {
				if err == _default.UseDefaultValue { val[fieldName] = stringNew } else {
					return err
				}
			}
		case &_field.Float64Pair:
			float64DHelper.Field = fieldName
			float64DHelper.SetFullValue(val)
			float64DHelper.SetExtra(extra)
			if false == exists || interval == nil {
				float64DHelper.Undefind = true
				float64DHelper.That     = 0
			} else {
				float64DHelper.Undefind = false
				float64DHelper.That     = interval.(float64)
			}
			float64New,err = fieldSetting.Default.(_field.Float64DefaultVal)(float64DHelper)
			if nil != err {
				if err == _default.UseDefaultValue { val[fieldName] = float64New } else {
					return err
				}
			}
		case &_field.BoolPair:
			boolDHelper.Field = fieldName
			boolDHelper.SetFullValue(val)
			boolDHelper.SetExtra(extra)
			if false == exists || interval == nil {
				boolDHelper.Undefind = true
				boolDHelper.That     = false
			} else {
				boolDHelper.Undefind = false
				boolDHelper.That = interval.(bool)
			}
			boolNew,err = fieldSetting.Default.(_field.BoolDefaultVal)(boolDHelper)
			if nil != err {
				if err == _default.UseDefaultValue { val[fieldName] = boolNew } else {
					return err
				}
			}
		case &_field.ArrayPair:
			//先调用本模型填充默认值（如果需要）
			arrayDHelper.Field = fieldName
			arrayDHelper.SetFullValue(val)
			arrayDHelper.SetExtra(extra)
			if false == exists || interval == nil {
				arrayDHelper.Undefind = true
				arrayDHelper.That     = nil
			} else {
				arrayDHelper.Undefind = false
				arrayDHelper.That = interval.([]interface{})
			}
			arrayNew,err = fieldSetting.Default.(_field.ArrayDefaultVal)(arrayDHelper)
			if nil != err {
				if err == _default.UseDefaultValue { val[fieldName] = arrayNew } else {
					return err
				}
			}
		case &_field.JsonPair:
			jsonDHelper.Field = fieldName
			jsonDHelper.SetFullValue(val)
			jsonDHelper.SetExtra(extra)
			if false == exists || interval == nil {
				jsonDHelper.Undefind = true
				jsonDHelper.That  = nil
			} else {
				jsonDHelper.Undefind = false
				jsonDHelper.That  = interval.(map[string]interface{})
			}
			jsonNew,err = fieldSetting.Default.(_field.JsonDefaultVal)(jsonDHelper)
			if nil != err {
				if err == _default.UseDefaultValue { val[fieldName] = jsonNew } else {
					return err
				}
			}
		default:
			return errors.New("unknown field save type")
		}
	}


	//默认值填充好后，寻找依赖其他模型的字段，调用其他模型
	//处理array字段
	arrayFormatsLen := len(this.arrayFormats)
	for i:=0; i<arrayFormatsLen; i++ {
		fieldName       = this.arrayFormats[i]
		interval,exists = val[fieldName]
		if !exists {
			errMsg := fmt.Sprintf("field %s not exists in value",fieldName)
			return errors.New(errMsg)
		}
		fieldSetting    = this.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		//字段处理模型
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if ok && nil!=fieldOrm {
			interArray := interval.([]interface{})
			arrLen := len(interArray)
			for xxt:=0; xxt<arrLen; xxt++ {
				//调用模型处理
				xxtVal := interArray[xxt].(map[string]interface{})
				if nil != xxtVal {
					ormVal,err := fieldOrm.NewRormValue(xxtVal)
					if nil != err {
						return err
					}
					xxtVal = ormVal.Dict
					err = fieldOrm.IntegrityProcess(xxtVal,val)
					if nil != err {
						return err
					}
				}
				interArray[xxt] = xxtVal
			}
			val[fieldName] = interArray
		}
		//基本数据类型交给用户处理
	}

	//处理json字段
	jsonFormatsLen := len(this.jsonFormats)
	for i:=0; i<jsonFormatsLen; i++ {
		fieldName       = this.jsonFormats[i]
		interval,exists = val[fieldName]
		if !exists {
			errMsg := fmt.Sprintf("field %s not exists in value",fieldName)
			return errors.New(errMsg)
		}
		fieldSetting    = this.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		//字段处理模型
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if !ok {
			//json必须定义好对象的模型来处理
			errMsg := fmt.Sprintf("field %s must defined ormModel",fieldName)
			panic(errMsg)
		}
		xxtVal := interval.(map[string]interface{})
		if nil != xxtVal {
			//空值不进行模型调用验证
			ormVal,err := fieldOrm.NewRormValue(xxtVal)
			if nil != err {
				return err
			}
			xxtVal = ormVal.Dict
			err = fieldOrm.IntegrityProcess(xxtVal,val)
			if nil != err {
				return err
			}
		}
		val[fieldName] = xxtVal
	}



	//第二个循环过滤
	for i:=0; i<fieldListLen; i++ {
		fieldName       = this.FieldList[i]
		//该填默认值填了后，在检查是否已定义
		interval,exists = val[fieldName]
		if false == exists {
			errMsg := fmt.Sprintf("undefined field: %s",fieldName)
			return errors.New(errMsg)
		}
		if interval == nil {
			errMsg := fmt.Sprintf("field %s value is nil",fieldName)
			return errors.New(errMsg)
		}
		fieldSetting = this.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission field setting",fieldName)
			panic(errMsg)
		}
		typePair = fieldSetting.Type
		if fieldSetting.Filter == nil { continue }
		switch typePair {
		case &_field.Uint64Pair:
			uint64FHelper.Field = fieldName
			uint64FHelper.SetFullValue(val)
			uint64FHelper.SetExtra(extra)
			uint64FHelper.That  = interval.(uint64)
			uint64New,err = fieldSetting.Filter.(_field.Uint64FilterVal)(uint64FHelper)
			if nil != err {
				return err
			}
			val[fieldName] = uint64New
		case &_field.Int64Pair:
			int64FHelper.Field = fieldName
			int64FHelper.SetFullValue(val)
			int64FHelper.SetExtra(extra)
			int64FHelper.That  = interval.(int64)
			int64New,err = fieldSetting.Filter.(_field.Int64FilterVal)(int64FHelper)
			if nil != err {
				return err
			}
			val[fieldName] = int64New
		case &_field.StringPair:
			stringFHelper.Field = fieldName
			stringFHelper.SetFullValue(val)
			stringFHelper.SetExtra(extra)
			stringFHelper.That  = interval.(string)
			stringNew,err = fieldSetting.Filter.(_field.StringFilterVal)(stringFHelper)
			if nil != err {
				return err
			}
			val[fieldName] = stringNew
		case &_field.Float64Pair:
			float64FHelper.Field = fieldName
			float64FHelper.SetFullValue(val)
			float64FHelper.SetExtra(extra)
			float64FHelper.That  = interval.(float64)
			float64New,err = fieldSetting.Filter.(_field.Float64FilterVal)(float64FHelper)
			if nil != err {
				return err
			}
			val[fieldName] = float64New
		case &_field.ArrayPair:
			arrayFHelper.Field = fieldName
			arrayFHelper.SetFullValue(val)
			arrayFHelper.SetExtra(extra)
			arrayFHelper.That = interval.([]interface{})
			arrayNew,err = fieldSetting.Filter.(_field.ArrayFilterVal)(arrayFHelper)
			if nil != err {
				return err
			}
			val[fieldName] = arrayNew
		case &_field.JsonPair:
			jsonFHelper.Field = fieldName
			jsonFHelper.SetFullValue(val)
			jsonFHelper.SetExtra(extra)
			jsonFHelper.That = interval.(map[string]interface{})
			jsonNew,err = fieldSetting.Filter.(_field.JsonFilterVal)(jsonFHelper)
			if nil != err {
				return err
			}
			val[fieldName] = jsonNew
		default:
			return errors.New("unknown field save type")
		}
	}



	//填充了默认值，并且过滤后，再验证
	for i:=0; i<fieldListLen; i++ {
		fieldName       = this.FieldList[i]
		interval,exists = val[fieldName]
		if false == exists {
			errMsg := fmt.Sprintf("undefined field: %s",fieldName)
			return errors.New(errMsg)
		}
		if interval == nil {
			errMsg := fmt.Sprintf("field %s value is nil",fieldName)
			return errors.New(errMsg)
		}
		fieldSetting = this.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission field setting",fieldName)
			panic(errMsg)
		}
		typePair = fieldSetting.Type
		if fieldSetting.Verify == nil { continue }
		switch typePair {
		case &_field.Uint64Pair:
			uint64VHelper.Field = fieldName
			uint64VHelper.SetFullValue(val)
			uint64VHelper.SetExtra(extra)
			uint64VHelper.That  = interval.(uint64)
			err = fieldSetting.Verify.(_field.Uint64Validation)(uint64VHelper)
			if nil != err {
				return err
			}
		case &_field.Int64Pair:
			int64VHelper.Field = fieldName
			int64VHelper.SetFullValue(val)
			int64VHelper.SetExtra(extra)
			int64VHelper.That  = interval.(int64)
			err = fieldSetting.Verify.(_field.Int64Validation)(int64VHelper)
			if nil != err {
				return err
			}
		case &_field.StringPair:
			stringVHelper.Field  = fieldName
			stringVHelper.SetFullValue(val)
			stringVHelper.SetExtra(extra)
			stringVHelper.That   = interval.(string)
			err = fieldSetting.Verify.(_field.StringValidation)(stringVHelper)
			if nil != err {
				return err
			}
		case &_field.Float64Pair:
			float64VHelper.Field = fieldName
			float64VHelper.SetFullValue(val)
			float64VHelper.SetExtra(extra)
			float64VHelper.That  = interval.(float64)
			err = fieldSetting.Verify.(_field.Float64Validation)(float64VHelper)
			if nil != err {
				return err
			}
		case &_field.BoolPair:
			boolVHelper.Field = fieldName
			boolVHelper.SetFullValue(val)
			boolVHelper.SetExtra(extra)
			boolVHelper.That = interval.(bool)
			err = fieldSetting.Verify.(_field.BoolValidation)(boolVHelper)
			if nil != err {
				return err
			}
		case &_field.ArrayPair:
			arrayVHelper.Field = fieldName
			arrayVHelper.SetFullValue(val)
			arrayVHelper.SetExtra(extra)
			arrayVHelper.That = interval.([]interface{})
			err = fieldSetting.Verify.(_field.ArrayValidation)(arrayVHelper)
			if nil != err {
				return err
			}
		case &_field.JsonPair:
			jsonVHelper.Field = fieldName
			jsonVHelper.SetFullValue(val)
			jsonVHelper.SetExtra(extra)
			jsonVHelper.That = interval.(map[string]interface{})
			err = fieldSetting.Verify.(_field.JsonValidation)(jsonVHelper)
			if nil != err {
				return err
			}
		default:
			return errors.New("unknown field save type")
		}
	}
	return nil
}
