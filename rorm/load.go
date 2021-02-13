package rorm

import (
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	_field "github.com/lucidStream/coify/rorm/field"
	"strconv"
	"strings"
	"time"
)









//加载旧值 用于填充未提供的字段值
//这里不能直接加载其他模型的数据，因为对其他模型数据的加锁操作还未完成
func (this *WriteOrmWithClient) getBodyDict( key *KwPatter ) (map[string]interface{},error) {
	var (
		errMsg     string
		fieldName  string
		valString  string
		typeOk     bool
		formated = string(key.Formart())
	)
	body,err := this.client.HMGet(formated,this.model.FieldList...).Result()
	if nil != err {
		return nil,err
	}
	val := make(map[string]interface{},len(body))
	fieldListLen := len(this.model.FieldList)
	for i:=0; i<fieldListLen; i++ {
		fieldName = this.model.FieldList[i]
		valString,typeOk = body[i].(string)
		if false == typeOk {
			errMsg = fmt.Sprintf("%s type not string",fieldName)
			return nil,errors.New(errMsg)
		}
		fieldSetting := this.model.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		if ok && nil!=fieldOrm && fieldUsingDataId != "" {
			switch fieldSetting.Type {
			case &_field.ArrayPair:
				var idsArray PrimaryKeys
				err = jsoniter.UnmarshalFromString(valString,&idsArray)
				if nil != err {
					return nil,err
				}
				for il:=0; il<len(idsArray); il++ {
					if nil != idsArray[il] {
						idsArray[il] = PrimaryKey(idsArray[il].(float64))
					} else {
						idsArray[il] = NullValue(0)
					}
				}
				val[fieldName] = idsArray
			case &_field.JsonPair,&_field.XMLPair:
				var primaryKey uint64
				if valString != "null" {
					primaryKey,err = strconv.ParseUint(valString,10,64)
					if nil != err {
						return nil,err
					}
					val[fieldName] = PrimaryKey(primaryKey)
				} else {
					primaryKey = 0
					val[fieldName] = NullValue(primaryKey)
				}
			default:
				errMsg := fmt.Sprintf("field %s noSupport reletion",fieldName)
				return nil,errors.New(errMsg)
			}
		} else {
			val[fieldName],err = this.model.StringParse[fieldName](valString)
			if nil != err {
				return nil,err
			}
		}
	}
	return val,err
}












//使用旧字段值填充新值
func (this *RormWritePlain) fillNewValue( oldval,newval map[string]interface{} ) error {
	//在填充不存在的值之前，旧值的数据类型要确定好
	var (
		newElement    interface{}
		oldElement    interface{}
		fieldName     string
		exists        bool
	)
	fieldListLen := len(this.ormwith.model.FieldList)
	for i:=0; i<fieldListLen; i++ {
		fieldName = this.ormwith.model.FieldList[i]
		newElement,exists = newval[fieldName]
		if ! exists {
			oldElement,exists = oldval[fieldName]
			if !exists {
				errMsg := fmt.Sprintf("oldval not exists key: %s",fieldName)
				return errors.New(errMsg)
			}
			newval[fieldName] = oldElement
		}
		//为容器类型填充ID
		fieldSetting := this.ormwith.model.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if !ok || nil==fieldOrm || fieldUsingDataId=="" {
			continue
		}
		//需要从数据库中查询出的对象有ID字段才能进行填充
		oldElement,exists = oldval[fieldName]
		if !exists {
			errMsg := fmt.Sprintf("oldval not exists key: %s",fieldName)
			return errors.New(errMsg)
		}
		switch fieldSetting.Type {
		case &_field.ArrayPair:
			//不指定ID的情况下数组类型的更新操作应该保证顺序与数据库中一致
			//判断值是否需要填充主键
			interArray,ok := newElement.([]interface{})
			if !ok {
				//传入的字段值不是有效的数组
				break
			}
			naolArray,ok := oldElement.(PrimaryKeys)
			if !ok {
				//数据库查询返回的对象不是有效的主键集合
				break
			}
			naolLen  := len(naolArray)
			interLen := len(interArray)
			var usingLen int
			if interLen < naolLen {
				usingLen = interLen
				//删除多余的元素
				ormBindClient := NewWriteOrmWithClient(fieldOrm,this.ormwith.client)
				splain,err := ormBindClient.SyncPlain(this.splain.GetDispatch(),10*time.Second)
				if nil != err {
					return err
				}
				for il:=interLen; il<naolLen; il++ {
					var dataId uint64
					switch naolArray[il].(type) {
					case uint64:
						dataId = naolArray[il].(uint64)
					case PrimaryKey:
						dataId = uint64(naolArray[il].(PrimaryKey))
					case NewPrimaryKey:
						dataId = uint64(naolArray[il].(NewPrimaryKey))
					default:
						return errors.New("unknown primaryKey type")
					}
					err = splain.doDelete([]interface{}{dataId})
					if nil != err {
						return err
					}
				}
			} else {
				usingLen = naolLen
				//新增的元素不允许带主键字段
				for il:=naolLen; il<interLen; il++ {
					xxtVal,ok := interArray[il].(map[string]interface{})
					if ok {
						delete(xxtVal,fieldOrm.IdFieldName)
					}
				}
			}
			for il:=0; il<usingLen; il++ {
				xxtVal,ok := interArray[il].(map[string]interface{})
				if !ok {
					//传入的数组元素不是有效的对象
					break
				}
				//强制覆盖主键字段值
				primaryKey,ok := naolArray[il].(PrimaryKey)
				if ok {
					xxtVal[fieldOrm.IdFieldName] = uint64(primaryKey)
				}
			}
			newval[fieldName] = interArray
		case &_field.JsonPair,&_field.XMLPair:
			xxtVal,ok := newElement.(map[string]interface{})
			if !ok {
				//传入的字段值不是有效的对象
				break
			}
			naolVal,ok := oldElement.(PrimaryKey)
			if !ok {
				//数据库查询返回的对象不是有效的主键
				break
			}
			//强制覆盖主键
			xxtVal[fieldOrm.IdFieldName] = uint64(naolVal)
			newval[fieldName] = xxtVal
		default:
		}
	}
	return nil
}





