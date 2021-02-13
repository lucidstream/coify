package rorm

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
	coifyRedis "github.com/lucidStream/coify/redis"
	_field "github.com/lucidStream/coify/rorm/field"
	"strconv"
	"strings"
	"time"
)


//辅助创建同步对象
//尽量避免业务中用错client
type RormWritePlain struct{
	splain   coifyRedis.SyncPlain
	ormwith  *WriteOrmWithClient
}



//创建绑定orm的同步对象
func (this *WriteOrmWithClient) SyncPlain( dispatch *coifyRedis.SyncCoifyRedis,timeout time.Duration ) (
	splain RormWritePlain,err error) {
	plain,err := dispatch.SyncPlain(timeout)
	if nil != err {
		return splain,err
	}
	splain.splain  = plain
	splain.ormwith = this
	return splain,nil
}














///设置关联模型的值补全结构
func (this *WriteOrmWithClient) converReletion( val map[string]interface{} ) error {
	arrayFormatsLen := len(this.model.arrayFormats)
	for i:=0; i<arrayFormatsLen; i++ {
		fieldName := this.model.arrayFormats[i]
		fieldSetting := this.model.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if ok && nil!=fieldOrm && fieldUsingDataId != "" {
			ormBindClient := NewReadOrmWithClient(fieldOrm,this.client)
			switch fieldSetting.Type {
			case &_field.ArrayPair:
				//这里需要断言为数组，在根据数组元素判断应该怎么获得主键
				interVal,ok := val[fieldName].([]interface{})
				if !ok {
					//该字段值不是有效集合
					break
				}
				var primaryKey uint64
				for il:=0; il<len(interVal); il++ {
					switch interVal[il].(type) {
					case PrimaryKey:
						primaryKey = uint64(interVal[il].(PrimaryKey))
					case map[string]interface{}:
						xxtVal := interVal[il].(map[string]interface{})
						_primaryKey,ok := xxtVal[fieldOrm.IdFieldName]
						if !ok {
							return errors.New("fieldValue does has primaryKey")
						}
						switch _primaryKey.(type) {
						case NewPrimaryKey:
							primaryKey = uint64(_primaryKey.(NewPrimaryKey))
						case uint64:
							primaryKey = uint64(_primaryKey.(uint64))
						default:
							return errors.New("invalid primaryKey type")
						}
					}
					if primaryKey <= 0 {
						return errors.New("cannot get primaryKey")
					}
					info,err := ormBindClient.GetBody([]interface{}{primaryKey},nil)
					if nil != err {
						//不一定能获取到，比如新增的元素
						if err == ErrBodyNotExists {
							continue
						} else {
							return err
						}
					}
					switch interVal[il].(type) {
					case PrimaryKey:
						interVal[il] = info
					case map[string]interface{}:
						xxtVal := interVal[il].(map[string]interface{})
						//只覆盖不存在的字段（ID字段除外）
						for k,v := range info {
							if _,ok=xxtVal[k]; !ok {
								xxtVal[k] = v
							}
						}
						//强制覆盖主键
						xxtVal[fieldOrm.IdFieldName] = info[fieldOrm.IdFieldName]
						interVal[il] = xxtVal
					}
				}
				val[fieldName] = interVal
			case &_field.JsonPair,&_field.XMLPair:
				var xxtVal map[string]interface{}
				var primaryKey uint64
				fieldVal,ok := val[fieldName]
				if !ok {
					errMsg := fmt.Sprintf("field not exists: %s",fieldName)
					return errors.New(errMsg)
				}
				switch fieldVal.(type) {
				case PrimaryKey:
					primaryKey = uint64(fieldVal.(PrimaryKey))
				case map[string]interface{}:
					xxtVal = fieldVal.(map[string]interface{})
					_primaryKey,ok := xxtVal[fieldOrm.IdFieldName]
					if !ok {
						return errors.New("fieldValue does has primaryKey")
					}
					switch _primaryKey.(type) {
					case NewPrimaryKey:
						primaryKey = uint64(_primaryKey.(NewPrimaryKey))
					case uint64:
						primaryKey = uint64(_primaryKey.(uint64))
					default:
						return errors.New("invalid primaryKey type")
					}
				default:
					return errors.New("invalid primaryKey type")
				}
				if primaryKey <= 0 {
					return errors.New("cannot get primaryKey")
				}
				info,err := ormBindClient.GetBody([]interface{}{primaryKey},nil)
				if nil != err {
					//不一定能获取到，比如原本是nil，更新为非nil值
					if err == ErrBodyNotExists {
						continue
					} else {
						return err
					}
				}
				switch fieldVal.(type) {
				case PrimaryKey:
					val[fieldName] = info
				case map[string]interface{}:
					xxtVal = fieldVal.(map[string]interface{})
					for k,v := range info {
						if _,ok=xxtVal[k]; !ok {
							xxtVal[k] = v
						}
					}
					//强制覆盖主键
					xxtVal[fieldOrm.IdFieldName] = info[fieldOrm.IdFieldName]
					val[fieldName] = xxtVal
				}
			default:
			}
		}
	}
	return nil
}










//序列化的同时干掉模型中不存在的字段
func (this *WriteOrmWithClient) serializeReletionVal( val map[string]interface{} ) (map[string]interface{},error) {
	//将容器类型序列化
	var serializeVal map[string]interface{}
	arrayFormatsLen := len(this.model.arrayFormats)
	jsonFormatsLen := len(this.model.jsonFormats)
	if arrayFormatsLen > 0 || jsonFormatsLen > 0 {
		//存在容器类型数据，将原数据拷贝到新字典
		serializeVal = make(map[string]interface{},len(val))
		fieldLen := len(this.model.FieldList)
		for i:=0; i<fieldLen; i++ {
			fieldName := this.model.FieldList[i]
			serializeVal[fieldName] = val[fieldName]
		}
	} else {
		//没有容器类型数据，直接返回
		serializeVal = val
		return serializeVal,nil
	}

	for i:=0; i<arrayFormatsLen; i++ {
		fieldName := this.model.arrayFormats[i]
		_primaryKeys,ok := serializeVal[fieldName]
		if !ok {
			return nil,errors.New("cannot access array fieldValue")
		}
		primaryKeys,ok := _primaryKeys.(PrimaryKeys)
		if !ok {
			//调用字段定义的序列化方法
			toString := this.model.ConverToString[fieldName]
			strVal,err := toString(_primaryKeys)
			if nil != err {
				return nil,err
			}
			serializeVal[fieldName] = strVal
		} else {
			//如果是主键集合，则使用默认的json序列化
			jsonStr,err := jsoniter.MarshalToString(primaryKeys)
			if nil != err {
				return nil,err
			}
			serializeVal[fieldName] = jsonStr
		}
	}


	for i:=0; i<jsonFormatsLen; i++ {
		fieldName := this.model.jsonFormats[i]
		_primaryKey,ok := serializeVal[fieldName]
		if !ok {
			return nil,errors.New("cannot access objectValue")
		}
		primaryKey,ok := _primaryKey.(PrimaryKey)
		if !ok {
			//调用字段定义的序列化方法
			toString := this.model.ConverToString[fieldName]
			strVal,err := toString(_primaryKey)
			if nil != err {
				return nil,err
			}
			serializeVal[fieldName] = strVal
		} else {
			serializeVal[fieldName] = uint64(primaryKey)
		}
	}

	return serializeVal,nil
}






func (this *WriteOrmWithClient) insert( pipeline *redis.Pipeliner,key *KwPatter,val map[string]interface{} ) error {
	var (
		err               error
		set_index         *RedisSetIndex
		smember           uint64
		formated_key      FormatString
		classname         string
		inject_belong_key string
	)


	//将容器类型序列化
	serializeVal,err := this.serializeReletionVal(val)
	if nil != err {
		return err
	}


	for _,set_index = range this.model.setIndexList {
		formated_key,classname,smember,err = set_index.BuildSmember(val)
		if nil != err {
			return err
		}
		err = (*pipeline).SAdd(string(formated_key),smember).Err()
		if nil != err {
			return err
		}
		//在写入数据中注入当前数据所属集合分类名称
		inject_belong_key = "__setindex:"+set_index.identification+"__"
		val[inject_belong_key] = classname
		//存储索引的分类列表
		err = (*pipeline).SAdd(set_index.classPattern,classname).Err()
		if nil != err {
			return err
		}
	}


	err = (*pipeline).HMSet(string(key.Formart()),serializeVal).Err()
	if err != err {
		return err
	}


	//遍历索引列表构建索引
	var (
		build_memberz   redis.Z
		unique_index    *RedisUniqueIndex
	)
	for _,unique_index = range this.model.uniqueIndexList {
		formated_key,build_memberz,err = unique_index.BuildZMember(val)
		if nil != err {
			return err
		}
		err = (*pipeline).ZAdd(string(formated_key),build_memberz).Err()
		if nil != err {
			return err
		}
	}


	//构建比特索引
	var (
		bitOffset  int64
		bitBool    int
		bitindex   *RedisBitIndex
	)
	for _,bitindex = range this.model.bitIndexList {
		formated_key,bitOffset,bitBool,err = bitindex.BuildOffsetWithBool(val)
		if nil != err {
			return err
		}
		err = (*pipeline).SetBit(string(formated_key),bitOffset,bitBool).Err()
		if nil != err {
			return err
		}
	}

	//没必要做数据条数统计，要找表的条数，找个唯一索引，看它的条数就知道了
	return nil
}





func (this *WriteOrmWithClient) update(pipeline *redis.Pipeliner,key *KwPatter,oldval,newval map[string]interface{}) error {
	dataId := key.key[0].(uint64)
	//处理集合迁移的问题
	var (
		err               error
		set_index         *RedisSetIndex
		SMOVE_KEYS        [6]string
		inject_belong_key string
		oldclassName      string
		newclassName      string
	)


	//将容器类型序列化
	serializeVal,err := this.serializeReletionVal(newval)
	if nil != err {
		return err
	}


	SMOVE_KEYS[0] = this.model.tableName
	SMOVE_KEYS[5] = strconv.FormatUint(dataId,10)
	for _,set_index = range this.model.setIndexList {
		//旧数据所属集合分类名称
		oldclassName,err = set_index.ExprClassName(UserExprAcess{oldval,set_index.classNameUse})
		if nil != err {
			return err
		}
		//新数据所属集合分类名称
		newclassName,err = set_index.ExprClassName(UserExprAcess{newval,set_index.classNameUse})
		if nil != err {
			return err
		}
		//迁移成员
		if oldclassName != newclassName {
			//err = (*pipeline).SMove(oldclassKey,newclassKey,dataId).Err()
			SMOVE_KEYS[1] = set_index.identification
			SMOVE_KEYS[2] = set_index.patternField
			SMOVE_KEYS[3] = oldclassName
			SMOVE_KEYS[4] = newclassName
			err = (*pipeline).EvalSha(smoveTiggerClassName,SMOVE_KEYS[:]).Err()
			if nil != err {
				return err
			}
			//存储索引的新分类
			err = (*pipeline).SAdd(set_index.classPattern,newclassName).Err()
			if nil != err {
				return err
			}
		}
		//更新数据中当前数据所属集合名称
		inject_belong_key = "__setindex:"+set_index.identification+"__"
		newval[inject_belong_key] = newclassName
	}


	//更新body数据，直接覆盖即可
	err = (*pipeline).HMSet(string(key.Formart()),serializeVal).Err()
	if nil != err {
		return err
	}

	//更新唯一索引
	var(
		formated_key      FormatString
		build_memberz     redis.Z
		old_formarted_key FormatString
		old_build_memberz redis.Z
		unique_index     *RedisUniqueIndex
	)
	for _,unique_index = range this.model.uniqueIndexList {
		//old
		old_formarted_key,old_build_memberz,err = unique_index.BuildZMember(oldval)
		if nil != err {
			return err
		}
		//new
		formated_key,build_memberz,err = unique_index.BuildZMember(newval)
		if nil != err {
			return err
		}
		//equal
		if formated_key==old_formarted_key&&build_memberz==old_build_memberz{
			continue
		}
		//删除旧成员
		err = (*pipeline).ZRem(string(formated_key),old_build_memberz.Member).Err()
		if nil != err {
			return err
		}
		//添加新成员
		err = (*pipeline).ZAdd(string(formated_key),build_memberz).Err()
		if nil != err {
			return err
		}
	}


	//更新比特索引
	var (
		bitOffset     int64
		bitBool       int
		old_bitOffset int64
		old_bitBool   int
		bitindex      *RedisBitIndex
	)
	for _,bitindex = range this.model.bitIndexList {
		//old
		old_formarted_key,old_bitOffset,old_bitBool,err = bitindex.BuildOffsetWithBool(oldval)
		if nil != err {
			return err
		}
		//new
		formated_key,bitOffset,bitBool,err = bitindex.BuildOffsetWithBool(newval)
		if nil != err {
			return err
		}
		//equal
		if formated_key==old_formarted_key&&old_bitOffset==bitOffset&&old_bitBool==bitBool {
			continue
		}
		err = (*pipeline).SetBit(string(formated_key),bitOffset,bitBool).Err()
		if nil != err {
			return err
		}
	}

	return nil
}





func (this *WriteOrmWithClient) delete(pipeline *redis.Pipeliner,key *KwPatter,val map[string]interface{}) error {
	dataId := key.GetKey()[0].(uint64)
	if dataId <= 0 {
		return errors.New("invalid dataid")
	}

	var (
		err           error
		set_index     *RedisSetIndex
		DEL_KEYS      [5]string
		classname     string
	)

	DEL_KEYS[0] = this.model.tableName
	DEL_KEYS[4] = strconv.FormatUint(dataId,10)
	for _,set_index = range this.model.setIndexList {
		classname,err = set_index.ExprClassName(UserExprAcess{val,set_index.classNameUse})
		if nil != err {
			return err
		}
		DEL_KEYS[1] = set_index.identification
		DEL_KEYS[2] = set_index.patternField
		DEL_KEYS[3] = classname
		err = (*pipeline).EvalSha(deleteTiggerClassName,DEL_KEYS[:]).Err()
		if nil != err {
			return err
		}
	}


	var (
		formated_key    FormatString
		unique_index    *RedisUniqueIndex
		build_memberz   redis.Z
	)
	for _,unique_index = range this.model.uniqueIndexList {
		formated_key,build_memberz,err = unique_index.BuildZMember(val)
		if nil != err {
			return err
		}
		//移除唯一索引成员
		err = (*pipeline).ZRem(string(formated_key),build_memberz.Member).Err()
		if nil != err {
			return err
		}
	}

	//比特索引比较特殊，比如男女性别，不能因为删除这个动作改变某条数据性别
	//因此比特索引不作操作
	//删除body数据
	err = (*pipeline).Del(string(key.Formart())).Err()
	if nil != err {
		return err
	}
	return nil
}
