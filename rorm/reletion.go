package rorm

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unsafe"
)




type NullValue     uint64
type PrimaryKey    uint64
type NewPrimaryKey PrimaryKey
type PrimaryKeys   []interface{}



func (this *RormWritePlain) reletionInsert( val,extra map[string]interface{} ) (map[string]interface{},error) {
	return this.reletionOpration(val,extra,"insert")
}


func (this *RormWritePlain) reletionUpdate( val,extra map[string]interface{} ) (map[string]interface{},error) {
	return this.reletionOpration(val,extra,"update")
}


func (this *RormWritePlain) reletionDelete( val,extra map[string]interface{} ) (map[string]interface{},error) {
	return this.reletionOpration(val,extra,"delete")
}





func (this *RormWritePlain) primaryKeyInsert( val,extra map[string]interface{} ) ([]func() error,error) {
	unLocks,err := this.reletionPrimaryKey(val,extra,"insert")
	if nil != unLocks && nil!=err {
		for i:=0; i<len(unLocks); i++ {
			_=unLocks[i]()
		}
		unLocks = nil
	}
	return unLocks,err
}


func (this *RormWritePlain) primaryKeyUpdate( val,extra map[string]interface{} ) ([]func() error,error) {
	unLocks,err := this.reletionPrimaryKey(val,extra,"update")
	if nil != unLocks && nil!=err {
		for i:=0; i<len(unLocks); i++ {
			_=unLocks[i]()
		}
		unLocks = nil
	}
	return unLocks,err
}


func (this *RormWritePlain) primaryKeyDelete( val,extra map[string]interface{} ) ([]func() error,error) {
	unLocks,err := this.reletionPrimaryKey(val,extra,"delete")
	if nil != unLocks && nil!=err {
		for i:=0; i<len(unLocks); i++ {
			_=unLocks[i]()
		}
		unLocks = nil
	}
	return unLocks,err
}






//在任何操作之前先确定好关联对象的主键
//当确定要操作数据库，先把对应的数据ID上锁，返回解锁回调函数
func (this *RormWritePlain) reletionPrimaryKey( val,extra map[string]interface{},opration string ) ([]func() error,error) {
	unLockList := make([]func() error,0,16)
	arrayFormatsLen := len(this.ormwith.model.arrayFormats)
	var primaryKey uint64
	for i:=0; i<arrayFormatsLen; i++ {
		fieldName := this.ormwith.model.arrayFormats[i]
		fieldSetting := this.ormwith.model.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		interval,exists := val[fieldName]
		if !exists {
			errMsg := fmt.Sprintf("field %s not exists in value",fieldName)
			return unLockList,errors.New(errMsg)
		}
		//字段处理模型
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if !ok || fieldOrm == nil {
			continue
		}
		ormBindClient := NewWriteOrmWithClient(fieldOrm,this.ormwith.client)
		plain,err := ormBindClient.SyncPlain(this.splain.GetDispatch(),10*time.Second)
		if nil != err {
			return unLockList,err
		}
		interArray := interval.([]interface{})
		arrLen := len(interArray)
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		for xxt:=0; xxt<arrLen; xxt++ {
			interface_obj := (*struct{ Type uintptr ; Data uintptr })(unsafe.Pointer(&interArray[xxt]))
			if interface_obj.Data == 0 {
				//数组元素为空，不需要搞出一个主键，不需要锁住谁
				continue
			}
			//关联的数据使用同样的客户端节点，不支持关联八杆子打不着的数据
			//调用模型处理
			var xxtVal map[string]interface{}
			switch interArray[xxt].(type) {
			case PrimaryKey:
				_primaryKey := interArray[xxt].(PrimaryKey)
				primaryKey = uint64(_primaryKey)
				goto checkPrimaryKey
			case map[string]interface{}:
				xxtVal = interArray[xxt].(map[string]interface{})
			default:
				return unLockList,errors.New("unknown array element type")
			}
			switch opration {
			case "insert":
				//确定主键
				if fieldUsingDataId != "" {
					if fieldUsingDataId != "autoinc" {
						//数组不能使用宿主ID作为主键
						//避免多个数组中对象主键相同的情况
						return unLockList,errors.New("array element cannot using parent value as primaryKey")
					}
					newid,err := ormBindClient.SetAutoIncId(xxtVal)
					if nil != err {
						return unLockList,err
					}
					primaryKey = newid
				} else {
					//为当前字段填充一个零值ID
					primaryKey = 0
				}
				//设置主键
				xxtVal[fieldOrm.IdFieldName] = primaryKey
			case "update":
				//空值表示不进行数据操作
				if fieldUsingDataId != "" {
					primaryKey,ok = xxtVal[fieldOrm.IdFieldName].(uint64)
					if !ok {
						if fieldUsingDataId != "autoinc" {
							//数组不能使用宿主ID作为主键
							//避免多个数组中对象主键相同的情况
							return unLockList,errors.New("array element cannot using parent value as primaryKey")
						}
						newid,err := ormBindClient.SetAutoIncId(xxtVal)
						if nil != err {
							return unLockList,err
						}
						primaryKey = newid
						xxtVal[fieldOrm.IdFieldName] = NewPrimaryKey(primaryKey)
					} else {
						xxtVal[fieldOrm.IdFieldName] = primaryKey
					}
				} else {
					primaryKey = 0
					xxtVal[fieldOrm.IdFieldName] = primaryKey
				}
			case "delete":
				if fieldUsingDataId != "" {
					primaryKey,ok = xxtVal[fieldOrm.IdFieldName].(uint64)
					if !ok {
						return unLockList,errors.New("bad access reletion id")
					}
					xxtVal[fieldOrm.IdFieldName] = primaryKey
				} else {
					primaryKey = 0
					xxtVal[fieldOrm.IdFieldName] = primaryKey
				}
			default:
				errMsg := fmt.Sprintf("field %s unknown opration: %s",fieldName,opration)
				return unLockList,errors.New(errMsg)
			}
		checkPrimaryKey:
			if primaryKey > 0 {
				//先锁住对应的数据行
				unLock,err := plain.Lock([]interface{}{uint64(primaryKey)})
				if nil != err {
					return unLockList,err
				}
				unLockList = append(unLockList,unLock)
			}
		}
		val[fieldName] = interArray
	}

	jsonFormatsLen := len(this.ormwith.model.jsonFormats)
	for i:=0; i<jsonFormatsLen; i++ {
		fieldName := this.ormwith.model.jsonFormats[i]
		fieldSetting := this.ormwith.model.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		interval,exists := val[fieldName]
		if !exists {
			errMsg := fmt.Sprintf("field %s not exists in value",fieldName)
			return unLockList,errors.New(errMsg)
		}
		//字段处理模型
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if !ok || fieldOrm == nil {
			continue
		}
		interface_obj := (*struct{ Type uintptr ; Data uintptr })(unsafe.Pointer(&interval))
		if interface_obj.Data == 0 {
			//字段值为空，不需要搞出一个主键，不需要锁住谁
			continue
		}
		ormBindClient := NewWriteOrmWithClient(fieldOrm,this.ormwith.client)
		plain,err := ormBindClient.SyncPlain(this.splain.GetDispatch(),10*time.Second)
		if nil != err {
			return unLockList,err
		}
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		var xxtVal map[string]interface{}
		switch interval.(type) {
		case PrimaryKey:
			_primaryKey := interval.(PrimaryKey)
			primaryKey  = uint64(_primaryKey)
			goto checkPrimaryKey2
		case map[string]interface{}:
			xxtVal = interval.(map[string]interface{})
		default:
			return unLockList,errors.New("unSupport objectValue type")
		}
		switch opration {
		case "insert":
			//确定主键
			if fieldUsingDataId != "" {
				if fieldUsingDataId != "autoinc" {
					//非自增ID取本模型某个字段作为ID
					pkey,ok := val[fieldUsingDataId]
					if !ok {
						return unLockList,errors.New("bad access primaryKey")
					}
					primaryKey,ok = pkey.(uint64)
					if !ok {
						return unLockList,errors.New("primaryKey not uint64")
					}
					xxtVal[fieldOrm.IdFieldName] = primaryKey
				} else {
					newid,err := ormBindClient.SetAutoIncId(xxtVal)
					if nil != err {
						return unLockList,err
					}
					primaryKey = newid
					xxtVal[fieldOrm.IdFieldName] = primaryKey
				}
			} else {
				primaryKey = 0
				xxtVal[fieldOrm.IdFieldName] = primaryKey
			}
		case "update":
			if fieldUsingDataId != "" {
				//使用没有主键字段表示需要生成
				//正常情况下更新一个对象类型的字段，如果该字段以前有值，则应强制覆盖ID
				primaryKey,ok = xxtVal[fieldOrm.IdFieldName].(uint64)
				if !ok {
					if fieldUsingDataId != "autoinc" {
						//非自增ID取本模型某个字段作为ID
						pkey,ok := val[fieldUsingDataId]
						if !ok {
							return unLockList,errors.New("bad access primaryKey")
						}
						primaryKey,ok = pkey.(uint64)
						if !ok {
							return unLockList,errors.New("primaryKey not uint64")
						}
						xxtVal[fieldOrm.IdFieldName] = NewPrimaryKey(primaryKey)
					} else {
						newid,err := ormBindClient.SetAutoIncId(xxtVal)
						if nil != err {
							return unLockList,err
						}
						primaryKey = newid
						xxtVal[fieldOrm.IdFieldName] = NewPrimaryKey(primaryKey)
					}
				} else {
					xxtVal[fieldOrm.IdFieldName] = primaryKey
				}
			} else {
				primaryKey = 0
				xxtVal[fieldOrm.IdFieldName] = primaryKey
			}
		case "delete":
			if fieldUsingDataId != "" {
				primaryKey,ok = xxtVal[fieldOrm.IdFieldName].(uint64)
				if !ok {
					return unLockList,errors.New("bad access reletion id")
				}
				xxtVal[fieldOrm.IdFieldName] = primaryKey
			} else {
				primaryKey = 0
				xxtVal[fieldOrm.IdFieldName] = primaryKey
			}
		default:
			errMsg := fmt.Sprintf("field %s unknown opration: %s",fieldName,opration)
			return unLockList,errors.New(errMsg)
		}
		val[fieldName] = xxtVal
	checkPrimaryKey2:
		if primaryKey > 0 {
			//先锁住对应的数据行
			unLock,err := plain.Lock([]interface{}{uint64(primaryKey)})
			if nil != err {
				return unLockList,err
			}
			unLockList = append(unLockList,unLock)
		}
	}
	return unLockList,nil
}





func (this *RormWritePlain) reletionOpration( val,extra map[string]interface{},opration string ) (map[string]interface{},error) {
	arrayFormatsLen := len(this.ormwith.model.arrayFormats)
	var primaryKey uint64
	//保存变幻之前的内容
	frostyAfterKeys := make(map[string]interface{})
	for i:=0; i<arrayFormatsLen; i++ {
		fieldName := this.ormwith.model.arrayFormats[i]
		fieldSetting := this.ormwith.model.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		interval,exists := val[fieldName]
		if !exists {
			errMsg := fmt.Sprintf("field %s not exists in value",fieldName)
			return nil,errors.New(errMsg)
		}
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		//字段处理模型
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if !ok || fieldOrm==nil || "" == fieldUsingDataId {
			continue
		}
		ormBindClient := NewWriteOrmWithClient(fieldOrm,this.ormwith.client)
		plain,err := ormBindClient.SyncPlain(this.splain.GetDispatch(),10*time.Second)
		if nil != err {
			return nil,err
		}
		interArray := interval.([]interface{})
		arrLen := len(interArray)
		frostyAfterValue := make(PrimaryKeys,arrLen)
		copy(frostyAfterValue,interArray)
		for xxt:=0; xxt<arrLen; xxt++ {
			interface_obj := (*struct{ Type uintptr ; Data uintptr })(unsafe.Pointer(&interArray[xxt]))
			if interface_obj.Data == 0 {
				//如果是一个空值，请问你用什么主键insert，update谁？delete谁？
				continue
			}
			//每个元素的数据操作类型可能是不同的
			currentOpration := opration
			//关联的数据使用同样的客户端节点，不支持关联八杆子打不着的数据
			//调用模型处理
			xxtVal := interArray[xxt].(map[string]interface{})
			xxtValPrimaryKey,ok := xxtVal[fieldOrm.IdFieldName]
			if !ok {
				return nil,errors.New("primaryKey field not exists")
			}
			//该字段的值中包含了主键
			switch xxtValPrimaryKey.(type) {
			case NewPrimaryKey:
				primaryKey = uint64(xxtValPrimaryKey.(NewPrimaryKey))
				ok = true
				currentOpration = "insert"
			case uint64:
				primaryKey,ok = xxtValPrimaryKey.(uint64)
			default:
				return nil,errors.New("")
			}
			if !ok {
				return nil,errors.New("bad access reletion id")
			}
			if 0 == primaryKey {
				//零值ID代表不进行实际的数据操作
				continue
			}
			switch currentOpration {
			case "insert":
				//确定主键
				err = plain.doInsert([]interface{}{primaryKey},xxtVal,val)
			case "update":
				err = plain.doUpdate([]interface{}{primaryKey},xxtVal,val)
			case "delete":
				err = plain.doDelete([]interface{}{primaryKey})
			default:
				errMsg := fmt.Sprintf("field %s unknown opration: %s",fieldName,opration)
				err = errors.New(errMsg)
			}
			if nil != err {
				return nil,err
			}
			//将数组元素变幻为主ID
			interArray[xxt] = PrimaryKey(primaryKey)
		}
		//将变幻后的数组转换为主键集合类型
		val[fieldName] = PrimaryKeys(interArray)
		//保存变幻前的值
		frostyAfterKeys[fieldName] = frostyAfterValue
	}

	jsonFormatsLen := len(this.ormwith.model.jsonFormats)
	for i:=0; i<jsonFormatsLen; i++ {
		currentOpration := opration
		fieldName := this.ormwith.model.jsonFormats[i]
		fieldSetting := this.ormwith.model.TypeExpr[fieldName]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
			panic(errMsg)
		}
		interval,exists := val[fieldName]
		if !exists {
			errMsg := fmt.Sprintf("field %s not exists in value",fieldName)
			return nil,errors.New(errMsg)
		}
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		//字段处理模型
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if !ok || fieldOrm==nil || "" == fieldUsingDataId {
			continue
		}
		interface_obj := (*struct{ Type uintptr ; Data uintptr })(unsafe.Pointer(&interval))
		if interface_obj.Data == 0 {
			//该字段是一个空值，往哪个模型去写？主键是什么？内容是什么？
			continue
		}
		ormBindClient := NewWriteOrmWithClient(fieldOrm,this.ormwith.client)
		plain,err := ormBindClient.SyncPlain(this.splain.GetDispatch(),10*time.Second)
		if nil != err {
			return nil,err
		}
		xxtVal := interval.(map[string]interface{})
		//该字段的值中包含了主键
		xxtValPrimaryKey,ok := xxtVal[fieldOrm.IdFieldName]
		if !ok {
			return nil,errors.New("primaryKey field not exists")
		}
		//该字段的值中包含了主键
		switch xxtValPrimaryKey.(type) {
		case NewPrimaryKey:
			primaryKey = uint64(xxtValPrimaryKey.(NewPrimaryKey))
			ok = true
			currentOpration = "insert"
		case uint64:
			primaryKey,ok = xxtValPrimaryKey.(uint64)
		default:
			return nil,errors.New("")
		}
		if !ok {
			return nil,errors.New("bad access reletion id")
		}
		//保存变幻之前的值
		frostyAfterKeys[fieldName] = xxtVal
		switch currentOpration {
		case "insert":
			err = plain.doInsert([]interface{}{primaryKey},xxtVal,val)
		case "update":
			err = plain.doUpdate([]interface{}{primaryKey},xxtVal,val)
		case "delete":
			err = plain.doDelete([]interface{}{primaryKey})
		default:
			errMsg := fmt.Sprintf("field %s unknown opration: %s",fieldName,opration)
			err = errors.New(errMsg)
		}
		if nil != err {
			return nil,err
		}
		//将字段变幻为主键ID
		val[fieldName] = PrimaryKey(primaryKey)
	}
	//返回发生变幻的字段包含变幻前的内容
	return frostyAfterKeys,nil
}
