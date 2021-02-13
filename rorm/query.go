package rorm

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
	_field "github.com/lucidStream/coify/rorm/field"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

//仅提供基础查询支持
//复杂查询请使用Lua实现

//有哪些分类
func (this *ReadOrmWithClient)ClassList( index_name string,match string ) ([]string,error) {
	index := this.model.setIndexList[index_name]
	if nil == index {
		return nil,errors.New("no setindex:"+index_name)
	}
	if match == "" { match="*" }
	var (
		err     error
		cursor  uint64
		extract []string
		exists  bool
		iter    int
	)
	repeat  := make(map[string]struct{},100)
	result  := make([]string,0,100)
	for {
		extract,cursor,err = this.client.SScan(index.classPattern,cursor,match,1000).Result()
		if nil != err {
			return nil,err
		}

		for iter = 0; iter<len(extract); iter++ {
			_,exists = repeat[extract[iter]]
			if false == exists {
				repeat[extract[iter]] = struct{}{}
				result = append(result,extract[iter])
			}
		}

		if cursor == 0 { break }
	}
	return result,nil
}



//分类总数
func (this *ReadOrmWithClient)ClassTotal(index_name string) (uint64,error) {
	index := this.model.setIndexList[index_name]
	if nil == index {
		return 0,errors.New("no setindex:"+index_name)
	}
	total,err := this.client.SCard(index.classPattern).Result()
	if nil != err {
		return 0,err
	}
	return uint64(total),nil
}





//分类成员 match:成员匹配规则
func (this *ReadOrmWithClient) ClassMemebers( index_name string,classname string,match string ) ([]uint64,error) {
	index := this.model.setIndexList[index_name]
	if nil == index {
		return nil,errors.New("no setindex:"+index_name)
	}
	if match == "" { match="*" }
	var (
		err     error
		cursor  uint64
		extract []string
		exists  bool
		iter    int
		parse_member uint64
	)
	setkey := fmt.Sprintf(index.keyPattern,classname)
	repeat  := make(map[string]struct{},100)
	result  := make([]uint64,0,100)
	for {
		extract,cursor,err = this.client.SScan(setkey,cursor,match,1000).Result()
		if nil != err {
			return nil,err
		}

		for iter = 0; iter<len(extract); iter++ {
			_,exists = repeat[extract[iter]]
			if false == exists {
				repeat[extract[iter]] = struct{}{}
				parse_member,err = strconv.ParseUint(extract[iter],10,64)
				if nil != err {
					return nil,err
				}
				result = append(result,parse_member)
			}
		}

		if cursor == 0 { break }
	}
	return result,nil
}




//总数统计
func (this *ReadOrmWithClient) Total( index_name string ) (uint64,error) {
	index := this.model.uniqueIndexList[index_name]
	if nil == index {
		return 0,errors.New("no unique index:"+index_name)
	}
	//时间复杂度：O(1)
	result,err := this.client.ZCard(index.keyPattern).Result()
	if nil != err {
		return 0,err
	}
	return uint64(result),nil
}





var ErrBodyNotExists = errors.New("bodyData not exists")



func (this *ReadOrmWithClient) isFullNil( r []interface{} ) bool {
	rLen := len(r)
	var nilCounter int
	for i:=0; i<rLen; i++ {
		if r[i] == nil {
			nilCounter++
		}
	}
	return nilCounter == rLen
}




//通过主键查询数据
func (this *ReadOrmWithClient) GetBody( key []interface{},fields []string ) (map[string]interface{},error) {
	var (
		err       error
		result    []interface{}
		val       map[string]interface{}
	)
	kpttern := newKeyWithPattern(key,&this.model.pattern)
	format_key := string(kpttern.Formart())
	if fields == nil || len(fields) == 0 {
		fields = this.model.FieldList
	}
	result,err = this.client.HMGet(format_key,fields...).Result()
	if nil != err {
		return nil,err
	}
	var ( valString string; typeOk bool; errMsg string )
	fieldsLen := len(fields)
	for i:=0; i<fieldsLen; i++ {
		//有一个字段为nil则无法解析解析
		if result[i] == nil {
			if this.isFullNil(result) {
				return nil,ErrBodyNotExists
			}
			errMsg = fmt.Sprintf("%s is nil value",fields[i])
			return nil,errors.New(errMsg)
		}
		valString,typeOk = result[i].(string)
		if false == typeOk {
			errMsg = fmt.Sprintf("%s type not string",fields[i])
			return nil,errors.New(errMsg)
		}
		if val == nil {
			val = make(map[string]interface{},len(result))
		}
		//检查数据是否需要从其他模型加载
		fieldSetting := this.model.TypeExpr[fields[i]]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fields[i])
			panic(errMsg)
		}
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if ok && nil!=fieldOrm && fieldUsingDataId != "" {
			err = this.fromOrmLoad(fields[i],valString,val)
			if nil != err {
				return nil,err
			}
		} else {
			val[fields[i]],err = this.model.StringParse[fields[i]](valString)
			if nil != err {
				return nil,err
			}
		}
	}
	return val,nil
}





//从其他模型加载字段值
func (this *ReadOrmWithClient) fromOrmLoad ( fieldName,valString string,val map[string]interface{} ) error {
	var ( err error; uint64Id uint64 )
	fieldSetting := this.model.TypeExpr[fieldName]
	if fieldSetting == nil {
		errMsg := fmt.Sprintf("field %s mission fieldSetting",fieldName)
		panic(errMsg)
	}
	fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
	if !ok || nil == fieldOrm {
		return errors.New("fieldSetting orm is nil pointer")
	}
	ormBindClient :=NewReadOrmWithClient(fieldOrm,this.client)
	switch fieldSetting.Type {
	case &_field.ArrayPair:
		var idsArray PrimaryKeys
		err = jsoniter.UnmarshalFromString(valString,&idsArray)
		if nil != err {
			return err
		}
		valArray := make([]interface{},0,len(idsArray))
		for j:=0; j<len(idsArray); j++ {
			if nil != idsArray[j] {
				//直接查全部字段
				uint64Id = uint64(idsArray[j].(float64))
				info,err := ormBindClient.GetBody([]interface{}{uint64Id},nil)
				if nil != err {
					return err
				}
				valArray = append(valArray,info)
			} else {
				valArray = append(valArray,nil)
			}
		}
		val[fieldName] = valArray
	case &_field.JsonPair,&_field.XMLPair:
		var primaryKey uint64
		if valString != "null" {
			primaryKey,err = strconv.ParseUint(valString,10,64)
			if nil != err {
				return err
			}
			info,err := ormBindClient.GetBody([]interface{}{primaryKey},nil)
			if nil != err {
				return err
			}
			val[fieldName] = info
		} else {
			val[fieldName] = nil
		}
	}
	return nil
}





func (this *ReadOrmWithClient) pipeGetBody( key []interface{},fields []string,pipeline *redis.Pipeliner ) error {
	if fields == nil || len(fields) == 0 {
		fields = this.model.FieldList
	}
	var err error
	kpttern := newKeyWithPattern(key,&this.model.pattern)
	format_key := string(kpttern.Formart())
	_,err = (*pipeline).HMGet(format_key,fields...).Result()
	if nil != err {
		return err
	}
	return nil
}






//批量获取，其中一个不存在时，对应数组为空
func (this *ReadOrmWithClient) MutilGetBody( keys [][]interface{},fields []string ) ([]map[string]interface{},error) {
	if fields == nil || len(fields) == 0 {
		fields = this.model.FieldList
	}
	klen := len(keys)
	result_list := make([]map[string]interface{},0,klen)
	pipeline := this.client.TxPipeline()
	defer func() {
		err := pipeline.Close()
		if nil != err { logrus.Error(err) }
	}()
	var (
		err       error
		mutil_cmd []redis.Cmder
	)
	for i:=0; i<klen; i++ {
		err = this.pipeGetBody(keys[i],fields,&pipeline)
		if nil != err {
			return nil,err
		}
	}
	if mutil_cmd,err = pipeline.Exec();nil != err {
		return nil,err
	}

	var (
		cmd         *redis.SliceCmd
		cmd_ret     []interface{}
		iter        int
		jter        int
		val         map[string]interface{}
		valString   string
		typeOk      bool
	)
	cmdLen    := len(mutil_cmd)
	fieldsLen := len(fields)
loop:
	cmd = mutil_cmd[iter].(*redis.SliceCmd)
	cmd_ret,err = cmd.Result()
	if nil != err {
		return nil,err
	}
	//parse row
	val = nil
	for jter=0; jter<fieldsLen; jter++ {
		valString,typeOk = cmd_ret[jter].(string)
		if false == typeOk {
			return nil,errors.New("type not string")
		}
		if val == nil {
			val = make(map[string]interface{},len(cmd_ret))
		}
		//检查数据是否需要从其他模型加载
		fieldSetting := this.model.TypeExpr[fields[jter]]
		if fieldSetting == nil {
			errMsg := fmt.Sprintf("field %s mission fieldSetting",fields[jter])
			panic(errMsg)
		}
		fieldUsingDataId := strings.Trim(fieldSetting.DataId," ")
		fieldOrm,ok := fieldSetting.Rorm.(*RedisOrm)
		if ok && nil!=fieldOrm && fieldUsingDataId != "" {
			err = this.fromOrmLoad(fields[jter],valString,val)
			if nil != err {
				return nil,err
			}
		} else {
			val[fields[jter]],err = this.model.StringParse[fields[jter]](valString)
			if nil != err {
				return nil,err
			}
		}
	}
	result_list = append(result_list,val)
	if iter++; iter<cmdLen {
		goto loop
	}

	//长度必须相等，空元素为nil
	if klen != len(result_list) {
		return nil,errors.New("missing something data")
	}
	return result_list,nil
}





//批量根据主键检查body数据是否存在
func (this *ReadOrmWithClient) ExistsBody( keys [][]interface{} ) ([]int64,error) {
	keysLen := len(keys)
	if 0 == keysLen {
		return nil,errors.New("empty keys")
	}
	var (
		groupLen = len(keys[0])
		ARGV       []interface{}
		result     []int64
		KEYS =     [2]string{this.model.pattern,strconv.Itoa(groupLen)}
	)
	for i:=0; i<keysLen; i++ {
		ARGV = append(ARGV,keys[i]...)
	}
	ret,err := this.client.EvalSha(mutilBodyExistsCheck,KEYS[:],ARGV...).Result()
	if nil != err {
		return nil,err
	}
	retType := ret.([]interface{})
	for i:=0; i<keysLen; i++ {
		result = append(result,retType[i].(int64))
	}
	return result,nil
}






type UniqueInspect struct{
	Index         *RedisUniqueIndex
	Member        string
	Score         float64
}



//根据提供的值检查唯一索引唯一性
//newval需要是字段填充完整的数据
func (this *ReadOrmWithClient) UniqueCheck( newval,oldval map[string]interface{} ) (UniqueInspect,error) {
	var (
		err               error
		checkRet          UniqueInspect
		resultType        []interface{}
		unique_index      *RedisUniqueIndex
		build_memberz     redis.Z
		old_build_memberz redis.Z
	)
	uniqueListLen := len(this.model.uniqueIndexList)
	KEYS := []string{this.model.tableName}
	ARGV := make([]interface{},0,4*uniqueListLen)
	for _,unique_index = range this.model.uniqueIndexList {
		_,build_memberz,err = unique_index.BuildZMember(newval)
		if nil != err {
			return checkRet,err
		}
		_,old_build_memberz,err = unique_index.BuildZMember(oldval)
		if nil != err {
			return checkRet,err
		}
		//相等证明member并没有改变，在每个member都是唯一的前提下
		//排除掉唯一的member在用被排除掉的唯一member检查数据就是不存在的
		if old_build_memberz.Member == build_memberz.Member {
			continue
		}
		//当member发生改变时，检查新member(key)是否已经存在
		ARGV = append(ARGV,unique_index.identification)
		ARGV = append(ARGV,unique_index.patterMfield)
		ARGV = append(ARGV,unique_index.patterSfield)
		ARGV = append(ARGV,build_memberz.Member)
	}
	if len(ARGV) == 0 {
		return checkRet,nil
	}
	result,err := this.client.EvalSha(memberExistsCheck,KEYS,ARGV...).Result()
	if nil != err {
		return checkRet,err
	}
	resultType = result.([]interface{})
	if len(resultType) == 0 {
		return checkRet,nil
	}

	checkRet.Index  = this.model.uniqueIndexList[resultType[0].(string)]
	checkRet.Member = resultType[1].(string)
	checkRet.Score,err = strconv.ParseFloat(resultType[2].(string),64)
	if nil != err {
		return checkRet,err
	}
	if checkRet.Index == nil {
		return checkRet,errors.New("cannot find index")
	}
	return checkRet,nil
}




//检查后如果需要做写操作需要在一个事务保证隔离性
func (this *ReadOrmWithClient) ExistsCheck( val map[string]interface{} ) (UniqueInspect,error) {
	var (
		err             error
		checkRet        UniqueInspect
		resultType      []interface{}
		unique_index    *RedisUniqueIndex
		build_memberz   redis.Z
	)
	uniqueListLen := len(this.model.uniqueIndexList)
	KEYS := []string{this.model.tableName}
	ARGV := make([]interface{},0,4*uniqueListLen)
	for _,unique_index = range this.model.uniqueIndexList {
		_,build_memberz,err = unique_index.BuildZMember(val)
		if nil != err {
			return checkRet,err
		}
		ARGV = append(ARGV,unique_index.identification)
		ARGV = append(ARGV,unique_index.patterMfield)
		ARGV = append(ARGV,unique_index.patterSfield)
		ARGV = append(ARGV,build_memberz.Member)
	}
	if len(ARGV) == 0 {
		return checkRet,nil
	}
	result,err := this.client.EvalSha(memberExistsCheck,KEYS,ARGV...).Result()
	if nil != err {
		return checkRet,err
	}
	resultType = result.([]interface{})
	if len(resultType) == 0 {
		return checkRet,nil
	}

	checkRet.Index  = this.model.uniqueIndexList[resultType[0].(string)]
	checkRet.Member = resultType[1].(string)
	checkRet.Score,err = strconv.ParseFloat(resultType[2].(string),64)
	if nil != err {
		return checkRet,err
	}
	if checkRet.Index == nil {
		return checkRet,errors.New("cannot find index")
	}
	return checkRet,nil
}