package rorm

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"strings"
	_field "github.com/lucidStream/coify/rorm/field"
)



type Pattern string


const(
	SetIndexPattern    = Pattern("table=%s:type=%s:index=%s:field=%s:class=%s")
	BitIndexPattern    = Pattern("table=%s:type=%s:index=%s:field=%s")
	UniqueIndexPattern = Pattern("table=%s:type=%s:index=%s:mfield=%s:sfield=%s")
	SetClassPattern    = Pattern("table=%s:type=%s:index=%s:classlist")
)




func defaultOneScoreMerge( a []float64 ) (f float64, e error) {
	return a[0],nil
}




//redis唯一索引
//比如createtime，就应当在有序集合的score里面，而不应该放到主数据体中
//需要查看对象的createtime，就需要从索引中查找
//索引中需要携带DataId，否则无法通过索引进行body数据查找
//最次方案举例：
//member=openid score=updatetime
//通过openid找到的是数据对应的updatetime，这种情况下，最好再加一个索引如下
//member=openid score=id
//这样一来就可以通过索引找到body数据
type RedisUniqueIndex struct {
	identification       string  //索引名称
	keyPattern           string   //主键有序列表keyname 一般情况存储Member=ID Score=ID Member需要具有唯一属性
	zMemberUse           []string //构建主键列表的member使用哪些字段
	zScoreUse            []string //构建主键列表的socre使用哪些字段
	MutilScoreMerge      func(a[]float64) (float64,error)
	fieldConverToString  map[string]_field.ConverToString
	memberJoinSymbol     string //多个成员连接符
	patterMfield         string //mfield字段值
	patterSfield         string //sfiled字段
}




func NewRedisUniqueIndex( identification string ) *RedisUniqueIndex {
	var unique RedisUniqueIndex
	unique.identification   = identification
	unique.memberJoinSymbol = "|"
	return &unique
}







type MemberScore struct {
	Member  string
	Score   string
}

//menber_field=key1,key2,key3... score_field=value1,value2,value3
//对象的member值=多个字段值拼接，score值需要调用一个函数进行处理
func (this *RedisUniqueIndex) SetMemberZUseField( m []MemberScore ) {
	for i:=0; i<len(m); i++ {
		this.zMemberUse = append(this.zMemberUse,m[i].Member)
		this.zScoreUse  = append(this.zScoreUse,m[i].Score)
	}
	if len(m) == 1 {
		this.MutilScoreMerge = defaultOneScoreMerge
	}
}

func (this *RedisUniqueIndex) SetFieldConverToString(a map[string]_field.ConverToString) { this.fieldConverToString = a }

func (this *RedisUniqueIndex) SetMemberJoinSymbol(a string) { this.memberJoinSymbol=a }

func (this *RedisUniqueIndex) GetMemberJoinSymbol() string { return this.memberJoinSymbol }





//构建索引
func (this *RedisUniqueIndex) BuildZMember( val map[string]interface{} ) (FormatString,redis.Z,error) {
	var (
		err            error
		memberz        redis.Z
		members        []string
		scores         []float64
		getMember      string
		getScore       string
		parseFloat     float64
	)
	mfieldLen := len(this.zMemberUse)
	for i:=0; i<mfieldLen; i++ {
		getMember,err = this.fieldConverToString[this.zMemberUse[i]](val[this.zMemberUse[i]])
		if nil != err {
			return FormatString(""),memberz,err
		}
		getScore,err  = this.fieldConverToString[this.zScoreUse[i]](val[this.zScoreUse[i]])
		if nil != err {
			return FormatString(""),memberz,err
		}
		parseFloat,err = strconv.ParseFloat(getScore,64)
		if nil != err {
			return FormatString(""),memberz,err
		}
		members = append(members,getMember)
		scores  = append(scores,parseFloat)
	}
	memberz.Score,err = this.MutilScoreMerge(scores)
	if nil != err {
		return FormatString(""),memberz,err
	}
	memberz.Member = strings.Join(members,this.memberJoinSymbol)

	if memberz.Member == "" {
		return FormatString(""),memberz,errors.New("empty member")
	}
	return FormatString(this.keyPattern),memberz,nil
}








//比特类型索引
//存在的问题，redis不支持将一个bit数据集完整取出，也不支持取某一段
//并且数据删除后，再用bit进行数据统计会出错
//比如[1,1,1,1,1,0,0,0,0,0]，1男0女 id=3取值为1性别为男，但id=3这条数据删除后，难道把它变成女的？
//这种情况下统计男女分别有多少人会出错
//这种索引可能适合不需要用来统计的场景
type RedisBitIndex struct{
	identification     string  //索引名称
	keyPattern         string //索引键名
	idFieldName        string //ID字段名称
	boolFieldName      []string //多个字段值参与表达式运算得出真或假
	patterField        string //拼接key的field字段值
	ExprReal           func(a UserExprAcess) (bool,error)
}



func NewRedisBitIndex( identification string ) RedisBitIndex {
	var bitindex RedisBitIndex
	bitindex.identification = identification
	return bitindex
}


func (this *RedisBitIndex) SetIdFieldName(a string) { this.idFieldName=a }

func (this *RedisBitIndex) SetUseField( boole []string ) { this.boolFieldName=boole }
//表达式可以外部访问无需Set



func (this *RedisBitIndex) BuildOffsetWithBool( val map[string]interface{} ) (FormatString,int64,int,error) {
	key    := this.keyPattern
	offset := val[this.idFieldName].(uint64)
	//结果是真是假也可以是多个值执行表达式的结果
	boole,err := this.ExprReal(UserExprAcess{val,this.boolFieldName})
	if nil != err {
		return FormatString(""),0,0,err
	}
	var boole_ret int
	if boole { boole_ret = 1 } else {
		boole_ret = 0
	}
	return FormatString(key),int64(offset),boole_ret,nil
}



//主要用来防止自己写错代码，避免自己访问自己未设置的字段
type UserExprAcess struct{
	mp   map[string]interface{}
	k    []string
}

//强制访问无限制
func (this *UserExprAcess) ForceGet( key string ) (interface{},bool) {
	interval,ok := this.mp[key]
	return interval,ok
}

func (this *UserExprAcess) Get( key string ) interface{} {
	key_in := false
	klen := len(this.k)
	for i:=0; i<klen; i++ {
		if this.k[i] == key {
			key_in = true
			break
		}
	}
	if false == key_in {
		panic("cannot access oterfield")
	}
	//检查指定字段是否存在
	result,exists := this.mp[key]
	if false == exists {
		panic("field not exists")
	}
	return result
}








//集合类型索引
type RedisSetIndex struct{
	identification     string  //索引名称
	keyPattern         string
	classPattern       string
	classNameUse       []string//匹配哪些字段 多个字段参与表达式得出该数据应属于哪个集合
	idFieldName        string
	patternField       string
	ExprClassName      func(v UserExprAcess) (string,error)//检查是否满足集合条件
}


//keyPattern可以理解为命名规则
func NewRedisSetIndex( identification string ) RedisSetIndex {
	var setindex RedisSetIndex
	setindex.identification = identification
	return setindex
}


func (this *RedisSetIndex) SetClassNameUseField( field []string ) { this.classNameUse = field }

func (this *RedisSetIndex) SetIdFieldName(a string){ this.idFieldName=a }



//如果是多个字段值参与表达式计算出分类名称，那么修改的时候也必须完整提供这两个字段值
func (this *RedisSetIndex) BuildSmember( val map[string]interface{} ) (FormatString,string,uint64,error) {
	var (
		err       error
		member    uint64
		classname string
		formatKey FormatString
	)
	//分类名称
	classname,err = this.ExprClassName(UserExprAcess{val,this.classNameUse})
	if nil != err {
		return formatKey,classname,member,err
	}

	//组合命名规则第一个占位符需要是集合的分类名称
	formatKey = FormatString(fmt.Sprintf(this.keyPattern,classname))
	member    = val[this.idFieldName].(uint64)
	if member <= 0 {
		return formatKey,classname,member,errors.New("invalid memberId")
	}

	return formatKey,classname,member,err
}

