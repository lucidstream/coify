package rorm

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	coifyRedis "github.com/lucidStream/coify/redis"
	_field "github.com/lucidStream/coify/rorm/field"
	"github.com/modern-go/reflect2"
	"reflect"
	"regexp"
	"strings"
	"unsafe"
)

//使用redis作为主数据源时，需要一个结构规范键值对命名，索引等
//简而言之，需要使用此模型大致模仿做到使用mysql的效果
//本模型只提供写操作的封装，读操作仅提供数据关系辅助，不作具体实现
//因为读数据需求太复杂了，八仙过海各显神通


type FormatString string


type UserClientOptions struct{
	Client      *coifyRedis.RelvanceRedis //用于实现写入必须的redis操作
	Pipeline    *redis.Pipeliner //用户压入需要写入的数据
}



//传入的val是经过填充的
//frosty 变幻前的内容
//extra 扩展访问内容，由其他模型调用传入，仅支持一层扩展访问
type BeforFunc = func(cli UserClientOptions,key *KwPatter,val,frosty,extra map[string]interface{}) error
type AfterFunc = func(cli UserClientOptions,key *KwPatter,val,frosty,extra map[string]interface{}) error




type RedisOrm struct {
	_field.RormField
	tableName           string  //相关的数据键都会加上此前缀
	pattern             string  //data keyname 单个数据对象的key
	jsonFormats         []string//json类型字段名
	arrayFormats        []string//数组类型字段名
	xmlFormats          []string//xml类型字段名
	//操作之前之后
	InsertBefor         BeforFunc
	InsertAfter         AfterFunc
	UpdateBefor         BeforFunc
	UpdateAfter         AfterFunc
	DeleteBefor         BeforFunc
	DeleteAfter         AfterFunc
	//索引
	uniqueIndexList     map[string]*RedisUniqueIndex //唯一索引列表
	bitIndexList        map[string]*RedisBitIndex    //比特索引列表
	setIndexList        map[string]*RedisSetIndex    //集合索引列表
}




var (
	//对象绑定的ORM映射
	typeBindOrm = make(map[string]*RedisOrm)
)





//根据传入类型获取类型完整映射名
func GetTypeName( objc interface{} ) (string,error) {
	typeVal   := reflect2.TypeOf(objc).(reflect2.StructType)
	key := typeVal.String()
	if key == "" {
		return "",errors.New("empty type name")
	}
	return key,nil
}




//关联的ORM模型
func GetRelationOrm( name string ) (*RedisOrm,error) {
	orm,ok := typeBindOrm[name]
	if !ok {
		errMsg := fmt.Sprintf("NoDesignated rormInstace: %s",name)
		return nil,errors.New(errMsg)
	}
	return orm,nil
}




func NewRedisOrm( tableName,idName string ) *RedisOrm {
	var orm RedisOrm
	orm.tableName       = tableName
	//table=TABLENAME:body:id=%d
	orm.pattern         = "table="+tableName+":body:"+idName+"=%d"
	orm.uniqueIndexList = make(map[string]*RedisUniqueIndex)
	orm.bitIndexList    = make(map[string]*RedisBitIndex)
	orm.setIndexList    = make(map[string]*RedisSetIndex)
	orm.IdFieldName     = idName
	return &orm
}





//设置类型映射ORM
func (this *RedisOrm) SetRelationType( objc interface{} ) error {
	key,err := GetTypeName(objc)
	if nil != err {
		return err
	}
	typeBindOrm[key] = this
	return nil
}






func (this *RedisOrm) SetExtPattern( extPattern string ) {
	this.pattern = fmt.Sprintf("%s:%s",this.pattern,extPattern)
}



//创建时间字段名
func (this *RedisOrm) SetCreateTimeFieldName( name string ) {
	this.CreateField = name
}



//更新时间字段名
func (this *RedisOrm) SetUpdateTimeFieldName( name string ) {
	this.UpdateField = name
}




//设置模型字段
func (this *RedisOrm) SetFieldList( field map[string]*_field.Setting ) {
	var (
		err error
		field_parse  = make(map[string]_field.StringParse)
		field_string = make(map[string]_field.ConverToString)
	)
	for k,v := range field {
		if nil == v.Type {
			errMsg := fmt.Sprintf("field %s mission typePair",k)
			panic(errMsg)
		}
		this.FieldList = append(this.FieldList,k)
		field_parse[k]  = v.Type[0].(_field.StringParse)
		field_string[k] = v.Type[1].(_field.ConverToString)
		//检查字段定义的正确性
		if v.Field == nil {
			errMsg := fmt.Sprintf("field %s mission reflect2 field",k)
			panic(errMsg)
		}
		switch v.Type {
		case &_field.JsonPair:
			this.jsonFormats = append(this.jsonFormats,k)
		case &_field.XMLPair:
			this.xmlFormats = append(this.xmlFormats,k)
		case &_field.ArrayPair:
			this.arrayFormats = append(this.arrayFormats,k)
		default:
		}
		err = fieldSettingCorrectness(k,v)
		if nil != err {
			panic(err)
		}
	}
	this.StringParse    = field_parse
	this.ConverToString = field_string
	this.TypeExpr       = field
}





//检查字段设置类型正确性
func fieldSettingCorrectness( k string,config *_field.Setting ) error {
	switch config.Type {
	case &_field.Uint64Pair:
		if config.Default != nil {
			_,ok := config.Default.(_field.Uint64DefaultVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Default not Uint64DefaultVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Filter != nil {
			_,ok := config.Filter.(_field.Uint64FilterVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Filter not Uint64FilterVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Verify != nil {
			_,ok := config.Verify.(_field.Uint64Validation)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Verify not Uint64Validation",k)
				return errors.New(errMsg)
			}
		}
	case &_field.Int64Pair:
		if config.Default != nil {
			_,ok := config.Default.(_field.Int64DefaultVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Default not Int64DefaultVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Filter != nil {
			_,ok := config.Filter.(_field.Int64FilterVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Filter not Int64FilterVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Verify != nil {
			_,ok := config.Verify.(_field.Int64Validation)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Verify not Int64Validation",k)
				return errors.New(errMsg)
			}
		}
	case &_field.StringPair:
		if config.Default != nil {
			_,ok := config.Default.(_field.StringDefaultVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Default not StringDefaultVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Filter != nil {
			_,ok := config.Filter.(_field.StringFilterVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Filter not StringFilterVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Verify != nil {
			_,ok := config.Verify.(_field.StringValidation)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Verify not StringValidation",k)
				return errors.New(errMsg)
			}
		}
	case &_field.Float64Pair:
		if config.Default != nil {
			_,ok := config.Default.(_field.Float64DefaultVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Default not Float64DefaultVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Filter != nil {
			_,ok := config.Filter.(_field.Float64FilterVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Filter not Float64FilterVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Verify != nil {
			_,ok := config.Verify.(_field.Float64Validation)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Verify not Float64Validation",k)
				return errors.New(errMsg)
			}
		}
	case &_field.BoolPair:
		if config.Default != nil {
			_,ok := config.Default.(_field.BoolDefaultVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Default not BoolDefaultVal",k)
				return errors.New(errMsg)
			}
		}
		//一个布尔值还需要过滤什么？
		if config.Verify != nil {
			_,ok := config.Verify.(_field.BoolValidation)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Verify not BoolValidation",k)
				return errors.New(errMsg)
			}
		}
	case &_field.ArrayPair:
		//如果array里面放的是另一个模型，那么在数据过滤时不仅要符合对应模型的规则，还要符合本模型的规则
		if config.Default != nil {
			_,ok := config.Default.(_field.ArrayDefaultVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Default not ArrayDefaultVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Filter != nil {
			_,ok := config.Filter.(_field.ArrayFilterVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Filter not ArrayFilterVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Verify != nil {
			_,ok := config.Verify.(_field.ArrayValidation)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Verify not ArrayValidation",k)
				return errors.New(errMsg)
			}
		}
	case &_field.JsonPair:
		if config.Default != nil {
			_,ok := config.Default.(_field.JsonDefaultVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Default not JsonDefaultVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Filter != nil {
			_,ok := config.Filter.(_field.JsonFilterVal)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Filter not JsonFilterVal",k)
				return errors.New(errMsg)
			}
		}
		if config.Verify != nil {
			_,ok := config.Verify.(_field.JsonValidation)
			if false == ok {
				errMsg := fmt.Sprintf("field %s.Verify not JsonValidation",k)
				return errors.New(errMsg)
			}
		}
	default:
		errMsg := fmt.Sprintf("field %s unsupport typePair",k)
		return errors.New(errMsg)
	}
	return nil
}






type IndexType string

const (
	Unique = IndexType("unique")
	Set    = IndexType("set")
	Bit    = IndexType("bit")
)




type UniqueQuery struct {
	index         *RedisUniqueIndex
	table_name    string
	kind          string
}

func (this *UniqueQuery) GetIndex() *RedisUniqueIndex { return this.index }

func (this *UniqueQuery) GetTableName() string { return this.table_name }

func (this *UniqueQuery) GetType() string { return this.kind }

func (this *UniqueQuery) GetIndexName() string { return this.index.identification }

func (this *UniqueQuery) GetMfield() string { return this.index.patterMfield }

func (this *UniqueQuery) GetSfield() string { return this.index.patterSfield }

func (this *UniqueQuery) ZsetKey() string { return this.index.keyPattern }

func (this *RedisOrm) QueryUnique( indexName string ) (uh UniqueQuery,err error) {
	index := this.uniqueIndexList[indexName]
	if nil == index {
		return uh,errors.New("non unique index:"+indexName)
	}
	uh.index      = index
	uh.table_name = this.tableName
	uh.kind       = string(Unique)
	return uh,nil
}



type SetQuery struct {
	index         *RedisSetIndex
	table_name    string
	kind          string
}

func (this *SetQuery) GetTableName() string { return this.table_name }

func (this *SetQuery) GetType() string { return this.kind }

func (this *SetQuery) GetIndexName() string { return this.index.identification }

func (this *SetQuery) GetField() string { return this.index.patternField }

//需传入表达式计算分类所需的值
func (this *SetQuery) ClassFormat( exprVal map[string]interface{} ) (string,error) {
	//分类名称
	classname,err := this.index.ExprClassName(UserExprAcess{exprVal,this.index.classNameUse})
	if nil != err {
		return "",err
	}
	return fmt.Sprintf(this.index.keyPattern,classname),nil
}


func (this *RedisOrm) QuerySet( indexName string ) (sh SetQuery,err error) {
	index := this.setIndexList[indexName]
	if nil == index {
		return sh,errors.New("non set index:"+indexName)
	}
	sh.index      = index
	sh.table_name = this.tableName
	sh.kind       = string(Set)
	return sh,nil
}



type BitQuery struct {
	index         *RedisBitIndex
	table_name    string
	kind          string
}

func (this *BitQuery) GetTableName() string { return this.table_name }

func (this *BitQuery) GetType() string { return this.kind }

func (this *BitQuery) GetIndexName() string { return this.index.identification }

func (this *BitQuery) GetField() string { return this.index.patterField }

func (this *RedisOrm) QueryBit( indexName string ) (bh BitQuery,err error) {
	index := this.bitIndexList[indexName]
	if nil == index {
		return bh,errors.New("non bit index:"+indexName)
	}
	bh.index      = index
	bh.table_name = this.tableName
	bh.kind       = string(Bit)
	//bh.index_name = index.identification
	//bh.field      = index.patterField
	return bh,nil
}



func (this *RedisOrm) AddUniqueIndex( index *RedisUniqueIndex ) error {
	if index.identification == "" {
		return errors.New("identification empty")
	}

	_,ok := this.uniqueIndexList[index.identification]
	if ok == true {
		return errors.New("identification already exist")
	}
	index.SetFieldConverToString(this.ConverToString)
	index.patterMfield = strings.Join(index.zMemberUse,"+")
	index.patterSfield = strings.Join(index.zScoreUse,"+")

	lay := fmt.Sprintf(string(UniqueIndexPattern),
		this.tableName,
		Unique,
		index.identification,
		index.patterMfield,
		index.patterSfield,
		)
	//不能把索引名称重命名了，查找的时候还需要更新名字查找索引对象
	//index.identification = lay
	index.keyPattern = lay
	this.uniqueIndexList[index.identification] = index
	return nil
}



func (this *RedisOrm) AddBitIndex( index *RedisBitIndex ) error {
	if index.identification == "" {
		return errors.New("identification empty")
	}

	_,ok := this.bitIndexList[index.identification]
	if ok == true {
		return errors.New("identification already exist")
	}
	index.patterField = strings.Join(index.boolFieldName,"+")

	lay := fmt.Sprintf(string(BitIndexPattern),
		this.tableName,
		Bit,
		index.identification,
		index.patterField,
	)
	index.keyPattern = lay
	index.SetIdFieldName(this.IdFieldName)
	this.bitIndexList[index.identification] = index
	return nil
}



func (this *RedisOrm) AddSetIndex( index *RedisSetIndex ) error {
	if index.identification == "" {
		return errors.New("identification empty")
	}

	_,ok := this.setIndexList[index.identification]
	if ok == true {
		return errors.New("identification already exist")
	}
	index.patternField = strings.Join(index.classNameUse,"+")

	lay := fmt.Sprintf(strings.TrimRight(string(SetIndexPattern),"%s"),
		this.tableName,
		Set,
		index.identification,
		index.patternField,
	)
	lay += "%s"

	classp := fmt.Sprintf(string(SetClassPattern),
		this.tableName,
		Set,
		index.identification,
	)

	index.classPattern = classp
	index.keyPattern   = lay
	index.SetIdFieldName(this.IdFieldName)
	this.setIndexList[index.identification] = index
	return nil
}




//键名拼接组合对象
type KwPatter struct{
	key         []interface{}
	pattern     *string
	serialize   FormatString
}


//初始化后不允许修改成员key
func newKeyWithPattern( key []interface{},pattern *string ) KwPatter {
	var kpattern KwPatter
	kpattern.key     = key
	kpattern.pattern = pattern
	return kpattern
}


func (this *KwPatter) Formart() FormatString {
	if this.serialize == "" {
		this.serialize = FormatString(fmt.Sprintf(*this.pattern,this.key...))
	}
	return this.serialize
}


func (this *KwPatter) GetKey() []interface{} { return this.key }


//model需要绑定一个客户端后才可进行操作
type RedisOrmWithClient interface {
	SavePattern() error
}

type WriteOrmWithClient struct {
	RedisOrmWithClient
	model    *RedisOrm
	client   *coifyRedis.RelvanceRedis
}


type ReadOrmWithClient struct {
	RedisOrmWithClient
	model    *RedisOrm
	client   *coifyRedis.RelvanceRedis
}



func NewWriteOrmWithClient( orm *RedisOrm,client *coifyRedis.RelvanceRedis ) WriteOrmWithClient {
	var op WriteOrmWithClient
	op.model  = orm
	op.client = client
	return op
}


func (this *WriteOrmWithClient) ToReader() *ReadOrmWithClient { return (*ReadOrmWithClient)(this) }
func (this *WriteOrmWithClient) Client() *coifyRedis.RelvanceRedis { return this.client }


func NewReadOrmWithClient( orm *RedisOrm,client *coifyRedis.RelvanceRedis ) ReadOrmWithClient {
	if nil == orm {
		panic("nil pointer: orm")
	}
	var op ReadOrmWithClient
	op.model  = orm
	op.client = client
	return op
}


func (this *ReadOrmWithClient) ToWriter() *WriteOrmWithClient { return (*WriteOrmWithClient)(this) }
func (this *ReadOrmWithClient) Client() *coifyRedis.RelvanceRedis { return this.client }



//存储数据对象、各种索引的命名规则到redis
//redis即可通过lua脚本进行使用
func (this *WriteOrmWithClient) SavePattern() error { return savePattern(this.model,this.client) }
func (this *ReadOrmWithClient) SavePattern() error { return savePattern(this.model,this.client) }




func savePattern( model *RedisOrm,client *coifyRedis.RelvanceRedis ) error {
	rule := make(map[string]interface{})
	rule["body"] = model.pattern
	for k,v := range model.uniqueIndexList {
		rule["unique="+k] = v.keyPattern
	}
	for k,v := range model.bitIndexList {
		rule["bit="+k] = v.keyPattern
	}
	for k,v := range model.setIndexList {
		rule["set="+k]   = v.keyPattern
		rule["ident="+k] = v.classPattern
	}
	layout := fmt.Sprintf("table=%s:pattern",model.tableName)
	return client.HMSet(layout,rule).Err()
}





func (this *WriteOrmWithClient) GetAutoIncId() (uint64,error) {
	key := fmt.Sprintf("table=%s:autoincre_id",this.model.tableName)
	newid,err := this.client.Incr(key).Result()
	if nil != err {
		return 0,err
	}
	return uint64(newid),nil
}




//委托设置数据自增ID
func (this *WriteOrmWithClient) SetAutoIncId( val map[string]interface{} ) (uint64,error) {
	newid,err := this.GetAutoIncId()
	if nil != err {
		return 0,err
	}
	val[this.model.IdFieldName] = newid
	return newid,nil
}






var (
	localBasicTypes = []string{
		"string",
		"float64",
		"float32",
		"int",
		"uint",
		"int32",
		"uint32",
		"int64",
		"uint64",
		"int16",
		"uint16",
		"int8",
		"uint8",
		"bool",
	}
)




func isBisicArray( typeName string ) bool {
	for i:=0; i<len(localBasicTypes); i++ {
		regStr := fmt.Sprintf(`^(?:\[\d*\])+%s$`,localBasicTypes[i])
		regex := regexp.MustCompile(regStr)
		if regex.MatchString(typeName) {
			return true
		}
	}
	return false
}



func stripArrayBrackets( typeName string ) string {
	regex := regexp.MustCompile(`(?:\[\d*\])`)
	return regex.ReplaceAllString(typeName,"")
}




func stripPointerBrackets ( typeName string ) string {
	if string(typeName[0]) == "*" {
		return string(typeName[1:])
	}
	return typeName
}



func GetFieldSettings( objc interface{} ) (_field.SettingExpr,[]string) {
	var(
		err error
		settings  = make(_field.SettingExpr)
		valueOf   = reflect.ValueOf(objc)
		typeOf    = reflect.TypeOf(objc)
		typeOf2   = reflect2.TypeOf(objc)
		typeVal   = typeOf2.(reflect2.StructType)
		fieldNum  = valueOf.NumField()
	)
	jsonTags := make(map[string]struct{})
	for i:=0; i<fieldNum; i++ {
		tagIns    := typeOf.Field(i).Tag
		jsonTags[strings.Trim(tagIns.Get("json")," ")] = struct{}{}
	}
	for i:=0; i<fieldNum; i++ {
		ref2Field := typeVal.Field(i)
		//在go编程中一般对象字段名称不会带下划线之类的
		methodName := fmt.Sprintf("%sSetting",ref2Field.Name())
		relMethod := valueOf.MethodByName(methodName)
		relMethod2 := (*struct {
			typ uintptr
			ptr unsafe.Pointer
		})(unsafe.Pointer(&relMethod))
		if relMethod2.ptr==nil {
			errMsg := fmt.Sprintf("Undefined %sSetting()",ref2Field.Name())
			panic(errMsg)
		}
		exprInter := relMethod.Call(nil)[0].Interface().(_field.Setting)
		tagIns    := typeOf.Field(i).Tag
		tagName   := strings.Trim(tagIns.Get("json")," ")
		if tagName == "" {
			errMsg := fmt.Sprintf("%s not set tagName: json",ref2Field.Name())
			panic(errMsg)
		}
		exprInter.Name     = tagName
		exprInter.Field    = &ref2Field
		if exprInter.Field == nil {
			panic("cannot create ref2field instance")
		}
		dataId := strings.Trim(tagIns.Get("id")," ")
		if dataId != "" && dataId != "autoinc" {
			//指定字段作为子模型主键，检查字段是否存在
			if _,ok := jsonTags[dataId]; !ok {
				errMsg := fmt.Sprintf("dataId field not exists: %s",dataId)
				panic(errMsg)
			}
		}
		exprInter.DataId = dataId
		switch exprInter.Type {
		case &_field.JsonPair,&_field.XMLPair:
			//检查该对象关联的模型，数据默认值，过滤，验证等使用另外一个模型处理
			itypeName := stripPointerBrackets(ref2Field.Type().String())
			//json对象必须关联模型
			exprInter.Rorm,err = GetRelationOrm(itypeName)
			if nil != err {
				panic(err)
			}
		case &_field.ArrayPair:
			//一对多模型不允许使用本模型字段值作为主键
			if dataId != "" && dataId != "autoinc" {
				panic("oneToMany model notAllow current field")
			}
			//什么是强类型？尼玛搞个数组里面什么类型都有，这没办法写啊
			//除基本数据类型和多维数组外，都需要指定模型处理
			itypeName := stripPointerBrackets(ref2Field.Type().String())
			itypeName2 := stripArrayBrackets(itypeName)
			if !isBisicArray(itypeName) {
				exprInter.Rorm,err = GetRelationOrm(itypeName2)
				if nil != err {
					panic(err)
				}
			} else {
				//不允许使用基础数据类型数组，因为无法建立模型进行验证
				panic("notAllowed using basicArray")
			}
		default:
		}
		settings[tagName]  = &exprInter
	}
	fields := make([]string,0,len(settings))
	for k,_ := range settings { fields = append(fields,k) }
	return settings , fields
}












