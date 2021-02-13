package coify

import (
	"database/sql"
	"fmt"
	"github.com/go-ini/ini"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"time"
)


//本框架的设计思想是mysql的数据基本从redis同步而来
//mysql尽量只用作查询用途，用于解决nosql难以书写的复杂查询


type DatabaseOption struct {
	Host           string `ini:"host"`
	Port           int    `ini:"port"`
	User           string `ini:"user"`
	Pass           string `ini:"pass"`
	Dbname         string `ini:"dbname"`
	Charset        string `ini:"charset"`
	MaxOpenConn    int    `ini:"max_open_conn"`
	MaxIdleConn    int    `ini:"max_idle_conn"`
	MaxLifeTime    time.Duration `ini:"max_life_time"`
}



type CoifyMysql struct {
	*sql.DB
	Option        DatabaseOption
	Name          string
}



type fwith_default_value struct {
	fieldName         string
	hasDefault        bool
	defaultValue      string
}



type SqlFields struct {
	Need          []string //必须是手动指定的字段，按参照表排序
	Sequence      []fwith_default_value //完整字段序列
	NeedJoin      map[string]string
	SequenceJoin  map[string]string
}




func array_in( who string,whereIn []string ) bool {
	for i:=0; i<len(whereIn); i++ {
		if whereIn[i] == who {
			return true
		}
	}
	return false
}



//source 所有字段和默认值
//need   需要用到的字段，==nil时，表示需要所有的字段
func NewSqlFields(source []string,need []string) (f SqlFields) {
	f.Need     = make([]string,0,20)
	f.Sequence = make([]fwith_default_value,0,20)
	var hasNeed bool
	var fwith_default fwith_default_value

	for i:=0; i < len(source); i+=2 {
		if nil == need || len(need) == 0 {
			hasNeed = true
		} else {
			hasNeed = array_in(source[i],need)
		}
		if hasNeed {
			f.Need = append(f.Need,source[i])
		}
		if nil == need || len(need) == 0 || hasNeed {
			//需要的字段
			fwith_default.fieldName     = source[i]
			fwith_default.hasDefault    = false
			fwith_default.defaultValue  = ""
		} else {
			//不需要的字段
			fwith_default.fieldName    = source[i]
			fwith_default.hasDefault   = true
			fwith_default.defaultValue = source[i+1]
		}
		f.Sequence = append(f.Sequence,fwith_default)
	}

	var tmptrim string
	f.NeedJoin      = make(map[string]string)
	var f_need      = make([]string,0,len(f.Need))
	for i:=0; i<len(f.Need); i++ {
		tmptrim = strings.Trim(f.Need[i],"`")
		tmptrim = "`"+tmptrim+"`"
		f_need = append(f_need,tmptrim)
	}
	f.NeedJoin[","] = strings.Join(f_need,",")
	tmptrim = ""

	//不需要用的字段，需用默认值代替
	f.SequenceJoin  = make(map[string]string)
	var f_sequence  = make([]string,0,len(f.Sequence))
	for i:=0; i<len(f.Sequence); i++ {
		tmptrim = strings.Trim(f.Sequence[i].fieldName,"`")
		tmptrim = "`"+tmptrim+"`"
		if f.Sequence[i].hasDefault {
			tmptrim = `"`+f.Sequence[i].defaultValue+`" as `+tmptrim
		}
		f_sequence = append(f_sequence,tmptrim)
	}
	f.SequenceJoin[","] = strings.Join(f_sequence,",")
	return
}






func NewCoifySQL( section *ini.Section,session map[string]string ) (*CoifyMysql,error) {
	var (
		err     error
		client  CoifyMysql
		options DatabaseOption
	)
	client.Name = strings.Trim(section.Name()," ")
	if err = section.MapTo(&options); nil != err {
		return nil,err
	}
	client.Option = options
	dsn := joindsn(&client.Option,session)
	client.DB,err = sql.Open("mysql",dsn)
	if nil != err {
		return nil,err
	}
	client.DB.SetMaxOpenConns(client.Option.MaxOpenConn)
	client.DB.SetMaxIdleConns(client.Option.MaxIdleConn)
	client.DB.SetConnMaxLifetime(client.Option.MaxLifeTime)
	err = client.DB.Ping()
	return &client,err
}





func joindsn( opts *DatabaseOption,session map[string]string ) string {
	const(
		layout = `%s:%s@tcp(%s:%d)/%s?charset=%s&collation=%s_general_ci&maxAllowedPacket=0`
	)
	dsn := fmt.Sprintf(layout,opts.User,opts.Pass,opts.Host,opts.Port,
		opts.Dbname,opts.Charset,opts.Charset)
	if nil != session {
		for k,v := range session {
			dsn += "&"+k+"="+v
		}
	}
	return dsn
}





