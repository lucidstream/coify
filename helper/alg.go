package helper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"strings"
)

func StringArrayIn( who string,wherein []string ) bool {
	_len := len(wherein)
	for i:=0; i<_len; i++ {
		if wherein[i] == who {
			return true
		}
	}
	return false
}


func Uint64ArrayIn( who uint64,wherein []uint64 ) bool {
	_len := len(wherein)
	for i:=0; i<_len; i++ {
		if wherein[i] == who {
			return true
		}
	}
	return false
}


func Int64ArrayIn( who int64,wherein []int64 ) bool {
	_len := len(wherein)
	for i:=0; i<_len; i++ {
		if wherein[i] == who {
			return true
		}
	}
	return false
}


func Float64ArrayIn( who float64,wherein []float64,retain float64 ) bool {
	whor := int64(who*retain)
	_len := len(wherein)
	var i64where = make([]int64,_len)
	for i:=0; i<_len; i++ {
		i64where[i] = int64(wherein[i]*retain)
	}
	return Int64ArrayIn(whor,i64where)
}




func ParamaterRequire( args *fasthttp.Args,require []string ) error {
	var requireLen = len(require)
	for i:=0; i<requireLen; i++ {
		if len(args.Peek(require[i])) == 0 {
			return errors.New("require "+require[i])
		}
	}
	return nil
}



func PeekSetSame( args *fasthttp.Args,sameSet map[string]interface{},key string ) {
	sameSet[key] = string(args.Peek(key))
}



func NotContainerRemove( args *fasthttp.Args,value map[string]interface{} ) {
	var key string
	for key,_ = range value {
		if args.Has(key) || args.Has(key+"[]") {
			continue
		}
		delete(value,key)
	}
}



func FormSetSame( ctx *fasthttp.RequestCtx,sameSet map[string]interface{},key string ) {
	sameSet[key] = string(ctx.FormValue(key))
}


func NotContainerFormValue( ctx *fasthttp.RequestCtx,value map[string]interface{} ) error {
	form,err := ctx.MultipartForm()
	if nil != err { return err }
	var (key string;exists bool)
	for key,_ = range value {
		//文件字段名 和 值（路径）字段名需要一致
		if _,exists = form.Value[key]; exists {
			continue
		}
		if _,exists = form.File[key]; exists {
			continue
		}
		delete(value,key)
	}
	return nil
}



func PeekMultiArrayString( args *fasthttp.Args,to *[]string,key string )  {
	m := args.PeekMulti(key)
	var mLen = len(m)
	for i:=0; i<mLen; i++ {
		(*to) = append((*to),string(m[i]))
	}
}




func JsonFormatOutput( inter interface{},out *bytes.Buffer ) error {
	xxx,err := json.Marshal(inter)
	if nil != err {
		return err
	}
	err = json.Indent(out, xxx, "", "    ")
	if nil != err {
		return err
	}
	return nil
}




func UnicodeZh( s string ) string {
	var buf strings.Builder
	var rrr = []rune(s)
	for i:=0; i<len(rrr); i++ {
		buf.WriteString(fmt.Sprintf("\\U%x",rrr[i]))
	}
	return buf.String()
}