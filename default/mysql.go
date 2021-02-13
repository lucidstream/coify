package _default

import (
	"errors"
	"github.com/lucidStream/coify"
)






var (
	SQLWriter  *coify.CoifyMysql
	SQLReader  *coify.CoifyMysql
)



func SetDefaultReader( client *coify.CoifyMysql ) error {
	if nil == client {
		return errors.New("invalid client pointer")
	}
	SQLReader = client
	return nil
}





func SetDefaultWriter( client *coify.CoifyMysql ) error {
	if nil == client {
		return errors.New("invalid client pointer")
	}
	SQLWriter = client
	return nil
}