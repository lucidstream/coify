package coify

import (
	"github.com/go-ini/ini"
)

const (
	CONFIG_FILE_PATH = "./config.ini"
)

var (
	ConfigReader *ini.File
)



func InitialConfigWithFileName( filename string ) error {
	var err error
	if filename == "" || filename == "default" {
		filename = CONFIG_FILE_PATH
	}
	ConfigReader,err = ini.Load(filename)
	if nil != err {
		return err
	}
	return nil
}
