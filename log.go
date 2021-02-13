package coify

import (
	"bufio"
	"errors"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)



func SetLogsLevel( level string ) error {
	switch level {
	case "panic":
		log.SetLevel(log.PanicLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "trace":
		log.SetLevel(log.TraceLevel)
	default:
		return errors.New("unknown logs level")
	}
	return nil
}






func logFormart (f *runtime.Frame) (string, string) {
	s := strings.Split(f.Function, ".")
	funcname    := s[len(s)-1]
	_, filename := path.Split(f.File)
	filename += ":"+strconv.FormatInt(int64(f.Line),10)
	return funcname, filename
}





//重定向标准错误到日志文件
func DupStdErr( err_file string ) error {
	flag := os.O_RDWR|os.O_CREATE|os.O_APPEND
	stderr_file,err := os.OpenFile(err_file,flag,0666)
	if nil != err {
		return err
	}
	err = syscall.Dup2(int(stderr_file.Fd()), 2)
	if nil != err {
		return err
	}
	return nil
}





//更新进程pid
func UpdatePid( filename string ) error {
	if filename == "" {
		exec_paths := strings.Split(os.Args[0],"/")
		filename = "./"+exec_paths[len(exec_paths)-1]+".pid"
	}
	pidfile,err := os.OpenFile(filename,os.O_RDWR|os.O_CREATE,0666)
	if err != nil {
		return err
	}
	if err := pidfile.Truncate(0);nil != err {
		return err
	}
	_,err = pidfile.WriteAt([]byte(strconv.Itoa(os.Getpid())),0)
	if err != nil {
		return err
	}
	if err = pidfile.Close();nil != err {
		return err
	}
	return nil
}





func InitialLogWithConfig( config map[string]string ) error {
	if err := SetLogsLevel(config["level"]);nil != err {
		return err
	}
	log.SetReportCaller(true)
	//set record line
	formatter := log.StandardLogger().Formatter.(*log.TextFormatter)
	formatter.CallerPrettyfier = logFormart

	switch config["type"] {
	case "file":
		flag := os.O_RDWR|os.O_CREATE|os.O_APPEND
		logfile,err := os.OpenFile(config["stdout_file"],flag,0666)
		if nil != err {
			return err
		}
		//设置的日志等级小于Info才输出到日志文件
		//否则Info以及以上等级的日志打印到屏幕
		if log.GetLevel() < log.InfoLevel {
			logwriter := bufio.NewWriter(logfile)
			log.SetOutput(logwriter)
		}
	case "remote":
		panic("unrealized")
	default:
		return errors.New("unknown log type")
	}

	//标准错误日志
	//当前日志等级至少为Info才将标准错误输出到日志文件
	if config["stderr_file"] != "" && log.GetLevel() < log.DebugLevel {
		err := DupStdErr(config["stderr_file"])
		if nil != err {
			return err
		}
	}
	
	//记录pid
	if err := UpdatePid(config["pidfile"]); nil != err {
		return err
	}
	return nil
}
