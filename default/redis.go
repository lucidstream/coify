package _default

import (
	"errors"
	"github.com/lucidStream/coify/redis"
)



var (
	//默认实例
	//无需添加到分布式算法中
	Cache   *redis.CacheCoifyRedis
	Sync    *redis.SyncCoifyRedis
	Relvc   *redis.RelvanceRedis
)




func SetDefaultCache( client *redis.CacheCoifyRedis ) error {
	if nil == client {
		return errors.New("invalid client pointer")
	}
	Cache = client
	return nil
}



func SetDefaultSync( client *redis.SyncCoifyRedis ) error {
	if nil == client {
		return errors.New("invalid client pointer")
	}
	Sync = client
	return nil
}



func SetDefaultRelvc( client *redis.RelvanceRedis ) error {
	if nil == client {
		return errors.New("invalid client pointer")
	}
	Relvc = client
	return nil
}
