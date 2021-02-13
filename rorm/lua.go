package rorm

import (
	coifyRedis "github.com/lucidStream/coify/redis"
)


//language=LUA
var deleteTiggerClassName = coifyRedis.LuaScript(`
	local table        = KEYS[1];
	local indexName    = KEYS[2];
	local field        = KEYS[3];
	local className    = KEYS[4];
	local member       = KEYS[5];
	local setPattern  = "table=%s:type=set:index=%s:field=%s:class=%s";
	local set_key = string.format(setPattern,table,indexName,field,className);
	--将成员从分类中移除
	redis.call("srem",set_key,member);
	--检查集合是否还存在
	local check_exists = redis.call("exists",set_key);
	if check_exists==0 then
		--从分类列表中移除当前分类
		local classNameListPatter = "table=%s:type=set:index=%s:classlist";
		local classNameListKey = string.format(classNameListPatter,table,indexName);
		redis.call("srem",classNameListKey,className);
	end
	return "OK";
`)


//language=LUA
var smoveTiggerClassName = coifyRedis.LuaScript(`
	local table        = KEYS[1];
	local indexName    = KEYS[2];
	local field        = KEYS[3];
	local oldClassName = KEYS[4];
	local newClassName = KEYS[5];
	local member       = KEYS[6];
	local setPattern  = "table=%s:type=set:index=%s:field=%s:class=%s";
	
	if oldClassName == newClassName then
		return "OK";
	end
	
	local old_set_key = string.format(setPattern,table,indexName,field,oldClassName);
	local new_set_key = string.format(setPattern,table,indexName,field,newClassName);
	--移动数据所属分类
	redis.call("smove",old_set_key,new_set_key,member);
	--检查旧集合是否还存在
	local check_exists = redis.call("exists",old_set_key);
	if check_exists==0 then
		--从分类列表中移除当前分类
		local classNameListPatter = "table=%s:type=set:index=%s:classlist";
		local classNameListKey = string.format(classNameListPatter,table,indexName);
		redis.call("srem",classNameListKey,oldClassName);
	end
	return "OK";
`)



//language=LUA
var memberExistsCheck = coifyRedis.LuaScript(`
	local tableName  = KEYS[1];
	local indexName,mfield,sfield;
	local member;
	local keyPattern = "table=%s:type=unique:index=%s:mfield=%s:sfield=%s";
	local uniqueKey;
	local checkScore;
	local existsReult = {};
	local index = 1;
	repeat
		indexName  = ARGV[index];
		mfield     = ARGV[index+1];
		sfield     = ARGV[index+2];
		member     = ARGV[index+3];
		uniqueKey  = string.format(keyPattern,tableName,indexName,mfield,sfield);
		checkScore = redis.call("zscore",uniqueKey,member);
		if checkScore ~= false then
			existsReult[1] = indexName;
			existsReult[2] = member;
			existsReult[3] = checkScore;
			return existsReult;
		end
		index = index+4;
	until(index>#ARGV)
	return existsReult;
`)



//language=LUA
var mutilBodyExistsCheck = coifyRedis.LuaScript(`
	local bodyPattern = KEYS[1];
	local groupLen    = tonumber(KEYS[2]);
	local formatKey,exists;
	local result = {};
	local i = 1;
	repeat
		--最多支持4个扩展规则
		formatKey = string.format(bodyPattern,ARGV[i],ARGV[i+1],ARGV[i+2],ARGV[i+3],ARGV[i+4]);
		exists = redis.call("exists",formatKey);
		table.insert(result,exists);
		i = i+groupLen;
	until(i>#ARGV)
	
	return result;
`)