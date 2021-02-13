package rorm

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
)







func (this *RormWritePlain) Lock( key []interface{} ) (func() error,error) {
	kpttern := newKeyWithPattern(key,&this.ormwith.model.pattern)
	lockKey   := string(kpttern.Formart())
	unRand,err := this.splain.Lock(lockKey)
	if nil != err {
		return nil,err
	}
	return func() error {
		err := this.splain.UnLock(lockKey,unRand)
		if nil != err {
			return err
		}
		return nil
	},nil
}






func (this *RormWritePlain) doInsert( key []interface{},val,extra map[string]interface{} ) error {
	//已获得本模型数据操作权限，可以操作本模型数据
	kpttern := newKeyWithPattern(key,&this.ormwith.model.pattern)
	val[this.ormwith.model.IdFieldName] = key[0]

	//填充关联模型字段主键
	unLocks,err := this.primaryKeyInsert(val,extra)
	if nil != err {
		return err
	}
	defer func() {
		for i:=0; i<len(unLocks); i++ {
			_=unLocks[i]()
		}
	}()
	//已获得关联模型操作权限

	//字段定义完整性预处理
	if err := this.ormwith.model.IntegrityProcess(val,extra);nil != err {
		return err
	}
	existsCheck,err := (*ReadOrmWithClient)(this.ormwith).ExistsCheck(val)
	if nil != err {
		return err
	}
	if existsCheck.Index != nil {
		errFormat := fmt.Sprintf(`%s already exists member=%s score=%f`,
			existsCheck.Index.identification,
			existsCheck.Member,
			existsCheck.Score,
		)
		return errors.New(errFormat)
	}

	//关联模型操作
	frostyAfter,err := this.reletionInsert(val,extra)
	if nil!= err {
		return err
	}

	//使用pipeline保证操作原子性
	pipeline := this.ormwith.client.TxPipeline()
	defer func() {
		err = pipeline.Close()
		if nil != err {
			logrus.Error(err)
			return
		}
	}()

	cliOptions := UserClientOptions{this.ormwith.client,&pipeline}

	if this.ormwith.model.InsertBefor != nil {
		err := this.ormwith.model.InsertBefor(cliOptions,&kpttern,val,frostyAfter,extra)
		if nil != err {
			return err
		}
	}

	err = this.ormwith.insert(&pipeline,&kpttern,val)
	if nil != err {
		return err
	}
	if _,err = pipeline.Exec();nil != err {
		return err
	}

	if this.ormwith.model.InsertAfter != nil {
		//如果在After回调中使用pipeline，需要自行pipeline.Exec()
		err = this.ormwith.model.InsertAfter(cliOptions,&kpttern,val,frostyAfter,extra)
		if nil != err { return err }
	}

	return nil
}




//需要先锁住相关的数据
func (this *RormWritePlain) Insert( key []interface{},val,extra map[string]interface{} ) error {
	//先获得数据操作权限
	unLock,err := this.Lock(key)
	if nil != err {
		return err
	}
	defer func() { _=unLock() }()

	return this.doInsert(key,val,extra)
}







func (this *RormWritePlain) doUpdate( key []interface{},val,extra map[string]interface{} ) error {
	//已获得本模型数据操作权限
	kpttern := newKeyWithPattern(key,&this.ormwith.model.pattern)
	val[this.ormwith.model.IdFieldName] = key[0]
	oldval,err := this.ormwith.getBodyDict(&kpttern)
	if nil != err {
		return err
	}

	//使用数据库中查询出来的值填充传入的值
	//容器字段看情况填充主键ID
	if err = this.fillNewValue(oldval,val); nil!=err {
		return err
	}

	//填充关联字段主键
	unLocks,err := this.primaryKeyUpdate(val,extra)
	if nil != err {
		return err
	}
	defer func() {
		for i:=0; i<len(unLocks); i++ {
			_=unLocks[i]()
		}
	}()
	//已获得关联模型操作权限

	//设置关联模型的值补全结构
	if err = this.ormwith.converReletion(val); nil!=err {
		return err
	}


	//填充之后进行预处理
	if err = this.ormwith.model.IntegrityProcess(val,extra);nil != err {
		return err
	}

	uniqueCheck,err := this.ormwith.ToReader().UniqueCheck(val,oldval)
	if nil != err {
		return err
	}
	if uniqueCheck.Index != nil {
		errFormat := fmt.Sprintf(`%s already exists member=%s score=%f`,
			uniqueCheck.Index.identification,
			uniqueCheck.Member,
			uniqueCheck.Score,
		)
		return errors.New(errFormat)
	}


	//关联模型操作
	frostyAfter,err := this.reletionUpdate(val,extra)
	if nil != err {
		return err
	}


	//使用pipeline保证操作原子性
	pipeline := this.ormwith.client.TxPipeline()
	defer func() {
		err := pipeline.Close()
		if nil != err {
			logrus.Error(err)
			return
		}
	}()

	cliOptions := UserClientOptions{this.ormwith.client,&pipeline}

	if this.ormwith.model.UpdateBefor != nil {
		err := this.ormwith.model.UpdateBefor(cliOptions,&kpttern,val,frostyAfter,extra)
		if nil != err {
			return err
		}
	}

	err = this.ormwith.update(&pipeline,&kpttern,oldval,val)
	if nil != err {
		return err
	}
	if _,err := pipeline.Exec();nil != err {
		return err
	}

	if this.ormwith.model.UpdateAfter != nil {
		err = this.ormwith.model.UpdateAfter(cliOptions,&kpttern,val,frostyAfter,extra)
		if nil != err {
			return err
		}
	}

	return nil
}





//依赖分布式队列做原子性保证
func (this *RormWritePlain) Update( key []interface{},val,extra map[string]interface{} ) error {
	//先获得数据操作权限
	unLock,err := this.Lock(key)
	if nil != err {
		return err
	}
	defer func() { _=unLock() }()

	return this.doUpdate(key,val,extra)
}






func (this *RormWritePlain) doDelete( key []interface{} ) error {
	kpttern := newKeyWithPattern(key,&this.ormwith.model.pattern)
	val,err := this.ormwith.getBodyDict(&kpttern)
	if nil != err {
		return err
	}


	//关联模型操作
	//删除操作不需要进行模型数据验证，所以不需要extra
	frostyAfter,err := this.reletionDelete(val,nil)
	if nil != err {
		return err
	}


	//使用pipeline保证操作原子性
	pipeline := this.ormwith.client.TxPipeline()
	defer func() {
		err := pipeline.Close()
		if nil != err {
			logrus.Error(err)
			return
		}
	}()

	cliOptions := UserClientOptions{this.ormwith.client,&pipeline}

	if this.ormwith.model.DeleteBefor != nil {
		err := this.ormwith.model.DeleteBefor(cliOptions,&kpttern,val,frostyAfter,nil)
		if nil != err {
			return err
		}
	}

	err = this.ormwith.delete(&pipeline,&kpttern,val)
	if nil != err {
		return err
	}

	if _,err := pipeline.Exec();nil != err {
		return err
	}

	if this.ormwith.model.DeleteAfter != nil {
		err = this.ormwith.model.DeleteAfter(cliOptions,&kpttern,val,frostyAfter,nil)
		if nil != err {
			return err
		}
	}

	return nil
}





func (this *RormWritePlain) Delete( key []interface{} ) error {
	//先获得数据操作权限
	unLock,err := this.Lock(key)
	if nil != err {
		return err
	}
	defer func() { _=unLock() }()

	return this.doDelete(key)
}









