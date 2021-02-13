package _struct

import "io"

//管道读取事件封装

type PipeReader struct{
	pReader      *io.PipeReader
	onReadBefor  func() error
	onReadAfter  func() error
	onCloseAfter func() error
}


func NewPipeReader( pReader *io.PipeReader ) *PipeReader {
	return &PipeReader{
		pReader,
		nil,
		nil,
		nil,
	}
}


func (this *PipeReader) Read(p []byte) (n int, err error) {

	if this.onReadBefor != nil {
		err := this.onReadBefor()
		if nil != err {
			return 0,err
		}
	}

	n,err = this.pReader.Read(p)
	if nil != err {
		return 0,err
	}

	if this.onReadAfter != nil {
		err := this.onReadAfter()
		if nil != err {
			return 0,err
		}
	}

	return n,err
}



func (this *PipeReader) Close() error {
	err := this.pReader.Close()
	if nil != err {
		return err
	}
	if this.onCloseAfter != nil {
		err := this.onCloseAfter()
		if nil != err {
			return err
		}
	}
	return err
}



func (this *PipeReader) CloseWithError(err2 error) error {
	err := this.pReader.CloseWithError(err2)
	if nil != err {
		return err
	}
	if this.onCloseAfter != nil {
		err := this.onCloseAfter()
		if nil != err {
			return err
		}
	}
	return err
}






func (this *PipeReader) OnReadAfter( fn func() error ) {
	this.onReadAfter = fn
}


func (this *PipeReader) OnReadBefor( fn func() error ) {
	this.onReadBefor = fn
}


func (this *PipeReader) OnClose( fn func() error ) {
	this.onCloseAfter = fn
}
