package helper

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"runtime"
	"unsafe"
)




func BKDRHash(str string) uint64 {
	seed := uint64(131) // 31 131 1313 13131 131313 etc..
	hash := uint64(0)
	for i := 0; i < len(str); i++ {
		hash = (hash * seed) + uint64(str[i])
	}
	return hash
}




func APHash(str string) uint64 {
	hash := uint64(0xAAAAAAAA)
	for i := 0; i < len(str); i++ {
		if (i & 1) == 0 {
			hash ^= ((hash << 7) ^ uint64(str[i])*(hash>>3))
		} else {
			hash ^= (^((hash << 11) + uint64(str[i]) ^ (hash >> 5)))
		}
	}
	//fmt.Printf("APHash %v\n", hash)
	return hash
}





func Hash16String( sum [16]byte,enBuffer []byte ) {
	if len(enBuffer) != 32 {
		panic("enBuffer size should 32")
	}
	var sumptr = uintptr(unsafe.Pointer(&sum[0]))
	var encodesrc []byte
	encodesrc_struct := (*struct{
		array unsafe.Pointer
		len int
		cap int
	})(unsafe.Pointer(&encodesrc))
	encodesrc_struct.len = 16
	encodesrc_struct.cap = 16
	encodesrc_struct.array = unsafe.Pointer(sumptr)

	hex.Encode(enBuffer,encodesrc)
}




func Hash32String( sum [32]byte,enBuffer []byte ) {
	if len(enBuffer) != 64 {
		panic("enBuffer size should 64")
	}
	sumptr := uintptr(unsafe.Pointer(&sum[0]))
	var encodesrc []byte
	encodesrc_struct := (*struct{
		array unsafe.Pointer
		len int
		cap int
	})(unsafe.Pointer(&encodesrc))
	encodesrc_struct.len = 32
	encodesrc_struct.cap = 32
	encodesrc_struct.array = unsafe.Pointer(sumptr)

	hex.Encode(enBuffer,encodesrc)
}




func Hash32Base64( sum [32]byte,encodebuf []byte ) {
	base64.StdEncoding.Encode(encodebuf,sum[:])
}



func FileLineToken( skip int ) string {
	_,file,line,_ := runtime.Caller(skip)
	hexBefor := fmt.Sprintf("%s:%d",file,line)
	ret := sha256.Sum256([]byte(hexBefor))
	hex64 := base64.RawStdEncoding.EncodeToString(ret[:])
	return hex64
}