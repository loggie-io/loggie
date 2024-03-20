package util

import (
	"unsafe"
)

func Bytes2Str(slice []byte) string {
	return *(*string)(unsafe.Pointer(&slice))
}
