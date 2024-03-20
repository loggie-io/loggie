package decompress

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
)

type DecompressorFactory struct {
	decompressorFactory map[string]DecompressorWorkShop
	lock                sync.RWMutex
}

var mutex sync.RWMutex

var globalDecompressorFactory *DecompressorFactory

type DecompressorWorkShop func() Decompressor

func GetOrCreateDecompressorFactory() *DecompressorFactory {
	if globalDecompressorFactory != nil {
		return globalDecompressorFactory
	}
	mutex.Lock()
	defer mutex.Unlock()
	globalDecompressorFactory = &DecompressorFactory{
		decompressorFactory: make(map[string]DecompressorWorkShop),
	}
	return globalDecompressorFactory
}

func (factory *DecompressorFactory) Register(compressType string, workShop DecompressorWorkShop) DecompressorWorkShop {
	factory.lock.Lock()
	defer factory.lock.Unlock()
	value, ok := factory.decompressorFactory[compressType]
	if ok {
		return value
	}
	factory.decompressorFactory[compressType] = workShop
	return workShop
}

func (factory *DecompressorFactory) Get(compressType string) DecompressorWorkShop {
	value, ok := factory.decompressorFactory[compressType]
	if ok {
		return value
	}
	return nil
}

func (factory *DecompressorFactory) MakeDecompressor(path string) (Decompressor, error) {
	ext := filepath.Ext(path)
	workShop := factory.Get(ext)
	if workShop != nil {
		return workShop().Open(path)
	}
	return nil, errors.New(fmt.Sprintf("ext(%s) is not found", ext))
}
