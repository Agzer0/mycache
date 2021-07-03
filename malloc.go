package ccache

import (
	"fmt"
	"sync"
	"syscall"
	"unsafe"
)

// 每次分配多少内存, 64mb
const perAllocMemSize = chunkSize * 1024

var (
	freeChunks    []*[chunkSize]byte
	freeChunkLock sync.Mutex
)

func getChunk() []byte {
	freeChunkLock.Lock()
	defer freeChunkLock.Unlock()

	if len(freeChunks) == 0 {
		// 如果 chunks 池中没有可用，一次性分配 perAllocMemSize bytes 的内存出来
		data, err := syscall.Mmap(-1, 0, perAllocMemSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			panic(fmt.Errorf("cant`t alloc %d bytes via mmap: %s", perAllocMemSize, err))
		}
		// 将 data 分为 1024 份，每份大小为 chunkSize(64kb)，存放到 freeChunks 闲置池中
		for len(data) > 0 {
			p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
			freeChunks = append(freeChunks, p)
			data = data[chunkSize:]
		}
	}

	// 从 freeChunks 中 pop 出最后一个元素
	n := len(freeChunks) - 1
	p := freeChunks[n]
	freeChunks[n] = nil // TODO 如果不做这一步会怎样？
	freeChunks = freeChunks[:n]
	return p[:]
}

func putChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	// 执行这个操作，来确保 chunk 的容量有 chunkSize 大小，防止接下来转换指针出错
	// 如果 chunk 实际容量超出 chunkSize 这一步无关紧要
	// 如果不够的话，转换后的数据可能异常
	chunk = chunk[:chunkSize]
	p := (*[chunkSize]byte)(unsafe.Pointer(&chunk[0]))

	freeChunkLock.Lock()
	freeChunks = append(freeChunks, p)
	freeChunkLock.Unlock()
}
