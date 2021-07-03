package ccache

import (
	"fmt"
	"github.com/cespare/xxhash/v2"
	"sync"
)

const (
	bucketsCount = 512       // buckets的数量
	chunkSize    = 64 * 1024 // 每个chunk的大小，即 [chunkSize]byte

	// 定义组成索引的位信息
	bucketSizeBits        = 40                  // 低位用 40bit 存放kv信息
	genSizeBits           = 64 - bucketSizeBits // 高位用 24bit 存放 gen 信息
	maxGen                = 1<<genSizeBits - 1  // 最大 gen
	maxBucketSize  uint64 = 1 << bucketSizeBits // 最大 kv 信息
)

type Cache struct {
	buckets [bucketsCount]bucket
}

func New(maxBytes int) *Cache {
	if maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes))
	}
	var c Cache
	maxBucketBytes := uint64((maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] { // TODO 为什么不直接遍历 c.buckets ？
		c.buckets[i].Init(maxBucketBytes)
	}
	return &c
}

func (c *Cache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h)
}

func (c *Cache) Get(k []byte) []byte {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	dst, _ := c.buckets[idx].Get(k, h)
	return dst
}

func (c *Cache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
}

type bucket struct {
	mu     sync.RWMutex
	chunks [][]byte          // 存放数据的地方，用二维数组构建的环形链表
	m      map[uint64]uint64 // 索引映射
	idx    uint64            // 当前的索引值
	gen    uint64            // chunks 被重写的次数
}

func (b *bucket) Init(maxBytes uint64) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; must be smaller than %d", maxBytes, maxBucketSize))
	}
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	// 初始化chunk
	b.Reset()
}

func (b *bucket) Get(k []byte, h uint64) (dst []byte, found bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var (
		v    = b.m[h]
		bGen = b.gen & (1<<genSizeBits - 1)
	)
	if v > 0 {
		var (
			// 高于bucketSizeBits位置表示gen
			gen = v >> bucketSizeBits
			// 低于bucketSizeBits位置表示idx
			// 1左移bucketSizeBits再减一，即可得到 bucketSizeBits 位全是1的值作为掩码，
			// 再用v进行与操作，即可得到低位idx的值
			idx = v & (1<<bucketSizeBits - 1)

			inCurrentGenCircle = gen == bGen && idx < b.idx                 // chunks未被写满
			inPrevNotOverride  = gen+1 == bGen && idx >= b.idx              // chunks被写满，但是当前索引数据未被覆盖
			inBoundNotOverride = gen == maxGen && bGen == 1 && idx >= b.idx // chunks被写满，且数据存储的链表处于边界，当前b.gen从1开始，数据未被覆盖
		)
		if inCurrentGenCircle || inPrevNotOverride || inBoundNotOverride {
			chunkIdx := idx / chunkSize

			// chunk 索引位置不能超过 chunks 数组长度
			if chunkIdx >= uint64(len(b.chunks)) {
				return
			}
			chunk := b.chunks[chunkIdx]

			// 通过取模，获得索引在chunk中的便宜值
			// 存储的数据至少有4位是元信息
			idx %= chunkSize
			if idx+4 >= chunkSize {
				return
			}

			kvLenBuf := chunk[idx : idx+4] // 数据头，元信息
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
			if idx+keyLen+valLen >= chunkSize {
				return
			}

			if string(k) == string(chunk[idx:idx+keyLen]) {
				idx += keyLen
				dst = append(dst, chunk[idx:idx+valLen]...)
				found = true
			}
		}
	}
	return dst, found
}

func (b *bucket) Set(k, v []byte, h uint64) {
	// 由于 kv 数据需要存放在 chunk 中，前面定义 chunkSize 的大小为 1<<16 (64k)
	// 所以 kv 的大小要小于 chunkSize
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		return
	}

	// kvLenBuf 数据元信息
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k) >> 8)) // len(k) <= 16bit, 右移8位，以获取高位8bit
	kvLenBuf[1] = byte(len(k))              // 16bit转byte(8bit)，高位会溢出丢失，这里获取的是低位8bit
	kvLenBuf[2] = byte(uint16(len(v) >> 8)) // v hi 8bit
	kvLenBuf[3] = byte(len(v))              // v lo 8bit
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	// chunk中存储的包括 元数据+k+v，所以验证下总长度不能大于 chunkSize
	if kvLen >= chunkSize {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var (
		// 当前索引位置
		// idx相当于offset，记录的是当前chunk(kvLen 即数据header信息)在bucket中的偏移
		// idx作为数据的偏移值，所以idx的最大bit应该为 maxBucketSize, (表示一个桶最多可存储多少位数据)
		idx    = b.idx
		idxNew = idx + kvLen

		// 根据索引找到在 chunks 的位置
		chunkIdx    = idx / chunkSize
		chunkIdxNew = idxNew / chunkSize
	)

	// 新的索引是否超过当前索引，因为还有chunkIdx等于chunkIdxNew情况，所以需要先判断一下
	// 如果 chunkIdxNew == chunkIdx，说明 chunk 还有容量保存当前数据；
	// 否则应当用新的 chunk 去保存数据，也就是说，chunk 中保存的数据，只会 <= chunkSize
	if chunkIdxNew > chunkIdx {
		// 校验是否新索引已到chunks数组的边界
		// 已到边界，那么循环链表从头开始，此时会覆盖掉头部的chunk旧数据
		if chunkIdxNew > uint64(len(b.chunks)) {
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			b.gen++
			// 当 gen 等于 1<<genSizeBits时，才会等于0
			// 也就是用来限定 gen 的边界为1<<genSizeBits
			if b.gen&((1<<genSizeBits)-1) == 0 {
				// 如果处于边界，继续自增则会让高位溢出，参与后续的计算不会影响
				//b.gen++ 如果不重置gen，最终会溢出uint64
				b.gen = 1
			}
		} else {
			// 未到 chunks数组的边界,从下一个chunk开始
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		// 此时 chunkIdx 为新的chunk，重置chunk
		b.chunks[chunkIdx] = b.chunks[chunkIdx][:0]
	}

	// append 数据到 chunk 中
	chunk := b.chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0] // 清空切片
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	b.chunks[chunkIdx] = chunk
	// 因为 idx 不能超过bucketSizeBits，所以用一个 uint64 同时表示gen和idx
	// 所以高于bucketSizeBits位置表示gen
	// 低于bucketSizeBits位置表示idx
	b.m[h] = idx | (b.gen << bucketSizeBits) // TODO hash 冲突？
	b.idx = idxNew
}

func (b *bucket) Reset() {
	b.mu.Lock()
	// 将chunks的内存归还到缓存池中
	chunks := b.chunks
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil
	}
	// 删除索引字典中所有的数据
	bm := b.m // TODO 为什么要设置变量，而不是直接操作？
	for k := range bm {
		delete(bm, k)
	}
	b.idx = 0
	b.gen = 1
	b.mu.Unlock()
}
