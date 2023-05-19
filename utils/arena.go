package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

type Arena struct {
	n   uint32 //offset
	buf []byte
}

const MaxNodeSize = int(unsafe.Sizeof(Element{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
const nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

func newArena(n int64) *Arena {
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	//implement me here！！！
	// 在 arena 中分配指定大小的内存空间
	offset := atomic.AddUint32(&s.n, sz)
	// 空间不够了，需要扩容
	// 按当前空间的double 扩容， 不能超过32位能表示的最大值
	if offset > uint32(len(s.buf)) {
		grow := uint32(len(s.buf))

		if grow > 1<<30 {
			grow = 1 << 30
		}

		if grow < sz {
			grow = sz
		}

		buf := make([]byte, uint32(len(s.buf))+grow)
		AssertTrue(copy(buf, s.buf) == len(s.buf))
		s.buf = buf
	}
	return offset - sz
}

// 在arena里开辟一块空间，用以存放sl中的节点
// 返回值为在arena中的offset
func (s *Arena) putNode(height int) uint32 {
	//implement me here！！！
	unused := (defaultMaxLevel - height) * offsetSize

	space := uint32(MaxNodeSize-unused) + uint32(nodeAlign)

	offset := s.allocate(space)

	alignedOffset := (offset + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return alignedOffset
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	//implement me here！！！
	sz := v.EncodedSize()
	offset := s.allocate(sz)
	v.EncodeValue(s.buf[offset : offset+sz])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	//implement me here！！！
	sz := len(key)
	offset := s.allocate(uint32(sz))
	copy(s.buf[offset:offset+uint32(sz)], key)
	return offset
}

func (s *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}

	return (*Element)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(s.buf[offset : offset+size])
	return
}

// 用element在内存中的地址 - arena首字节的内存地址，得到在arena中的偏移量
func (s *Arena) getElementOffset(nd *Element) uint32 {
	//implement me here！！！
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	//implement me here！！！
	return e.levels[h]
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
