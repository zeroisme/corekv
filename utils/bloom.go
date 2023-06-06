// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "math"

// Mixing constants
const (
	M = 0x5bd1e995
	R = 24
	S = 0x1234ABCD
)

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContainKey _
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(h uint32) bool {
	//Implement me here!!!
	//在这里实现判断一个数据是否在bloom过滤器中
	//思路大概是经过K个Hash函数计算，判读对应位置是否被标记为1
	n := len(f)

	k := uint32(f[n-1])
	nBits := uint32(8 * (n - 1))
	delta := h>>17 | h<<15

	for j := uint32(0); j < k; j++ {
		bitPos := h % nBits
		bytePos := bitPos / 8
		exist := f[bytePos]&(1<<(bitPos%8)) != 0
		if !exist {
			return false
		}
		h = h + delta
	}

	return true
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	//Implement me here!!!
	//阅读bloom论文实现，并在这里编写公式
	//传入参数numEntries是bloom中存储的数据个数，fp是false positive假阳性率
	m := int(-float64(numEntries) * math.Log(fp) * (math.Log2E * math.Log2E))
	return int(m / numEntries)
}

func numHash(bitsPerkey int) uint32 {
	k := uint32(float64(bitsPerkey) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	//Implement me here!!!
	//在这里实现将多个Key值放入到bloom过滤器中
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := numHash(bitsPerKey)
	nBits := len(keys) * int(bitsPerKey)
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)

	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			bytePos := bitPos / 8
			filter[bytePos] = filter[bytePos] | (1 << (bitPos % 8))
			h += delta
		}
	}
	filter[len(filter)-1] = uint8(k)
	return filter
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	var k uint32
	h := S ^ uint32(len(b))

	// Mix 4 bytes at a time into the hash
	for l := len(b); l >= 4; l -= 4 {
		k = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h, _ = mmix(h, k)
		b = b[4:]
	}
	switch len(b) {
	case 3:
		h ^= uint32(b[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(b[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(b[0])
		h *= M
	}

	h ^= h >> 13
	h *= M
	h ^= h >> 15
	return h
}

// 32-bit mixing function
func mmix(h uint32, k uint32) (uint32, uint32) {
	k *= M
	k ^= k >> R
	k *= M
	h *= M
	h ^= k
	return h, k
}
