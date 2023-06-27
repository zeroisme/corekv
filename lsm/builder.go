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
package lsm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"unsafe"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
)

// TODO LAB 这里实现 序列化
type tableBuilder struct {
	opts         *Options
	blockList    []*block
	currentBlock *block
	keyHashes    []uint32
	maxVersion   uint64
	keyCount     int
	sstSize      int64
}

type block struct {
	offset            int
	chkLen            int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	checksum          []byte
	end               int
	entriesIndexStart int
}

type header struct {
	overlap uint16
	diff    uint16
}

const headerSize = uint32(unsafe.Sizeof(header{}))

func (h *header) decode(buf []byte) {
	ptr := unsafe.Pointer(h)
	copy((*[headerSize]byte)(ptr)[:], buf[:headerSize])
}

func (h header) encode() []byte {
	var b [headerSize]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

func (tb *tableBuilder) tryFinishBlock(e *codec.Entry) bool {
	if tb.currentBlock == nil {
		return true
	}

	if len(tb.currentBlock.entryOffsets) <= 0 {
		return false
	}

	entryOffsetsSize := (len(tb.currentBlock.entryOffsets)+1 /* add 1 entry */)*4 + 4 /* offsetLen */ + 8 /* checksum */ + 4 /*checksum length */

	estimatedSize := uint32(tb.currentBlock.end) + uint32(6 /* header size */) + uint32(len(e.Key)) + uint32(e.EncodedSize()) + uint32(entryOffsetsSize)
	return estimatedSize > uint32(tb.opts.BlockSize)
}

func (tb *tableBuilder) finishBlock() {
	if tb.currentBlock == nil || len(tb.currentBlock.entryOffsets) == 0 {
		return
	}

	// append entryOffsets and entry offsets length
	tb.append(codec.U32SliceToBytes(tb.currentBlock.entryOffsets))
	tb.append(codec.U32ToBytes(uint32(len(tb.currentBlock.entryOffsets))))

	// compute checksum and append
	chksum := utils.CalculateChecksum(tb.currentBlock.data[:tb.currentBlock.end])
	chksumBytes := codec.U64ToBytes(chksum)
	tb.append(chksumBytes)
	tb.append(codec.U32ToBytes(uint32(len(chksumBytes))))
	tb.blockList = append(tb.blockList, tb.currentBlock)
	tb.currentBlock = nil
}

func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	copy(dst, data)
}

func (tb *tableBuilder) allocate(sz int) []byte {
	currentBlockLen := len(tb.currentBlock.data[tb.currentBlock.end:])

	if currentBlockLen < sz {
		newSz := 2 * currentBlockLen
		if newSz < sz {
			newSz = sz
		}
		tmp := make([]byte, newSz+tb.currentBlock.end)
		copy(tmp, tb.currentBlock.data)
		tb.currentBlock.data = tmp
	}
	tb.currentBlock.end += sz
	return tb.currentBlock.data[tb.currentBlock.end-sz : tb.currentBlock.end]
}

func calDiffKey(baseKey []byte, key []byte) []byte {
	l := len(baseKey)
	if l > len(key) {
		l = len(key)
	}
	var i int
	for i = 0; i < l; i++ {
		if baseKey[i] != key[i] {
			break
		}
	}
	return key[i:]
}

func (tb *tableBuilder) add(e *codec.Entry) {
	if tb.tryFinishBlock(e) {
		tb.finishBlock()
		tb.currentBlock = &block{}
	}
	var diffKey []byte
	if len(tb.currentBlock.baseKey) == 0 {
		tb.currentBlock.baseKey = append(tb.currentBlock.baseKey[:0], e.Key...)
		diffKey = e.Key
	} else {
		diffKey = calDiffKey(tb.currentBlock.baseKey, e.Key)
	}
	tb.keyCount += 1
	tb.keyHashes = append(tb.keyHashes, utils.Hash(codec.ParseKey(e.Key)))
	if version := codec.ParseTs(e.Key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	overlap := uint16(len(tb.currentBlock.baseKey) - len(diffKey))
	diff := uint16(len(diffKey))

	h := header{
		overlap: overlap,
		diff:    diff,
	}

	tb.currentBlock.entryOffsets = append(tb.currentBlock.entryOffsets, uint32(tb.currentBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(e.EncodedSize()))
	e.EncodeEntry(dst)
}

func (tb *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = uint32(tb.keyCount)
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(&tableIndex)
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)
	return data, dataSize
}

func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		fmt.Printf("no block in table")
		return buildData{}
	}
	bd := buildData{
		blockList: tb.blockList,
	}

	var f utils.Filter
	if tb.opts.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opts.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}

	index, dataSize := tb.buildIndex(f)
	checksum := utils.CalculateChecksum(index)
	bd.index = index
	bd.checksum = codec.U64ToBytes(checksum)
	bd.size = int(dataSize) + len(index) + len(bd.checksum) + 4 + 4
	return bd
}

func (tb *tableBuilder) flush(lm *levelManager, tableName string) (*table, error) {
	bd := tb.done()
	t := &table{
		lm:  lm,
		fid: utils.FID(tableName),
	}
	t.ss = file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size),
	})
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], codec.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], codec.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	prevOverlap uint16

	it iterator.Item
}

func (iter *blockIterator) setBlock(b *block) {
	iter.block = b
	iter.err = nil
	iter.idx = 0
	iter.baseKey = iter.baseKey[:0]
	iter.prevOverlap = 0
	iter.key = iter.key[:0]
	iter.val = iter.val[:0]
	// Drop the index from the block. We don't need it anymore.
	iter.data = b.data[:b.entriesIndexStart]
	iter.entryOffsets = b.entryOffsets
}

func (iter *blockIterator) seekToFirst() {
	iter.setIdx(0)
}

func (iter *blockIterator) seekToLast() {
	iter.setIdx(len(iter.entryOffsets) - 1)
}

func (iter *blockIterator) seek(key []byte) {
	iter.err = nil
	startIndex := 0

	foundEntryIdx := sort.Search(len(iter.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		iter.setIdx(idx)
		return utils.CompareKeys(iter.key, key) >= 0
	})
	iter.setIdx(foundEntryIdx)
}

func (iter *blockIterator) setIdx(i int) {
	iter.idx = i
	if i >= len(iter.entryOffsets) || i < 0 {
		iter.err = io.EOF
		return
	}
	iter.err = nil
	startOffset := int(iter.entryOffsets[i])

	if len(iter.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(iter.data)
		iter.baseKey = iter.data[headerSize : headerSize+uint32(baseHeader.diff)]
	}
	var endOffset int
	// idx points to the last entry in the block.
	if iter.idx+1 == len(iter.entryOffsets) {
		endOffset = len(iter.data)
	} else {
		endOffset = int(iter.entryOffsets[iter.idx+1])
	}

	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered ====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				iter.tableID, iter.blockID, iter.idx, len(iter.data), startOffset, endOffset, len(iter.entryOffsets), iter.entryOffsets,
			)
			panic(debugBuf.String())
		}
	}()

	entryData := iter.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > iter.prevOverlap {
		iter.key = append(iter.key[:iter.prevOverlap], iter.baseKey[iter.prevOverlap:h.overlap]...)
	}

	iter.prevOverlap = h.overlap
	valueOff := headerSize + uint32(h.diff)
	diffKey := entryData[headerSize:valueOff]
	iter.key = append(iter.key[:h.overlap], diffKey...)
	e := &codec.Entry{Key: iter.key}
	e.DecodeEntry(entryData[valueOff:])
	iter.it = &Item{e: e}
}

func (iter *blockIterator) Error() error {
	return iter.err
}

func (iter *blockIterator) Next() {
	iter.setIdx(iter.idx + 1)
}

func (iter *blockIterator) Valid() bool {
	return iter.err != io.EOF
}

func (iter *blockIterator) Rewind() bool {
	iter.setIdx(0)
	return iter.Valid()
}

func (iter *blockIterator) Item() iterator.Item {
	return iter.it
}

func (iter *blockIterator) Close() error {
	return nil
}

func newTableBuilder(opts *Options) *tableBuilder {
	return &tableBuilder{
		opts:    opts,
		sstSize: opts.SSTableMaxSz,
	}
}
