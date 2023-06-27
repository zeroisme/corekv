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
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"github.com/pkg/errors"
)

var FID uint32

func GetNextFID() uint32 {
	return atomic.AddUint32(&FID, 1)
}

func SetFID(fid uint32) {
	atomic.StoreUint32(&FID, fid)
}

// TODO LAB 这里实现 table
type table struct {
	lm  *levelManager
	fid uint64
	ss  *file.SSTable
}

func (t *table) block(idx int) (*block, error) {
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	var ko pb.BlockOffset
	t.offsets(&ko, idx)
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}

	readPos := len(b.data) - 4
	b.chkLen = int(codec.BytesToU32(b.data[readPos : readPos+4]))
	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set ")
	}
	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]
	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}
	readPos -= 4
	numEntries := int(codec.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4
	b.entryOffsets = codec.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])
	b.entriesIndexStart = entriesIndexStart
	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

// blockCacheKey is used to store blocks in the block cache
func (t *table) blockCacheKey(idx int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

func (t *table) Search(key []byte, maxVs *uint64) (*codec.Entry, error) {
	idx := t.ss.Indexs()
	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&iterator.Options{})
	defer iter.Close()
	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if codec.SameKey(key, iter.Item().Entry().Key) {
		if version := codec.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}

	var (
		t   *table
		err error
	)

	fid := utils.FID(tableName)
	// 对builder存在的情况 把buf flush 到磁盘
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize),
		})
	}
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	itr := t.NewIterator(&iterator.Options{})
	defer itr.Close()
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)
	return t
}

func (t *table) NewIterator(opts *iterator.Options) iterator.Iterator {
	return &tableIterator{
		opt: *opts,
		t:   t,
		bi:  &blockIterator{},
	}
}

type tableIterator struct {
	it       iterator.Item
	opt      iterator.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (it *tableIterator) Next() {
	it.err = nil

	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}

	if len(it.bi.data) == 0 {
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.err = it.bi.Error()
		return
	}
	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}

func (it *tableIterator) Valid() bool {
	return it.err != io.EOF
}

func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}

func (it *tableIterator) Item() iterator.Item {
	return it.it
}

func (it *tableIterator) Close() error {
	it.bi.Close()
	return nil
}

func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().GetOffsets())
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().GetOffsets())
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}

	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// Seek
// 二分搜索offsets
// 如果idx == 0 说明key只能在第一个block中：block[0].MinKey <= key
// 否则block[0].MinKey > key
// 如果在idx-1的block中未找到key，那才可能出现在idx中
// 如果都没有，则当前key不在此table中
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		it.t.offsets(&ko, idx)
		if idx == len(it.t.ss.Indexs().GetOffsets()) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}

	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i]
	return true
}

func (t *table) Size() int64 { return int64(t.ss.Size()) }

func (t *table) GetCreatedAt() *time.Time {
	return t.ss.GetCreatedAt()
}

func (t *table) Delete() error {
	return t.ss.Delete()
}
