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

package file

import (
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"github.com/pkg/errors"
)

// TODO LAB 在这里实现 sst 文件操作
type SSTable struct {
	lock           *sync.RWMutex
	fid            uint32
	f              *MmapFile
	minKey         []byte
	maxKey         []byte
	idxLen         int
	idxStart       int
	idxTable       *pb.TableIndex
	hasBloomFilter bool
	createdAt      time.Time
}

func OpenSStable(opts *Options) *SSTable {
	filename := utils.FileNameSSTable(opts.Dir, uint64(opts.FID))
	omf, err := OpenMmapFile(filename, opts.Flag, opts.MaxSz)
	utils.Err(err)
	return &SSTable{
		fid:  opts.FID,
		f:    omf,
		lock: &sync.RWMutex{},
	}
}

func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

func (ss *SSTable) SetMaxKey(key []byte) {
	ss.maxKey = key
}

func (ss *SSTable) FID() uint32 {
	return ss.fid
}

func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}

func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[off : off+sz], nil
	}

	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(off))
	return res, err
}

func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) Init() error {
	var ko *pb.BlockOffset
	var err error
	if ko, err = ss.initTable(); err != nil {
		return err
	}
	// 从文件中获取创建时间
	stat, _ := ss.f.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	ss.createdAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)
	// init min key
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey
	ss.maxKey = minKey
	return nil
}

func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	readPos := len(ss.f.Data)

	// Read checksum len from the last 4 bytes.
	readPos -= 4
	buf := ss.readCheckError(readPos, 4)
	checksumLen := int(codec.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// Read checksum.
	readPos -= checksumLen
	expectedChk := ss.readCheckError(readPos, checksumLen)

	// Read index size from the footer.
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.idxLen = int(codec.BytesToU32(buf))

	// Read index.
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.readCheckError(readPos, ss.idxLen)
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to verify checksum for table: %s", ss.f.Fd.Name()))
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTable = indexTable

	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

func (ss *SSTable) Size() int64 {
	fileStats, err := ss.f.Fd.Stat()
	utils.Panic(err)
	return fileStats.Size()
}

func (ss *SSTable) GetCreatedAt() *time.Time {
	return &ss.createdAt
}

func (ss *SSTable) SetCreatedAt(t *time.Time) {
	ss.createdAt = *t
}

func (ss *SSTable) Delete() error {
	return ss.f.Delete()
}

func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTable
}
