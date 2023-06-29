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
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/hardcore-os/corekv/utils"
	"github.com/pkg/errors"
)

// LAB Recvery
type WalFile struct {
	f    *MmapFile
	off  int
	opt  *Options
	lock sync.Mutex
}

func OpenWalFile(opt *Options) *WalFile {
	mf, err := OpenMmapFile(opt.FileName, opt.Flag, opt.MaxSz)
	utils.Panic(err)

	return &WalFile{f: mf, off: 0, opt: opt, lock: sync.Mutex{}}
}

func (w *WalFile) Close() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f.ReName(w.f.Fd.Name() + ".del")
	return nil
}

func (w *WalFile) Write(entry *utils.Entry) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	key := entry.Key
	val := entry.Value
	expireAt := utils.U64ToBytes(entry.ExpiresAt)

	dataLen := 4 /* keylen */ + 4 /* valueLen */ + 8 /* expireAt */ + len(key) + len(val) + 4 /* crc32 */

	buf := make([]byte, dataLen)
	copy(buf, utils.U32ToBytes(uint32(len(key))))
	copy(buf[4:], utils.U32ToBytes(uint32(len(val))))
	copy(buf[8:], expireAt)
	copy(buf[16:], key)
	copy(buf[16+len(key):], val)
	fmt.Printf("write key: %s, val: %s\n", key, val)
	fmt.Printf("write key: %d, val: %d dataLen: %d, buflen: %d\n", len(key), len(val), dataLen, len(buf))
	fmt.Printf("KeyLen: %d", utils.BytesToU32(buf[:4]))
	fmt.Printf("write key: %s, val: %s\n", buf[16:16+len(key)], buf[16+len(key):16+len(key)+len(val)])

	crc32 := crc32.Checksum(buf[:dataLen-4], utils.CastagnoliCrcTable)
	fmt.Printf("crc32: %d\n", crc32)
	copy(buf[16+len(key)+len(val):], utils.U32ToBytes(crc32))

	dst, _, err := w.f.AllocateSlice(dataLen, w.off)
	if err != nil {
		return err
	}
	copy(dst, buf)
	if err = w.f.Sync(); err != nil {
		return err
	}
	w.off += dataLen + 4 /* dataLen */

	return nil
}

func (w *WalFile) Iterate(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	reader := bufio.NewReader(w.f.NewReader(int(offset)))

	var validEndOffset uint32 = offset

loop:
	for {
		buf := make([]byte, 4)

		_, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == utils.ErrTruncate {
				break loop
			}
			return validEndOffset, err
		}
		dataLen := utils.BytesToU32(buf)
		buf = make([]byte, dataLen)
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == utils.ErrTruncate {
				break loop
			}
			return validEndOffset, err
		}
		if n == 0 || dataLen <= 0 {
			break loop
		}
		keyLen := utils.BytesToU32(buf[:4])
		valLen := utils.BytesToU32(buf[4:8])
		expireAt := utils.BytesToU64(buf[8:16])
		key := buf[16 : 16+keyLen]
		val := buf[16+keyLen : 16+keyLen+valLen]
		crc := utils.BytesToU32(buf[dataLen-4:])

		entry := &utils.Entry{
			Key:       key,
			Value:     val,
			ExpiresAt: expireAt,
		}
		fmt.Printf("KeyLen: %d valLen: %d\n", keyLen, valLen)
		fmt.Printf("=== key: %s, val: %s ===\n", key, val)
		fmt.Printf("crc: %d\n crc32:%d\n", crc, crc32.Checksum(buf[:dataLen-4], utils.CastagnoliCrcTable))
		if crc != crc32.Checksum(buf[:dataLen-4], utils.CastagnoliCrcTable) {
			return validEndOffset, utils.ErrBadChecksum

		}
		validEndOffset += dataLen + 4

		if err := fn(entry, &utils.ValuePtr{}); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}
	return validEndOffset, nil
}

func (w *WalFile) Truncate(off int64) error {
	return w.f.Truncature(off)
}

func (w *WalFile) Name() string {
	return w.f.Fd.Name()
}

func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".wal") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".wal")
	id, err := strconv.Atoi(name)
	if err != nil {
		utils.Err(err)
		return 0
	}
	return uint64(id)
}
