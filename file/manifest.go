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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/hardcore-os/corekv/pb"
	"github.com/hardcore-os/corekv/utils"
	"github.com/pkg/errors"
)

// LAB Manifest .... Write your code
type ManifestFile struct {
	opt      *Options
	f        *os.File
	lock     sync.Mutex
	manifest *Manifest
}

type TableMeta struct {
	ID       uint64
	Checksum []byte
}

type Manifest struct {
	Levels    []levelManifest          // level -> table set 每层有哪些sst
	Tables    map[uint64]TableManifest // 用于查询一个table在哪一层
	Creations int
	Deletions int
}

type TableManifest struct {
	Level    uint8
	Checksum []byte
}

type levelManifest struct {
	Tables map[uint64]struct{}
}

func (m *Manifest) asChanges() *pb.ManifestChangeSet {
	changeSet := &pb.ManifestChangeSet{}

	for level, levelManifest := range m.Levels {
		for table := range levelManifest.Tables {
			change := &pb.ManifestChange{
				Id:       table,
				Op:       pb.ManifestChange_CREATE,
				Level:    uint32(level),
				Checksum: m.Tables[table].Checksum,
			}
			changeSet.Changes = append(changeSet.Changes, change)
		}
	}
	return changeSet
}

func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{lock: sync.Mutex{}, opt: opt}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		m := createManifest()
		fp, netCreations, err := helpRewrite(opt.Dir, m)
		utils.CondPanic(netCreations == 0, errors.Wrap(err, utils.ErrReWriteFailure.Error()))
		if err != nil {
			return mf, err
		}
		mf.f = fp
		f = fp
		mf.manifest = m
		return mf, nil
	}
	// 如果打开， 则对manifest文件重放
	manifest, truncOffset, err := ReplyManifest(f)
	fmt.Printf("ISEOF: %v\n", err == io.EOF)
	if err != nil {
		_ = f.Close()
		return mf, err
	}

	// Truncate file so we don't have a half-written entry at the end.
	if err := f.Truncate(int64(truncOffset)); err != nil {
		_ = f.Close()
		return mf, err
	}

	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return mf, err
	}
	mf.f = f
	mf.manifest = manifest

	return mf, nil
}

func applyChange(m *Manifest, change *pb.ManifestChange) error {
	for i := len(m.Levels); i <= int(change.Level); i++ {
		m.Levels = append(m.Levels, levelManifest{Tables: map[uint64]struct{}{}})
	}
	switch change.Op {
	case pb.ManifestChange_CREATE:
		m.Levels[change.Level].Tables[change.Id] = struct{}{}
		m.Tables[change.Id] = TableManifest{
			Level:    uint8(change.Level),
			Checksum: change.Checksum,
		}
		m.Creations++
	case pb.ManifestChange_DELETE:
		delete(m.Levels[change.Level].Tables, change.Id)
		delete(m.Tables, change.Id)
		m.Deletions++
	default:
		return errors.Errorf("unknown manifest change op: %v", change.Op)
	}
	return nil
}

func applyChangeSet(m *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		err := applyChange(m, change)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mf *ManifestFile) AddChanges(changes []*pb.ManifestChange) error {
	return mf.addChanges(changes)
}

func (mf *ManifestFile) addChanges(changes []*pb.ManifestChange) error {
	changeSet := &pb.ManifestChangeSet{Changes: changes}

	buf, err := changeSet.Marshal()
	if err != nil {
		return err
	}

	mf.lock.Lock()
	defer mf.lock.Unlock()

	if err := applyChangeSet(mf.manifest, changeSet); err != nil {
		return err
	}
	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
}

func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}
func (mf *ManifestFile) AddTableMeta(level int, t *TableMeta) (err error) {
	mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, level, t.Checksum),
	})
	return err
}

func (mf *ManifestFile) rewrite() error {
	mf.lock.Lock()
	defer mf.lock.Unlock()

	// 关闭Manifest文件
	if err := mf.f.Close(); err != nil {
		return err
	}

	// 重写Manifest文件
	fp, _, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.f = fp
	return nil
}

func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}
	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d not referenced in MANIFEST", id))
			filename := utils.FileNameSSTable(mf.opt.Dir, id)
			if err := os.Remove(filename); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

func createManifest() *Manifest {
	levels := make([]levelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

func helpRewrite(dir string, m *Manifest) (fp *os.File, nCreates int, err error) {
	tempFilename := path.Join(dir, utils.ManifestRewriteFilename)
	fp, err = os.OpenFile(tempFilename, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))

	nCreations := len(m.Tables)
	changes := m.asChanges()
	changeBuf, err := changes.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}

	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	// In Windows the files should be closed before doing a rename.
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}

	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(tempFilename, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}

	if err := utils.SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, nCreations, nil
}

func ReplyManifest(f *os.File) (*Manifest, int64, error) {
	// 读取文件头
	var count int
	var err error
	buf := make([]byte, 8)
	if count, err = f.Read(buf); err != nil {
		return nil, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(buf[0:4], utils.MagicText[:]) {
		return nil, 0, utils.ErrBadMagic
	}

	if binary.BigEndian.Uint32(buf[4:8]) != uint32(utils.MagicVersion) {
		return nil, 0, fmt.Errorf("manifest has unsupported version")
	}

	m := createManifest()
	var offset int64
	for {
		offset = offset + int64(count)
		var lenCrcBuf [8]byte
		count, err = f.Read(lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		var buf = make([]byte, length)
		var bufcount int
		if bufcount, err = f.Read(buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		count += bufcount
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}
		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return &Manifest{}, 0, err
		}

		if err := applyChangeSet(m, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}
	return m, offset, nil
}

func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}

func (mf *ManifestFile) Close() error {
	return mf.f.Close()
}
