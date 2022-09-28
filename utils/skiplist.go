package utils

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxHeight = 48
	p                = 0.25
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {

	header := &Element{
		levels: make([]*Element, defaultMaxHeight),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxHeight - 1,
		rand:     r,
	}
}

type Element struct {
	// levels[i] 存的是这个节点的第 i 个 level 的下一个节点
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()

	keyScore := list.calcScore(data.Key)
	header, maxLevel := list.header, list.maxLevel

	prevs := make([]*Element, maxLevel+1)
	prev := header
	// 寻找插入位置
	for i := maxLevel; i >= 0; i-- {
		for node := prev.levels[i]; node != nil; node = prev.levels[i] {
			if comp := list.compare(keyScore, data.Key, node); comp <= 0 {
				// key相等，更新对应值
				if comp == 0 {
					node.score = keyScore
					node.entry = data
					return nil
				}
				prev = node
			} else {
				break
			}
		}
		prevs[i] = prev
	}

	level := list.randLevel()
	node := newElement(keyScore, data, level)

	for i := level; i >= 0; i-- {
		node.levels[i] = prevs[i].levels[i]
		prevs[i].levels[i] = node
	}
	list.size += 1
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel

	prev := header
	for i := maxLevel; i >= 0; i-- {
		for node := prev.levels[i]; node != nil; node = prev.levels[i] {
			if comp := list.compare(keyScore, key, node); comp <= 0 {
				if comp == 0 {
					return node.entry
				}
				prev = node
			} else {
				break
			}
		}
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score < next.score {
		return -1
	} else if score > next.score {
		return 1
	} else {
		return bytes.Compare(key, next.entry.Key)
	}
}

func (list *SkipList) randLevel() int {
	level := 1
	for ; list.rand.Float32() < p && level < list.maxLevel; level++ {
	}

	return level
}

func (list *SkipList) Size() int64 {
	return list.size
}
