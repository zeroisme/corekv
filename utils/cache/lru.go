package cache

import "container/list"

type windowLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (lru *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	//implement me here!!!
	// 已经存在这条Key，替换Value
	elem, ok := lru.data[newitem.key]
	if ok {
		elem.Value = &newitem
		lru.list.MoveToFront(elem)
		return newitem, false
	}
	// 未达到缓存容量上限
	if lru.list.Len() < lru.cap {
		e := lru.list.PushFront(&newitem)
		lru.data[newitem.key] = e
		return newitem, false
	}
	// 到达缓存容量上限，驱逐最链表末尾元素
	e := lru.list.Back()
	lru.list.Remove(e)

	newelem := lru.list.PushFront(&newitem)
	lru.data[newitem.key] = newelem
	return *e.Value.(*storeItem), true
}

func (lru *windowLRU) get(v *list.Element) {
	//implement me here!!!
	item := v.Value.(*storeItem)

	if e, ok := lru.data[item.key]; ok {
		v.Value = e.Value
		lru.list.MoveToFront(e)
	}

	// Not found
}
