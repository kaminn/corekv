package utils

import (
	"bytes"
	"fmt"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultMaxLevel = 48
	P               = 0.25
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
		levels: make([]*Element, defaultMaxLevel),
	}

	return &SkipList{
		header:   header,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel: 1,
		length:   0,
		size:     0,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	score := calcScore(data.Key)
	var ele *Element
	prev := list.header
	prevElements := make([]*Element, defaultMaxLevel)

	// Find all previous head
	for i := defaultMaxLevel - 1; i >= 0; i-- {
		prevElements[i] = prev
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if cmp := list.compare(score, data.Key, next); cmp <= 0 {
				// If equal, just update
				if cmp == 0 {
					ele = next
					ele.entry = data
					return nil
				}
				break
			} else {
				// Search for the next level
				prev = next
				prevElements[i] = prev
			}
		}
	}

	list.lock.Lock()
	defer list.lock.Unlock()
	level := list.randLevel()
	ele = newElement(score, data, level)
	if level > list.maxLevel {
		list.maxLevel = level
	}

	for i := 0; i < level; i++ {
		ele.levels[i] = prevElements[i].levels[i]
		prevElements[i].levels[i] = ele
	}
	list.length++
	list.size++

	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	if list.length == 0 {
		return nil
	}

	keyScore := calcScore(key)
	prev := list.header
	level := list.maxLevel
	for i := level - 1; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if cmp := list.compare(keyScore, key, next); cmp <= 0 {
				if cmp == 0 {
					return next.entry
				}
				break
			} else {
				prev = next
			}

		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func calcScore(key []byte) (score float64) {
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
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	} else if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	// From Redis
	h := 1
	x := P * 0xFFFF
	for list.rand.Intn(0xFFFF) < int(x) {
		h++
	}
	if h > defaultMaxLevel {
		h = defaultMaxLevel
	}

	return h
}

func (list *SkipList) Size() int64 {
	return list.size
}

func (list *SkipList) Print() {
	// Unsafe on concurrent
	if list.size == 0 {
		return
	}

	for i := list.maxLevel - 1; i >= 0; i-- {
		c := 0
		for next := list.header.levels[i]; next != nil; next = next.levels[i] {
			fmt.Printf("%s->", string(next.entry.Key))
			if next.levels[i] == nil {
				fmt.Print("nil")
			}
			c++
		}
		println()
		fmt.Printf("level: %d count: %d\n", i, c)
	}
}
