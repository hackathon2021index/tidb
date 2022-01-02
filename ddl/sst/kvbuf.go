package sst

import (
	"github.com/pingcap/tidb/br/pkg/lightning/common"
)

// woker is addIndexWorker. one worker one cache.
type WorkerKVCache struct {
	pairs    []common.KvPair
	buf      []byte
	last     int
	cap      int
	needGrow bool // need grow
}

func NewWorkerKVCache() WorkerKVCache {
	bs := make([]byte, flush_size)
	return WorkerKVCache{
		pairs:    make([]common.KvPair, 0, flush_size/32),
		buf:      bs,
		last:     0,
		cap:      flush_size,
		needGrow: false,
	}
}

func (wc *WorkerKVCache) Reset() {
	wc.last = 0
	wc.pairs = wc.pairs[:0]
	if wc.needGrow {
		wc.cap = wc.cap * 2
		wc.buf = make([]byte, wc.cap)
		wc.needGrow = false
	}
}

func (wc *WorkerKVCache) Fetch() []common.KvPair {
	return wc.pairs
}

func (wc *WorkerKVCache) PushKeyValue(k, v []byte) {
	p := common.KvPair{
		Key: wc.cloneBytes(k),
		Val: wc.cloneBytes(v),
	}
	wc.pairs = append(wc.pairs, p)
}

func (wc *WorkerKVCache) cloneBytes(v []byte) []byte {
	if false == wc.needGrow {
		ret := wc.pushBytes(v)
		if ret != nil {
			return ret
		}
		// not enough cache;
	}
	ret := make([]byte, len(v))
	copy(ret, v)
	return ret
}

func (wc *WorkerKVCache) pushBytes(v []byte) []byte {
	vl := len(v)
	s := wc.last
	wc.last += vl
	if wc.last > wc.cap {
		wc.needGrow = true
		return nil
	}
	ret := wc.buf[s:wc.last]
	copy(ret, v)
	return ret
}
