package sst

import (
	"sync"
	"sync/atomic"
	"flag"
	"context"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/errors"
)

type engineInfo struct {
	*backend.OpenedEngine
	writer *backend.LocalEngineWriter
	cfg    *backend.EngineConfig
	// TODO: use channel later;
	ref  int32
	kvs  []common.KvPair
	size int
}

func (ei *engineInfo) ResetCache() {
	ei.kvs = ei.kvs[:0]
	ei.size = 0
}

func (ei *engineInfo) pushKV(k, v []byte) {
	klen := len(k)
	dlen := klen + len(v)
	ei.size = ei.size + dlen
	buf := make([]byte, dlen)
	copy(buf[:klen], k)
	copy(buf[klen:], v)
	ei.kvs = append(ei.kvs, common.KvPair{Key: buf[:klen], Val: buf[klen:]})
}

func (ec *engineCache) put(startTs uint64, cfg *backend.EngineConfig, en *backend.OpenedEngine) {
	ec.mtx.Lock()
	ec.cache[startTs] = &engineInfo{
		en,
		nil,
		cfg,
		0,
		nil,
		0,
	}
	ec.mtx.Unlock()
	LogDebug("put %d", startTs)
}

var (
	ErrNotFound       = errors.New("not object in this cache")
	ErrWasInUse       = errors.New("this object was in used")
	ec                = engineCache{cache: map[uint64]*engineInfo{}}
	cluster           ClusterInfo
	IndexDDLLightning = flag.Bool("ddl-mode", true, "index ddl use sst mode")
)

func (ec *engineCache) getEngineInfo(startTs uint64) (*engineInfo, error) {
	LogDebug("getEngineInfo by %d", startTs)
	ec.mtx.RLock()
	ei, ok := ec.cache[startTs]
	ec.mtx.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	if false == atomic.CompareAndSwapInt32(&ei.ref, 0, 1) {
		return nil, ErrWasInUse
	}
	return ei, nil
}

func (ec *engineCache) releaseRef(startTs uint64) {
	LogDebug("releaseRef by %d", startTs)
	ec.mtx.RLock()
	ei := ec.cache[startTs]
	ec.mtx.RUnlock()
	atomic.CompareAndSwapInt32(&ei.ref, 1, 0)
}

func (ec *engineCache) getWriter(startTs uint64) (*backend.LocalEngineWriter, error) {
	LogDebug("getWriter by %d", startTs)
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return nil, err
	}
	return ei.getWriter()
}

func (ei *engineInfo) getWriter() (*backend.LocalEngineWriter, error) {
	if ei.writer != nil {
		return ei.writer, nil
	}
	var err error
	ei.writer, err = ei.OpenedEngine.LocalWriter(context.TODO(), &backend.LocalWriterConfig{})
	if err != nil {
		return nil, err
	}
	return ei.writer, nil
}

type engineCache struct {
	cache map[uint64]*engineInfo
	mtx   sync.RWMutex
}
