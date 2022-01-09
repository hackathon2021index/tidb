package sst

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/parser/model"

	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
)

type engineInfo struct {
	backend      backend.Backend
	OpenedEngine *backend.OpenedEngine
	writer       *backend.LocalEngineWriter
	cfg          *backend.EngineConfig
	// TODO: use channel later;
	ref  int32
	kvs  []common.KvPair
	size int
	tbl  *model.TableInfo
}

func (ei *engineInfo) ResetCache() {
	ei.kvs = ei.kvs[:0]
	ei.size = 0
}

func (ec *engineCache) put(startTs uint64, cfg *backend.EngineConfig, be backend.Backend, en *backend.OpenedEngine, tbl *model.TableInfo) {
	ec.mtx.Lock()
	ec.cache[startTs] = &engineInfo{
		be,
		en,
		nil,
		cfg,
		0,
		nil,
		0,
		tbl,
	}
	ec.mtx.Unlock()
	LogDebug("put %d", startTs)
}

var (
	ErrNotFound = errors.New("not object in this cache")
	ErrWasInUse = errors.New("this object was in used")
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
	if ei == nil {
		return
	}
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

func (ec *engineCache) ReleaseEngine(startTs uint64) {
	ec.mtx.Lock()
	delete(ec.cache, startTs)
	ec.mtx.Unlock()
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
