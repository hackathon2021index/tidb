package sst

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/twmb/murmur3"
)

const prefix_str = "------->"

func LogInfo(format string, a ...interface{}) {
	logutil.BgLogger().Info(prefix_str + fmt.Sprintf(format, a...))
}

func LogDebug(format string, a ...interface{}) {
	logutil.BgLogger().Debug(prefix_str + fmt.Sprintf(format, a...))
}

func LogError(format string, a ...interface{}) {
	logutil.BgLogger().Error(prefix_str + fmt.Sprintf(format, a...))
}

func LogFatal(format string, a ...interface{}) {
	logutil.BgLogger().Fatal(prefix_str + fmt.Sprintf(format, a...))
}

// pdaddr; tidb-host/status
type ClusterInfo struct {
	PdAddr string
	// TidbHost string - 127.0.0.1
	Port   uint
	Status uint
}

type DDLInfo struct {
	Schema  string
	Table   *model.TableInfo
	StartTs uint64
}

const (
	indexEngineID = -1 // same to restore.table_restore.go indexEngineID
)

type engineInfo struct {
	*backend.OpenedEngine
	writer *backend.LocalEngineWriter
	cfg    *backend.EngineConfig
	ref    int32
	kvs    []common.KvPair
	size   int
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

func InitIndexOptimize() {
	cfg := tidbcfg.GetGlobalConfig()
	cluster.PdAddr = cfg.AdvertiseAddress
	cluster.Port = cfg.Port
	cluster.Status = cfg.Status.StatusPort
	cluster.PdAddr = cfg.Path
	fmt.Printf("InitOnce %+v.\n", cluster)
	log.SetAppLogger(logutil.BgLogger())
}

// TODO: 1. checkpoint??
// TODO: 2. EngineID can use startTs for only.
func PrepareIndexOp(ctx context.Context, ddl DDLInfo) error {
	_, err := ec.getEngineInfo(ddl.StartTs)
	if err == nil {
		// has found it ;
		LogDebug("ddl %d has exist.")
		ec.releaseRef(ddl.StartTs)
		return nil
	}
	if err != ErrNotFound {
		return err
	}
	LogInfo("PrepareIndexOp %+v", ddl)
	// err == ErrNotFound
	info := cluster
	be, err := createLocalBackend(ctx, info)
	if err != nil {
		LogFatal("PrepareIndexOp.createLocalBackend err:%s.", err.Error())
		return fmt.Errorf("PrepareIndexOp.createLocalBackend err:%w", err)
	}
	cpt := checkpoints.TidbTableInfo{
		genNextTblId(),
		ddl.Schema,
		ddl.Table.Name.String(),
		ddl.Table,
	}
	var cfg backend.EngineConfig
	cfg.TableInfo = &cpt
	//
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ddl.StartTs)
	h := murmur3.New32()
	h.Write(b[:])
	en, err := be.OpenEngine(ctx, &cfg, ddl.Table.Name.String(), int32(h.Sum32()))
	if err != nil {
		return fmt.Errorf("PrepareIndexOp.OpenEngine err:%w", err)
	}
	ec.put(ddl.StartTs, &cfg, en)
	return nil
}

func IndexOperator(ctx context.Context, startTs uint64, k, v []byte) error {
	LogDebug("IndexOperator '%x','%x'", k, v)
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return err
	}
	defer ec.releaseRef(startTs)
	//

	ei.pushKV(k, v)
	kvSize := ei.size
	if kvSize < int(config.SplitRegionSize) {
		return nil
	}
	//
	return flushKvs(ctx, ei)
}

func flushKvs(ctx context.Context, ei *engineInfo) error {
	if len(ei.kvs) <= 0 {
		return nil
	}
	LogInfo("flushKvs (%d)", len(ei.kvs))
	lw, err := ei.getWriter()
	if err != nil {
		return fmt.Errorf("IndexOperator.getWriter err:%w", err)
	}

	err = lw.WriteRows(ctx, nil, kv.NewKvPairs(ei.kvs))
	if err != nil {
		return fmt.Errorf("IndexOperator.WriteRows err:%w", err)
	}
	return nil
}

func FinishIndexOp(ctx context.Context, startTs uint64) error {
	LogInfo("FinishIndexOp %d", startTs)
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return err
	}
	defer ec.releaseRef(startTs)
	flushKvs(ctx, ei)
	//
	indexEngine := ei.OpenedEngine
	cfg := ei.cfg
	//
	closeEngine, err1 := indexEngine.Close(ctx, cfg)
	if err1 != nil {
		return fmt.Errorf("engine.Close err:%w", err1)
	}
	// use default value first;
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize))
	if err != nil {
		return fmt.Errorf("engine.Import err:%w", err)
	}
	// TODO: do not comment.
	// err = closeEngine.Cleanup(ctx)
	if err != nil {
		return fmt.Errorf("engine.Cleanup err:%w", err)
	}
	return nil
}
