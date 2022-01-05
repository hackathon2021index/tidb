package sst

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/twmb/murmur3"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
)

func InitIndexOptimize() {
	cfg := tidbcfg.GetGlobalConfig()
	cluster.PdAddr = cfg.AdvertiseAddress
	cluster.Port = cfg.Port
	cluster.Status = cfg.Status.StatusPort
	cluster.PdAddr = cfg.Path
	fmt.Printf("InitOnce %+v.ck=%v\n", cluster, *ck)
	log.SetAppLogger(logutil.BgLogger())
}

// TODO: 1. checkpoint??
// TODO: 2. EngineID can use startTs for only.
func PrepareIndexOp(ctx context.Context, ddl DDLInfo) error {
	_, err := ec.getEngineInfo(ddl.StartTs)
	if err == nil {
		// has found it ;
		LogDebug("ddl %d has exist.", ddl.StartTs)
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
		ddl.StartTs,
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
	LogFatal("IndexOperator logic error")
	LogDebug("IndexOperator '%x','%x'", k, v)
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return err
	}
	defer ec.releaseRef(startTs)
	//

	ei.pushKV(k, v)
	kvSize := ei.size
	if kvSize < flush_size {
		return nil
	}
	//
	return flushKvs(ctx, ei)
}

func flushKvs(ctx context.Context, ei *engineInfo) error {
	if len(ei.kvs) <= 0 {
		return nil
	}
	if ctx == nil {
		// this may be nil,and used in WriteRows;
		ctx = context.TODO()
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
	ei.ResetCache()
	return nil
}

func FlushKeyValSync(ctx context.Context, startTs uint64, cache *WorkerKVCache) error {
	ec.mtx.RLock()
	ei, ok := ec.cache[startTs]
	defer ec.mtx.RUnlock()
	if !ok {
		return ErrNotFound
	}
	lw, err := ei.getWriter()
	if err != nil {
		return fmt.Errorf("IndexOperator.getWriter err:%w", err)
	}
	err = lw.WriteRows(ctx, nil, kv.NewKvPairs(cache.Fetch()))
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
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		return fmt.Errorf("engine.Cleanup err:%w", err)
	}
	return nil
}
