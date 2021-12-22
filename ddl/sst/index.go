package sst

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
)

// pdaddr; tidb-host/status
type ClusterInfo struct {
	PdAddr string
	// TidbHost string - 127.0.0.1
	Port   int
	Status int
}

const (
	indexEngineID = -1 // same to restore.table_restore.go indexEngineID
)

// TODO: 1. checkpoint??
// TODO: 2. checkpoint??
func PrepareIndexOp(ctx context.Context, info ClusterInfo, dbname string, tbl *model.TableInfo, kvs <-chan kv.KvPairs) (*backend.OpenedEngine,error) {
	be, err := createLocalBackend(ctx, info)
	if err != nil {
		return nil,err
	}
	cpt := checkpoints.TidbTableInfo{
		genNextTblId(),
		dbname,
		tbl.Name.String(),
		tbl,
	}
	var cfg backend.EngineConfig
	cfg.TableInfo = &cpt
	return be.OpenEngine(ctx, &cfg, tbl.Name.String(), indexEngineID)
}

// stop this routine by close(kvs) or some context error.
func RunIndexOpRoutine(ctx context.Context, engine *backend.OpenedEngine, cfg *backend.EngineConfig, kvs <-chan kv.KvPairs) error {
	logutil.BgLogger().Info("createIndex-routine on dbname.tbl")

	running := true
	for running {
		select {
		case <-ctx.Done():
			fmt.Errorf("RunIndexOpRoutine was exit by Context.Done")
		case kvp, close := <-kvs:
			if close {
				running = false
				break
			}
			err := process(ctx, engine, kvp)
			if err != nil {
				return fmt.Errorf("process err:%s.clean data.", err.Error())
			}
		}
	}
	logutil.BgLogger().Info("createIndex-routine on dbname.tbl exit...")
	return nil
}

func FinishIndexOp(ctx context.Context, indexEngine *backend.OpenedEngine,cfg *backend.EngineConfig )error{
	closeEngine, err1 := indexEngine.Close(ctx, cfg)
	if err1 != nil {
		return fmt.Errorf("engine.Close err:%s", err1.Error())
	}
	// use default value first;
	err := closeEngine.Import(ctx, int64(config.SplitRegionSize))
	if err != nil {
		return fmt.Errorf("engine.Import err:%s", err.Error())
	}
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		return fmt.Errorf("engine.Cleanup err:%s", err.Error())
	}
	return nil
}

func process(ctx context.Context, indexEngine *backend.OpenedEngine, kvp kv.KvPairs) error {
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	if err != nil {
		return fmt.Errorf("LocalWriter err:%s", err.Error())
	}
	// columnNames 可以不需要，因为我们肯定是 非 sorted 的数据.
	err = indexWriter.WriteRows(ctx, nil, &kvp)
	if err != nil {
		indexWriter.Close(ctx)
		return fmt.Errorf("WriteRows err:%s", err.Error())
	}
	return nil
}
