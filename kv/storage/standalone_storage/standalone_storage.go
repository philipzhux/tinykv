package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		engine: engine_util.NewEngines(
			engine_util.CreateDB(conf.DBPath,conf.Raft),
			nil,
			conf.DBPath,
			"",
		),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Destroy()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		txn: s.engine.Kv.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := &engine_util.WriteBatch{}
	for _,entry:= range batch {
		switch ent:=entry.Data.(type){
		case storage.Delete:
			wb.DeleteCF(ent.Cf,ent.Key)
		case storage.Put:
			wb.SetCF(ent.Cf,ent.Key,ent.Value)
		}
	}
	return wb.WriteToDB(s.engine.Kv)
}



func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	b,err := engine_util.GetCFFromTxn(r.txn,cf,key)
	if err!=nil && err.Error()=="Key not found"{
		err = nil
	}
	return b,err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf,r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
