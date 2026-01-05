package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kvPath := conf.DBPath + "/data"
	db := engine_util.CreateDB(kvPath, false)
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &reader{db: s.db}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.db, data.Cf, data.Key, data.Value); err != nil {
				return err
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.db, data.Cf, data.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

type reader struct {
	db  *badger.DB
	txs []*badger.Txn
}

func (r *reader) GetCF(cf string, key []byte) (value []byte, err error) {
	value, err = engine_util.GetCF(r.db, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
	}
	return
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	tx := r.db.NewTransaction(false)
	r.txs = append(r.txs, tx)
	return engine_util.NewCFIterator(cf, tx)
}

func (r *reader) Close() {
	for _, tx := range r.txs {
		if tx == nil {
			continue
		}
		tx.Discard()
	}
}
