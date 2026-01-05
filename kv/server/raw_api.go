package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{Value: value, NotFound: value == nil}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	batch := []storage.Modify{
		{Data: storage.Put{Cf: req.GetCf(), Key: req.GetKey(), Value: req.GetValue()}},
	}
	if err := server.storage.Write(req.GetContext(), batch); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	batch := []storage.Modify{
		{Data: storage.Delete{Cf: req.GetCf(), Key: req.GetKey()}},
	}
	if err := server.storage.Write(req.GetContext(), batch); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	it := reader.IterCF(req.GetCf())
	defer it.Close()
	it.Seek(req.GetStartKey())

	res := &kvrpcpb.RawScanResponse{Kvs: make([]*kvrpcpb.KvPair, 0)}

	i := uint32(0)
	for it.Valid() && i < req.GetLimit() {
		pair := it.Item()
		key := pair.KeyCopy(nil)
		value, err := pair.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		it.Next()
		i++
	}

	return res, nil
}
