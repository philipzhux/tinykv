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
	// Your Code Here (1).
	reader,err:= server.storage.Reader(nil)
	ret:= &kvrpcpb.RawGetResponse{}
	if err!=nil {
		ret.Error = err.Error()
		return ret,err
	}
	defer reader.Close()
	response, err := reader.GetCF(req.GetCf(),req.GetKey())
	if response==nil {
		ret.NotFound = true
	} else {
		ret.Value = response
		if err!=nil {
			ret.Error = err.Error()
			return ret,err
		}
	}
	return ret, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	ret := &kvrpcpb.RawPutResponse{}
	batch := make([]storage.Modify,0)
	batch = append(batch, storage.Modify{
		Data: storage.Put{
			Key:req.GetKey(),
			Value: req.GetValue(),
			Cf: req.GetCf(),
		},
	})
	err := server.storage.Write(nil,batch)
	if err!=nil {
		ret.Error = err.Error()
	}
	return ret,err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	ret := &kvrpcpb.RawDeleteResponse{}
	batch := make([]storage.Modify,0)
	batch = append(batch, storage.Modify{
		Data: storage.Delete{
			Key:req.GetKey(),
			Cf: req.GetCf(),
		},
	})
	err := server.storage.Write(nil,batch)
	
	if err!=nil {
		ret.Error = err.Error()
		if ret.Error=="Key not found" {
			err = nil
		}
	}
	return ret,err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	ret := &kvrpcpb.RawScanResponse{}
	reader,err:= server.storage.Reader(nil)
	if err!=nil {
		ret.Error = err.Error()
		return ret,err
	}
	it:=reader.IterCF(req.GetCf())
	limit := req.GetLimit()
	defer it.Close()
	it.Seek(req.GetStartKey())
	ret.Kvs = make([]*kvrpcpb.KvPair,0)
	for i:=uint32(0);i<limit;i++ {
		if !it.Valid() {
			break
		}
		key := it.Item().KeyCopy(nil)
		value,err := it.Item().ValueCopy(nil)
		if err!=nil {
			ret.Error = err.Error()
			return ret,err
		} else {
			ret.Kvs = append(ret.Kvs, &kvrpcpb.KvPair{
				Key: key,
				Value: value,
			})
		}
		it.Next()
	}
	return ret, nil
}
