package couchbase

import (
	"fmt"

	"gopkg.in/couchbase/gocb.v1"
)

func valueFromOp(op gocb.BulkOp) (any, error) {
	switch o := op.(type) {
	case *gocb.GetOp:
		if o.Err != nil {
			return nil, o.Err
		}
		return *o.Value.(*any), nil
	case *gocb.InsertOp:
		return nil, o.Err
	case *gocb.RemoveOp:
		return nil, o.Err
	case *gocb.ReplaceOp:
		return nil, o.Err
	case *gocb.UpsertOp:
		return nil, o.Err
	}

	return nil, fmt.Errorf("type not supported")
}

func get(key string, _ []byte) gocb.BulkOp {
	var out any
	return &gocb.GetOp{
		Key:   key,
		Value: &out,
	}
}

func insert(key string, data []byte) gocb.BulkOp {
	return &gocb.InsertOp{
		Key:   key,
		Value: data,
	}
}

func remove(key string, _ []byte) gocb.BulkOp {
	return &gocb.RemoveOp{
		Key: key,
	}
}

func replace(key string, data []byte) gocb.BulkOp {
	return &gocb.ReplaceOp{
		Key:   key,
		Value: data,
	}
}

func upsert(key string, data []byte) gocb.BulkOp {
	return &gocb.UpsertOp{
		Key:   key,
		Value: data,
	}
}
