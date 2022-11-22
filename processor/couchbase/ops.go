package couchbase

import (
	"fmt"

	"gopkg.in/couchbase/gocb.v1"
)

func errorFromOp(op gocb.BulkOp) error {
	switch o := op.(type) {
	case *gocb.GetOp:
		return o.Err
	case *gocb.InsertOp:
		return o.Err
	case *gocb.RemoveOp:
		return o.Err
	case *gocb.ReplaceOp:
		return o.Err
	case *gocb.UpsertOp:
		return o.Err
	}

	return fmt.Errorf("type not supported")
}

func valueFromOp(op gocb.BulkOp) any {
	switch o := op.(type) {
	case *gocb.GetOp:
		return *o.Value.(*any)
	case *gocb.InsertOp:
		return *o.Value.(*any)
	case *gocb.RemoveOp:
		return nil
	case *gocb.ReplaceOp:
		return *o.Value.(*any)
	case *gocb.UpsertOp:
		return *o.Value.(*any)
	}

	return fmt.Errorf("type not supported")
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
