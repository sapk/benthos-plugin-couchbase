package couchbase

import "gopkg.in/couchbase/gocb.v1"

func get(bucket *gocb.Bucket) func(key string, data []byte) (any, error) {
	return func(key string, data []byte) (any, error) {
		var out any
		_, err := bucket.Get(key, &out)
		if err != nil {
			return nil, err
		}

		return out, err
	}
}

func insert(bucket *gocb.Bucket) func(key string, data []byte) (any, error) {
	return func(key string, data []byte) (any, error) {
		_, err := bucket.Insert(key, data, 0)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}

func remove(bucket *gocb.Bucket) func(key string, data []byte) (any, error) {
	return func(key string, data []byte) (any, error) {
		_, err := bucket.Remove(key, 0)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}

func replace(bucket *gocb.Bucket) func(key string, data []byte) (any, error) {
	return func(key string, data []byte) (any, error) {
		_, err := bucket.Replace(key, data, 0, 0)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}

func upsert(bucket *gocb.Bucket) func(key string, data []byte) (any, error) {
	return func(key string, data []byte) (any, error) {
		_, err := bucket.Upsert(key, data, 0)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}
