package couchbase

import "github.com/couchbase/gocb/v2"

func get(collection *gocb.Collection) func(key string, data []byte) ([]byte, error) {
	return func(key string, data []byte) ([]byte, error) {
		res, err := collection.Get(key, nil)
		if err != nil {
			return nil, err
		}

		var out []byte
		err = res.Content(&out)
		return out, err
	}
}

func insert(collection *gocb.Collection) func(key string, data []byte) ([]byte, error) {
	return func(key string, data []byte) ([]byte, error) {
		_, err := collection.Insert(key, data, nil)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}

func remove(collection *gocb.Collection) func(key string, data []byte) ([]byte, error) {
	return func(key string, data []byte) ([]byte, error) {
		_, err := collection.Remove(key, nil)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}

func replace(collection *gocb.Collection) func(key string, data []byte) ([]byte, error) {
	return func(key string, data []byte) ([]byte, error) {
		_, err := collection.Replace(key, data, nil)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}

func upsert(collection *gocb.Collection) func(key string, data []byte) ([]byte, error) {
	return func(key string, data []byte) ([]byte, error) {
		_, err := collection.Upsert(key, data, nil)
		if err != nil {
			return nil, err
		}

		return nil, err
	}
}
