package couchbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/gocb/v2"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	ErrInvalidOperation  = errors.New("invalid operation")
	ErrInvalidTranscoder = errors.New("invalid transcoder")
	ErrValueRequired     = errors.New("value required")
)

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec().Summary(
		"retrieve and update couchbase document",
	).
		Categories("Integration").
		Field(service.NewStringField("server")).
		Field(service.NewStringField("username").Optional()).
		Field(service.NewStringField("password").Optional()).
		Field(service.NewStringField("bucket")).
		Field(service.NewStringField("collection").Default("_default").Advanced().Optional()).
		Field(service.NewInterpolatedStringField("key").Default(`${! content() }`)).
		Field(service.NewBloblangField("value").Optional()).
		Field(service.NewStringEnumField("operation", "get", "insert", "remove", "replace", "upsert" /* add more , "exist" */).Default("get")).
		Field(service.NewStringEnumField("transcoder", "raw", "rawjson", "rawstring", "json", "legacy" /* add more , "exist" */).Default("raw").Advanced()).
		Field(service.NewDurationField("timeout").Advanced().Optional())

	// TODO add retry, more timeout configuration ...

	err := service.RegisterBatchProcessor("couchbase", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return new(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type couchbaseProcessor struct {
	collection *gocb.Collection
	cluster    *gocb.Cluster
	logger     *service.Logger
	//metrics *service.Metrics
	key   *service.InterpolatedString
	value *bloblang.Executor
	op    func(key string, data []byte) gocb.BulkOp
}

func new(conf *service.ParsedConfig, mgr *service.Resources) (*couchbaseProcessor, error) {
	// The logger and metrics components will already be labelled with the
	// identifier of this component within a config.

	// retrieve params
	server, err := conf.FieldString("server")
	if err != nil {
		return nil, err
	}
	bucket, err := conf.FieldString("bucket")
	if err != nil {
		return nil, err
	}
	timeout, err := conf.FieldDuration("timeout")
	if err != nil {
		return nil, err
	}

	// setup couchbase
	opts := gocb.ClusterOptions{
		// TODO add more configuration
		// TODO Tracer:   mgr.OtelTracer().Tracer(name).Start(context.Background(), operationName)
		// TODO Meter:    mgr.Metrics(),
	}

	if conf.Contains("timeout") {
		opts.TimeoutsConfig = gocb.TimeoutsConfig{
			ConnectTimeout:    timeout,
			KVTimeout:         timeout,
			KVDurableTimeout:  timeout,
			ViewTimeout:       timeout,
			QueryTimeout:      timeout,
			AnalyticsTimeout:  timeout,
			SearchTimeout:     timeout,
			ManagementTimeout: timeout,
		}
	}

	if conf.Contains("username") {
		username, err := conf.FieldString("username")
		if err != nil {
			return nil, err
		}
		password, err := conf.FieldString("password")
		if err != nil {
			return nil, err
		}
		opts.Authenticator = gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	if conf.Contains("transcoder") {
		tr, err := conf.FieldString("transcoder")
		if err != nil {
			return nil, err
		}
		switch tr {
		case "json":
			opts.Transcoder = gocb.NewJSONTranscoder() // maybe not supported
		case "raw":
			opts.Transcoder = gocb.NewRawBinaryTranscoder()
		case "rawjson":
			opts.Transcoder = gocb.NewRawJSONTranscoder()
		case "rawstring":
			opts.Transcoder = gocb.NewRawStringTranscoder()
		case "legacy":
			opts.Transcoder = gocb.NewLegacyTranscoder()
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidTranscoder, tr)
		}
	} else {
		opts.Transcoder = gocb.NewRawBinaryTranscoder()
	}

	cluster, err := gocb.Connect(server, opts)
	if err != nil {
		return nil, err
	}

	// check that we can do query
	err = cluster.Bucket(bucket).WaitUntilReady(timeout, nil)
	if err != nil {
		return nil, err
	}

	proc := &couchbaseProcessor{
		cluster: cluster,
		logger:  mgr.Logger(),
	}

	if conf.Contains("key") {
		if proc.key, err = conf.FieldInterpolatedString("key"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("value") {
		if proc.value, err = conf.FieldBloblang("value"); err != nil {
			return nil, err
		}
	}

	// retrieve collection
	if conf.Contains("collection") {
		collectionStr, err := conf.FieldString("collection")
		if err != nil {
			return nil, err
		}
		proc.collection = cluster.Bucket(bucket).Collection(collectionStr)
	} else {
		proc.collection = cluster.Bucket(bucket).DefaultCollection()
	}

	if conf.Contains("operation") {
		op, err := conf.FieldString("operation")
		if err != nil {
			return nil, err
		}
		switch op {
		case "get":
			proc.op = get
		case "remove":
			proc.op = remove
		case "insert":
			if proc.value == nil {
				return nil, ErrValueRequired
			}
			proc.op = insert
		case "replace":
			if proc.value == nil {
				return nil, ErrValueRequired
			}
			proc.op = replace
		case "upsert":
			if proc.value == nil {
				return nil, ErrValueRequired
			}
			proc.op = upsert
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidOperation, op)
		}
	} else {
		proc.op = get
	}

	return proc, nil
}

func (p *couchbaseProcessor) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {
	newMsg := inBatch.Copy()
	ops := make([]gocb.BulkOp, len(inBatch))

	// generate query
	for index := range newMsg {
		// generate key
		k := inBatch.InterpolatedString(index, p.key)

		// generate content
		var content []byte
		if p.value != nil {
			res, err := inBatch.BloblangQuery(index, p.value)
			if err != nil {
				return nil, err
			}
			content, err = res.AsBytes()
			if err != nil {
				return nil, err
			}
		}

		ops[index] = p.op(k, content)
	}

	// execute
	err := p.collection.Do(ops, &gocb.BulkOpOptions{})
	if err != nil {
		return nil, err
	}

	// set results
	for index, part := range newMsg {
		out, err := valueFromOp(ops[index])
		if err != nil {
			part.SetError(fmt.Errorf("couchbase operator failed: %w", err))
		}

		if data, ok := out.([]byte); ok {
			part.SetBytes(data)
		} else if out != nil {
			part.SetStructured(out)
		}
	}

	return []service.MessageBatch{newMsg}, nil
}

func (p *couchbaseProcessor) Close(ctx context.Context) error {
	return p.cluster.Close(&gocb.ClusterCloseOptions{})
}
