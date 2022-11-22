package couchbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
	"gopkg.in/couchbase/gocb.v1"
)

var (
	ErrInvalidOperation = errors.New("invalid operation")
	ErrValueRequired    = errors.New("value required")
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
	cluster *gocb.Cluster
	bucket  *gocb.Bucket
	logger  *service.Logger
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
	cluster, err := gocb.Connect(server)
	if err != nil {
		return nil, err
	}

	if conf.Contains("timeout") {
		cluster.SetAnalyticsTimeout(timeout)
		cluster.SetConnectTimeout(timeout)
		cluster.SetFtsTimeout(timeout)
		cluster.SetN1qlTimeout(timeout)
		cluster.SetNmvRetryDelay(timeout)
		cluster.SetServerConnectTimeout(timeout)
	}

	// Auth
	if conf.Contains("username") {
		username, err := conf.FieldString("username")
		if err != nil {
			return nil, err
		}
		password, err := conf.FieldString("password")
		if err != nil {
			return nil, err
		}
		auth := gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
		err = cluster.Authenticate(auth)
		if err != nil {
			return nil, err
		}
	}

	//cluster.SetTracer(mgr.OtelTracer())

	proc := &couchbaseProcessor{
		cluster: cluster,
		logger:  mgr.Logger(),
	}

	// open bucket
	proc.bucket, err = cluster.OpenBucket(bucket, "") // TODO add bucket auth ?
	if err != nil {
		return nil, err
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
	err := p.bucket.Do(ops)
	if err != nil {
		return nil, err
	}

	// set results
	for index, part := range newMsg {
		if err := errorFromOp(ops[index]); err != nil {
			part.SetError(fmt.Errorf("couchbase operator failed: %w", err))
		}

		out := valueFromOp(ops[index])

		if data, ok := out.([]byte); ok {
			part.SetBytes(data)
		} else if out != nil {
			part.SetStructured(out)
		}
	}

	return []service.MessageBatch{newMsg}, nil
}

func (p *couchbaseProcessor) Close(ctx context.Context) error {
	if err := p.bucket.Close(); err != nil {
		return err
	}

	return p.cluster.Close()
}
