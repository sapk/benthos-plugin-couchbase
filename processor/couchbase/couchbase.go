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

	err := service.RegisterProcessor("couchbase", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
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
	op    func(key string, data []byte) ([]byte, error)
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

	/*
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

	*/

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
			proc.op = get(proc.bucket)
		case "remove":
			proc.op = remove(proc.bucket)
		case "insert":
			if proc.value == nil {
				return nil, ErrValueRequired
			}
			proc.op = insert(proc.bucket)
		case "replace":
			if proc.value == nil {
				return nil, ErrValueRequired
			}
			proc.op = replace(proc.bucket)
		case "upsert":
			if proc.value == nil {
				return nil, ErrValueRequired
			}
			proc.op = upsert(proc.bucket)
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidOperation, op)
		}
	} else {
		proc.op = get(proc.bucket)
	}

	return proc, nil
}

// TODO implement ProcessBatch ?

func (p *couchbaseProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	k := p.key.String(m)

	var content []byte

	if p.value != nil {
		res, err := m.BloblangQuery(p.value)
		if err != nil {
			return nil, err
		}
		content, err = res.AsBytes()
		if err != nil {
			return nil, err
		}
	}

	// p.logger.With("key", k).Debugf("query")

	newBytes, err := p.op(k, content)
	if err != nil {
		return nil, err
	}

	// p.logger.With("key", k).Debugf("result: %s", string(newBytes))

	m.SetBytes(newBytes)
	return []*service.Message{m}, nil
}

func (p *couchbaseProcessor) Close(ctx context.Context) error {
	if err := p.bucket.Close(); err != nil {
		return err
	}

	return p.cluster.Close()
}
