package proxy

import "context"

type RequestLogStore interface {
	Init(ctx context.Context) error
	Insert(ctx context.Context, event *RequestLogEvent) error
	Health(ctx context.Context) error
	Close() error
}
