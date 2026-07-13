package proxy

import (
	"context"
	"fmt"
	"kiro-go/config"
	"kiro-go/logger"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type AsyncRequestLogger struct {
	store        RequestLogStore
	storeName    string
	queue        chan *RequestLogEvent
	enabled      atomic.Bool
	dropWhenFull bool
	dropped      atomic.Int64
	lastErr      atomic.Value
	stop         chan struct{}
	wg           sync.WaitGroup
}

func NewAsyncRequestLogger() *AsyncRequestLogger {
	cfg := config.GetRequestLogConfig()
	rl := &AsyncRequestLogger{
		storeName:    cfg.Store,
		queue:        make(chan *RequestLogEvent, max(cfg.QueueSize, 1)),
		dropWhenFull: cfg.DropWhenFull,
		stop:         make(chan struct{}),
	}
	if !cfg.Enabled {
		rl.lastErr.Store("request log collection disabled")
		return rl
	}

	switch strings.ToLower(strings.TrimSpace(cfg.Store)) {
	case "", "sqlite":
		rl.storeName = "sqlite"
		rl.store = NewSQLiteRequestLogStore(resolveRequestLogDBPath(cfg.DBPath))
	default:
		rl.setError(fmt.Errorf("unsupported request log store: %s", cfg.Store))
		return rl
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rl.store.Init(ctx); err != nil {
		rl.setError(err)
		logger.Warnf("[RequestLog] init failed: %v", err)
		return rl
	}

	rl.enabled.Store(true)
	rl.wg.Add(1)
	go rl.worker()
	return rl
}

func (rl *AsyncRequestLogger) Enqueue(event *RequestLogEvent) {
	if rl == nil || event == nil || !rl.enabled.Load() {
		return
	}
	if rl.dropWhenFull {
		select {
		case rl.queue <- event:
		default:
			rl.dropped.Add(1)
		}
		return
	}
	select {
	case rl.queue <- event:
	default:
		rl.dropped.Add(1)
	}
}

func (rl *AsyncRequestLogger) Health() map[string]interface{} {
	out := map[string]interface{}{
		"enabled":   false,
		"store":     "",
		"available": false,
		"queueLen":  0,
		"dropped":   0,
	}
	if rl == nil {
		out["lastError"] = "request logger not initialized"
		return out
	}
	out["enabled"] = rl.enabled.Load()
	out["store"] = rl.storeName
	out["available"] = rl.enabled.Load()
	out["queueLen"] = len(rl.queue)
	out["dropped"] = int(rl.dropped.Load())
	if v := rl.lastErr.Load(); v != nil {
		out["lastError"] = v.(string)
	}
	if rl.store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := rl.store.Health(ctx); err != nil {
			out["available"] = false
			out["lastError"] = err.Error()
		}
	}
	return out
}

func (rl *AsyncRequestLogger) Close() error {
	if rl == nil {
		return nil
	}
	if rl.enabled.Load() {
		close(rl.stop)
		rl.wg.Wait()
	}
	if rl.store != nil {
		return rl.store.Close()
	}
	return nil
}

func (rl *AsyncRequestLogger) worker() {
	defer rl.wg.Done()
	for {
		select {
		case event := <-rl.queue:
			rl.write(event)
		case <-rl.stop:
			for {
				select {
				case event := <-rl.queue:
					rl.write(event)
				default:
					return
				}
			}
		}
	}
}

func (rl *AsyncRequestLogger) write(event *RequestLogEvent) {
	if event == nil || rl.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rl.store.Insert(ctx, event); err != nil {
		rl.setError(err)
		logger.Warnf("[RequestLog] write failed: %v", err)
	}
}

func (rl *AsyncRequestLogger) setError(err error) {
	if err != nil {
		rl.lastErr.Store(err.Error())
	}
}
