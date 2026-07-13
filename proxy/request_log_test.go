package proxy

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRequestLogSummaryTruncatesPrompt(t *testing.T) {
	secretTail := "SECRET_TAIL_SHOULD_NOT_BE_SAVED"
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{{
			Role:    "user",
			Content: strings.Repeat("a", requestLogPromptMaxChars+50) + secretTail,
		}},
	}

	summary := summarizeClaudeRequest(req)
	if strings.Contains(summary, secretTail) {
		t.Fatalf("expected long prompt tail to be truncated, got %q", summary)
	}
	if len([]rune(summary)) > requestLogPromptMaxChars+len("user: ...") {
		t.Fatalf("summary unexpectedly large: %d runes", len([]rune(summary)))
	}
}

func TestMaskProxyURLRedactsCredentials(t *testing.T) {
	got := maskProxyURL("socks5://user:pass@127.0.0.1:1080")
	if strings.Contains(got, "user") || strings.Contains(got, "pass") {
		t.Fatalf("expected proxy credentials to be redacted, got %q", got)
	}
	if got != "socks5://****@127.0.0.1:1080" {
		t.Fatalf("unexpected masked proxy: %q", got)
	}
}

func TestSQLiteRequestLogStoreInsert(t *testing.T) {
	store := NewSQLiteRequestLogStore(filepath.Join(t.TempDir(), "request_logs.db"))
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatalf("init store: %v", err)
	}
	defer store.Close()

	event := &RequestLogEvent{
		ID:              "reqlog_test",
		StartedAt:       time.Now().Unix(),
		FinishedAt:      time.Now().Unix(),
		DurationMs:      12,
		Path:            "/v1/messages",
		Protocol:        "claude",
		Model:           "claude-sonnet-4.5",
		HTTPStatus:      200,
		Status:          "success",
		InputTokens:     10,
		OutputTokens:    5,
		TotalTokens:     15,
		Credits:         0.25,
		RequestSummary:  "user: hello",
		ResponseSummary: "hi",
		Attempts: []RequestLogAttempt{{
			AccountID:     "acc_1",
			AccountEmail:  "user@example.com",
			Status:        "success",
			AttemptNumber: 1,
			StartedAt:     time.Now().Unix(),
			FinishedAt:    time.Now().Unix(),
			DurationMs:    10,
		}},
	}
	if err := store.Insert(ctx, event); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	var count int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM request_logs WHERE id = ?", event.ID).Scan(&count); err != nil {
		t.Fatalf("query log count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one request log, got %d", count)
	}
	if err := store.db.QueryRow("SELECT COUNT(*) FROM request_log_attempts WHERE log_id = ?", event.ID).Scan(&count); err != nil {
		t.Fatalf("query attempt count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one request log attempt, got %d", count)
	}
}

func TestAsyncRequestLoggerDropsWhenQueueFull(t *testing.T) {
	rl := &AsyncRequestLogger{
		queue:        make(chan *RequestLogEvent, 1),
		dropWhenFull: true,
	}
	rl.enabled.Store(true)

	rl.Enqueue(&RequestLogEvent{ID: "one"})
	rl.Enqueue(&RequestLogEvent{ID: "two"})

	if got := rl.dropped.Load(); got != 1 {
		t.Fatalf("expected one dropped log, got %d", got)
	}
}

func TestAsyncRequestLoggerHealthUnavailable(t *testing.T) {
	rl := &AsyncRequestLogger{
		storeName: "sqlite",
		queue:     make(chan *RequestLogEvent, 1),
		store:     failingRequestLogStore{},
	}
	rl.enabled.Store(true)
	health := rl.Health()
	if health["available"] != false {
		t.Fatalf("expected unavailable health, got %#v", health)
	}
	if health["lastError"] == "" {
		t.Fatalf("expected lastError in health, got %#v", health)
	}
}

func TestRequestLogBuilderCapturesMetadata(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	b := newRequestLogBuilder(r, "claude", "claude-sonnet-4.5", false, true, []byte(`{"x":1}`), "hello")
	b.finishSuccess(nil, nil, 200, 10, 2, 0.1, strings.Repeat("r", requestLogResponseMaxChars+10), 0, "end_turn", promptCacheUsage{
		CacheCreationInputTokens:   3,
		CacheReadInputTokens:       4,
		CacheCreation5mInputTokens: 1,
		CacheCreation1hInputTokens: 2,
	})

	if b.event.TotalTokens != 12 {
		t.Fatalf("expected total tokens 12, got %d", b.event.TotalTokens)
	}
	if b.event.CacheReadTokens != 4 || b.event.CacheWriteTokens != 3 {
		t.Fatalf("unexpected cache tokens: %+v", b.event)
	}
	if len([]rune(b.event.ResponseSummary)) > requestLogResponseMaxChars+3 {
		t.Fatalf("response summary not truncated")
	}
}

type failingRequestLogStore struct{}

func (failingRequestLogStore) Init(context.Context) error { return errors.New("boom") }
func (failingRequestLogStore) Insert(context.Context, *RequestLogEvent) error {
	return errors.New("boom")
}
func (failingRequestLogStore) Health(context.Context) error { return errors.New("boom") }
func (failingRequestLogStore) Close() error                 { return nil }
