package proxy

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type SQLiteRequestLogStore struct {
	path string
	db   *sql.DB
}

func NewSQLiteRequestLogStore(path string) *SQLiteRequestLogStore {
	return &SQLiteRequestLogStore{path: path}
}

func (s *SQLiteRequestLogStore) Init(ctx context.Context) error {
	if s == nil {
		return errors.New("sqlite request log store is nil")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", s.path+"?_pragma=busy_timeout(1000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return err
	}
	s.db = db
	if err := s.initSchema(ctx); err != nil {
		_ = db.Close()
		s.db = nil
		return err
	}
	return nil
}

func (s *SQLiteRequestLogStore) Insert(ctx context.Context, event *RequestLogEvent) error {
	if s == nil || s.db == nil {
		return errors.New("request log database unavailable")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO request_logs (
		id, started_at, finished_at, duration_ms, path, protocol, stream, client_ip, model, thinking,
		request_summary, request_size, api_key_id, api_key_name, api_key_masked,
		account_id, account_email, account_nickname, account_auth_method, account_proxy,
		http_status, status, error_type, error_message, input_tokens, output_tokens, total_tokens,
		credits, cache_read_tokens, cache_write_tokens, cache_creation_5m_tokens, cache_creation_1h_tokens,
		upstream_endpoint, retry_count, failed_accounts, response_summary, tool_use_count, stop_reason, metadata_json
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, event.StartedAt, event.FinishedAt, event.DurationMs, event.Path, event.Protocol, boolInt(event.Stream), event.ClientIP, event.Model, boolInt(event.Thinking),
		event.RequestSummary, event.RequestSize, event.APIKeyID, event.APIKeyName, event.APIKeyMasked,
		event.AccountID, event.AccountEmail, event.AccountNickname, event.AccountAuthMethod, event.AccountProxy,
		event.HTTPStatus, event.Status, event.ErrorType, event.ErrorMessage, event.InputTokens, event.OutputTokens, event.TotalTokens,
		event.Credits, event.CacheReadTokens, event.CacheWriteTokens, event.CacheCreation5mTokens, event.CacheCreation1hTokens,
		event.UpstreamEndpoint, event.RetryCount, event.FailedAccounts, event.ResponseSummary, event.ToolUseCount, event.StopReason, event.MetadataJSON)
	if err != nil {
		return err
	}
	for _, a := range event.Attempts {
		_, err = tx.ExecContext(ctx, `INSERT INTO request_log_attempts (
			log_id, attempt_number, account_id, account_email, status, error_message, started_at, finished_at, duration_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			event.ID, a.AttemptNumber, a.AccountID, a.AccountEmail, a.Status, a.ErrorMessage, a.StartedAt, a.FinishedAt, a.DurationMs)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLiteRequestLogStore) Health(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("request log database unavailable")
	}
	return s.db.PingContext(ctx)
}

func (s *SQLiteRequestLogStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteRequestLogStore) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS request_logs (
			id TEXT PRIMARY KEY,
			started_at INTEGER NOT NULL,
			finished_at INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			path TEXT,
			protocol TEXT,
			stream INTEGER,
			client_ip TEXT,
			model TEXT,
			thinking INTEGER,
			request_summary TEXT,
			request_size INTEGER,
			api_key_id TEXT,
			api_key_name TEXT,
			api_key_masked TEXT,
			account_id TEXT,
			account_email TEXT,
			account_nickname TEXT,
			account_auth_method TEXT,
			account_proxy TEXT,
			http_status INTEGER,
			status TEXT,
			error_type TEXT,
			error_message TEXT,
			input_tokens INTEGER,
			output_tokens INTEGER,
			total_tokens INTEGER,
			credits REAL,
			cache_read_tokens INTEGER,
			cache_write_tokens INTEGER,
			cache_creation_5m_tokens INTEGER,
			cache_creation_1h_tokens INTEGER,
			upstream_endpoint TEXT,
			retry_count INTEGER,
			failed_accounts INTEGER,
			response_summary TEXT,
			tool_use_count INTEGER,
			stop_reason TEXT,
			metadata_json TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS request_log_attempts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			log_id TEXT NOT NULL,
			attempt_number INTEGER,
			account_id TEXT,
			account_email TEXT,
			status TEXT,
			error_message TEXT,
			started_at INTEGER,
			finished_at INTEGER,
			duration_ms INTEGER,
			FOREIGN KEY(log_id) REFERENCES request_logs(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_started_at ON request_logs(started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_status ON request_logs(status)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_protocol ON request_logs(protocol)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_model ON request_logs(model)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_account_id ON request_logs(account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_api_key_id ON request_logs(api_key_id)`,
		`CREATE INDEX IF NOT EXISTS idx_request_logs_http_status ON request_logs(http_status)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
