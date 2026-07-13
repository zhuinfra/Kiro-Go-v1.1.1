package proxy

import (
	"encoding/json"
	"fmt"
	"kiro-go/config"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	requestLogResponseMaxChars = 1000
	requestLogPromptMaxChars   = 500
)

type RequestLogEvent struct {
	ID                    string              `json:"id"`
	StartedAt             int64               `json:"startedAt"`
	FinishedAt            int64               `json:"finishedAt"`
	DurationMs            int64               `json:"durationMs"`
	Path                  string              `json:"path"`
	Protocol              string              `json:"protocol"`
	Stream                bool                `json:"stream"`
	ClientIP              string              `json:"clientIp"`
	Model                 string              `json:"model"`
	Thinking              bool                `json:"thinking"`
	RequestSummary        string              `json:"requestSummary"`
	RequestSize           int                 `json:"requestSize"`
	APIKeyID              string              `json:"apiKeyId"`
	APIKeyName            string              `json:"apiKeyName"`
	APIKeyMasked          string              `json:"apiKeyMasked"`
	AccountID             string              `json:"accountId"`
	AccountEmail          string              `json:"accountEmail"`
	AccountNickname       string              `json:"accountNickname"`
	AccountAuthMethod     string              `json:"accountAuthMethod"`
	AccountProxy          string              `json:"accountProxy"`
	HTTPStatus            int                 `json:"httpStatus"`
	Status                string              `json:"status"`
	ErrorType             string              `json:"errorType"`
	ErrorMessage          string              `json:"errorMessage"`
	InputTokens           int                 `json:"inputTokens"`
	OutputTokens          int                 `json:"outputTokens"`
	TotalTokens           int                 `json:"totalTokens"`
	Credits               float64             `json:"credits"`
	CacheReadTokens       int                 `json:"cacheReadTokens"`
	CacheWriteTokens      int                 `json:"cacheWriteTokens"`
	CacheCreation5mTokens int                 `json:"cacheCreation5mTokens"`
	CacheCreation1hTokens int                 `json:"cacheCreation1hTokens"`
	UpstreamEndpoint      string              `json:"upstreamEndpoint"`
	RetryCount            int                 `json:"retryCount"`
	FailedAccounts        int                 `json:"failedAccounts"`
	ResponseSummary       string              `json:"responseSummary"`
	ToolUseCount          int                 `json:"toolUseCount"`
	StopReason            string              `json:"stopReason"`
	MetadataJSON          string              `json:"metadataJson"`
	Attempts              []RequestLogAttempt `json:"attempts,omitempty"`
}

type RequestLogAttempt struct {
	AccountID     string `json:"accountId"`
	AccountEmail  string `json:"accountEmail"`
	Status        string `json:"status"`
	ErrorMessage  string `json:"errorMessage,omitempty"`
	StartedAt     int64  `json:"startedAt"`
	FinishedAt    int64  `json:"finishedAt"`
	DurationMs    int64  `json:"durationMs"`
	AttemptNumber int    `json:"attemptNumber"`
}

type requestLogBuilder struct {
	event *RequestLogEvent
	start time.Time
}

func firstRequestLogBuilder(builders []*requestLogBuilder) *requestLogBuilder {
	if len(builders) == 0 {
		return nil
	}
	return builders[0]
}

func newRequestLogBuilder(r *http.Request, protocol, model string, stream, thinking bool, body []byte, summary string) *requestLogBuilder {
	now := time.Now()
	event := &RequestLogEvent{
		ID:               "reqlog_" + uuid.New().String(),
		StartedAt:        now.Unix(),
		Path:             r.URL.Path,
		Protocol:         protocol,
		Stream:           stream,
		ClientIP:         clientIP(r),
		Model:            model,
		Thinking:         thinking,
		RequestSize:      len(body),
		RequestSummary:   truncateRunes(summary, requestLogPromptMaxChars),
		APIKeyID:         apiKeyIDFromContext(r.Context()),
		UpstreamEndpoint: config.GetPreferredEndpoint(),
	}
	if event.APIKeyID != "" {
		if entry := config.GetApiKeyEntry(event.APIKeyID); entry != nil {
			event.APIKeyName = entry.Name
			event.APIKeyMasked = config.MaskApiKey(entry.Key)
		}
	}
	return &requestLogBuilder{event: event, start: now}
}

func (b *requestLogBuilder) attempt(account *config.Account, status string, err error, attemptNumber int, started time.Time) {
	if b == nil || account == nil {
		return
	}
	finished := time.Now()
	msg := ""
	if err != nil {
		msg = truncateRunes(err.Error(), requestLogResponseMaxChars)
	}
	b.event.Attempts = append(b.event.Attempts, RequestLogAttempt{
		AccountID:     account.ID,
		AccountEmail:  account.Email,
		Status:        status,
		ErrorMessage:  msg,
		StartedAt:     started.Unix(),
		FinishedAt:    finished.Unix(),
		DurationMs:    finished.Sub(started).Milliseconds(),
		AttemptNumber: attemptNumber,
	})
	if status != "success" {
		b.event.FailedAccounts++
	}
}

func (b *requestLogBuilder) finishSuccess(logger *AsyncRequestLogger, account *config.Account, httpStatus, inputTokens, outputTokens int, credits float64, responseSummary string, toolUseCount int, stopReason string, cacheUsage promptCacheUsage) {
	if b == nil {
		return
	}
	b.finishBase(account, httpStatus, "success", "", "", inputTokens, outputTokens, credits, responseSummary, toolUseCount, stopReason, cacheUsage)
	logger.Enqueue(b.event)
}

func (b *requestLogBuilder) finishError(logger *AsyncRequestLogger, httpStatus int, errType, message string) {
	if b == nil {
		return
	}
	b.finishBase(nil, httpStatus, "error", errType, truncateRunes(message, requestLogResponseMaxChars), 0, 0, 0, "", 0, "", promptCacheUsage{})
	logger.Enqueue(b.event)
}

func (b *requestLogBuilder) finishBase(account *config.Account, httpStatus int, status, errType, errMsg string, inputTokens, outputTokens int, credits float64, responseSummary string, toolUseCount int, stopReason string, cacheUsage promptCacheUsage) {
	now := time.Now()
	b.event.FinishedAt = now.Unix()
	b.event.DurationMs = now.Sub(b.start).Milliseconds()
	b.event.HTTPStatus = httpStatus
	b.event.Status = status
	b.event.ErrorType = errType
	b.event.ErrorMessage = errMsg
	b.event.InputTokens = inputTokens
	b.event.OutputTokens = outputTokens
	b.event.TotalTokens = inputTokens + outputTokens
	b.event.Credits = credits
	b.event.ResponseSummary = truncateRunes(responseSummary, requestLogResponseMaxChars)
	b.event.ToolUseCount = toolUseCount
	b.event.StopReason = stopReason
	b.event.CacheReadTokens = cacheUsage.CacheReadInputTokens
	b.event.CacheWriteTokens = cacheUsage.CacheCreationInputTokens
	b.event.CacheCreation5mTokens = cacheUsage.CacheCreation5mInputTokens
	b.event.CacheCreation1hTokens = cacheUsage.CacheCreation1hInputTokens
	b.event.RetryCount = max(len(b.event.Attempts)-1, 0)
	if account != nil {
		b.event.AccountID = account.ID
		b.event.AccountEmail = account.Email
		b.event.AccountNickname = account.Nickname
		b.event.AccountAuthMethod = account.AuthMethod
		b.event.AccountProxy = maskProxyURL(account.ProxyURL)
	}
	meta, _ := json.Marshal(map[string]interface{}{
		"attempts":          len(b.event.Attempts),
		"upstreamEndpoint":  b.event.UpstreamEndpoint,
		"requestDBVersion":  1,
		"responseTruncated": utf8.RuneCountInString(responseSummary) > requestLogResponseMaxChars,
	})
	b.event.MetadataJSON = string(meta)
}

func summarizeClaudeRequest(req *ClaudeRequest) string {
	parts := []string{}
	if req.System != nil {
		parts = append(parts, "system: "+truncateRunes(fmt.Sprint(req.System), 200))
	}
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			text, images, toolResults := extractClaudeUserContent(req.Messages[i].Content)
			if text != "" {
				parts = append(parts, "user: "+truncateRunes(text, requestLogPromptMaxChars))
			}
			if len(images) > 0 {
				parts = append(parts, fmt.Sprintf("images: %d", len(images)))
			}
			if len(toolResults) > 0 {
				parts = append(parts, fmt.Sprintf("tool_results: %d", len(toolResults)))
			}
			break
		}
	}
	if len(req.Tools) > 0 {
		names := make([]string, 0, len(req.Tools))
		for _, tool := range req.Tools {
			names = append(names, tool.Name)
		}
		parts = append(parts, "tools: "+strings.Join(names, ","))
	}
	return strings.Join(parts, " | ")
}

func summarizeOpenAIRequest(req *OpenAIRequest) string {
	parts := []string{}
	for _, msg := range req.Messages {
		if msg.Role == "system" {
			parts = append(parts, "system: "+truncateRunes(fmt.Sprint(msg.Content), 200))
			break
		}
	}
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			text, images := extractOpenAIUserContent(req.Messages[i].Content)
			if text != "" {
				parts = append(parts, "user: "+truncateRunes(text, requestLogPromptMaxChars))
			}
			if len(images) > 0 {
				parts = append(parts, fmt.Sprintf("images: %d", len(images)))
			}
			break
		}
	}
	if len(req.Tools) > 0 {
		names := make([]string, 0, len(req.Tools))
		for _, tool := range req.Tools {
			names = append(names, tool.Function.Name)
		}
		parts = append(parts, "tools: "+strings.Join(names, ","))
	}
	return strings.Join(parts, " | ")
}

func summarizeResponsesRequest(req *ResponsesRequest, messages []OpenAIMessage) string {
	return summarizeOpenAIRequest(&OpenAIRequest{Model: req.Model, Messages: messages, Tools: req.Tools})
}

func truncateRunes(s string, maxChars int) string {
	if maxChars <= 0 || s == "" {
		return ""
	}
	if utf8.RuneCountInString(s) <= maxChars {
		return s
	}
	r := []rune(s)
	return string(r[:maxChars]) + "..."
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		return strings.TrimSpace(xr)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func maskProxyURL(v string) string {
	if v == "" {
		return ""
	}
	if at := strings.LastIndex(v, "@"); at >= 0 {
		schemeEnd := strings.Index(v, "://")
		if schemeEnd >= 0 {
			return v[:schemeEnd+3] + "****@" + v[at+1:]
		}
	}
	return v
}

func resolveRequestLogDBPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "request_logs.db"
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(config.GetConfigDir(), path)
}
