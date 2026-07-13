package proxy

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) apiRequestLogsHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(h.requestLogger.Health())
}
