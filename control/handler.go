package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/operation"
)

type Handler struct {
	service *operation.Service
	logger  *slog.Logger
	timeout time.Duration
}

func NewHandler(service *operation.Service, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{service: service, logger: logger, timeout: 30 * time.Second}
}

func (h *Handler) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /v1/merges", h.submit)
	mux.HandleFunc("GET /v1/merges", h.list)
	mux.HandleFunc("GET /v1/merges/{id}", h.status)
	mux.HandleFunc("POST /v1/merges/{id}/run", h.run)
	mux.HandleFunc("POST /v1/merges/{id}/cancel", h.cancel)
	mux.HandleFunc("GET /v1/merges/{id}/events", h.events)
	mux.HandleFunc("GET /v1/artifacts/{fileID}", h.artifact)
	return h.recoverPanic(h.requestLog(mux))
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	var request operation.Request
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err)
		return
	}
	if header := strings.TrimSpace(r.Header.Get("Idempotency-Key")); header != "" {
		if request.IdempotencyKey != "" && request.IdempotencyKey != header {
			writeError(w, http.StatusBadRequest, "idempotency_mismatch", fmt.Errorf("header and body keys differ"))
			return
		}
		request.IdempotencyKey = header
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	op, created, err := h.service.Submit(ctx, request)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "submit_rejected", err)
		return
	}
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/v1/merges/"+op.ID)
	writeJSON(w, status, map[string]any{"operation": op, "created": created})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	op, err := h.service.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, operation.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "status_failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": op})
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 1000 {
			writeError(w, http.StatusBadRequest, "invalid_limit", fmt.Errorf("limit must be between 1 and 1000"))
			return
		}
		limit = parsed
	}
	records := h.service.List(r.URL.Query().Get("state"), limit)
	writeJSON(w, http.StatusOK, map[string]any{"operations": records, "count": len(records)})
}

func (h *Handler) run(w http.ResponseWriter, r *http.Request) {
	op, err := h.service.Run(context.Background(), r.PathValue("id"))
	if errors.Is(err, operation.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, "run_failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": op})
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	op, err := h.service.Cancel(r.Context(), r.PathValue("id"))
	if errors.Is(err, operation.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err)
		return
	}
	if errors.Is(err, operation.ErrTerminal) {
		writeError(w, http.StatusConflict, "already_terminal", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cancel_failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": op})
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	events, err := h.service.Replay(r.Context(), r.PathValue("id"))
	if errors.Is(err, operation.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "replay_failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events, "count": len(events)})
}

func (h *Handler) artifact(w http.ResponseWriter, r *http.Request) {
	record, err := h.service.Artifact(r.Context(), r.PathValue("fileID"))
	if errors.Is(err, catalog.ErrArtifactNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "artifact_failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifact": record})
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 8<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return fmt.Errorf("request contains multiple JSON values")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string, err error) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"code": code, "message": err.Error()}})
}

func (h *Handler) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		h.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}

func (h *Handler) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				h.logger.Error("http panic", "path", r.URL.Path, "panic", recovered)
				writeError(w, http.StatusInternalServerError, "internal_panic", fmt.Errorf("request failed"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
