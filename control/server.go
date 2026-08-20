package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

type Server struct {
	httpServer *http.Server
	listener   net.Listener
}

func NewServer(address string, handler http.Handler) *Server {
	if address == "" {
		address = "127.0.0.1:8080"
	}
	return &Server{httpServer: &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
	}}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	err = s.httpServer.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Address() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.httpServer.Addr
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.listener == nil {
		return fmt.Errorf("server has not started")
	}
	s.httpServer.SetKeepAlivesEnabled(false)
	_ = s.listener.Close()
	_ = s.httpServer.Close()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(100 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil
			}
		}
	}
}
