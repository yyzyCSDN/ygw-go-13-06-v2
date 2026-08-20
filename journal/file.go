package journal

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileStore struct {
	mu     sync.Mutex
	path   string
	file   *os.File
	next   uint64
	closed bool
}

func OpenFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("journal path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	replay, err := decodeStream(context.Background(), mustSeekStart(file), "")
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, 2); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &FileStore{path: path, file: file, next: replay.LastSequence}, nil
}

func mustSeekStart(file *os.File) *os.File {
	_, _ = file.Seek(0, 0)
	return file
}

func (s *FileStore) Append(ctx context.Context, event Event) (Envelope, error) {
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Envelope{}, fmt.Errorf("journal is closed")
	}
	next := s.next + 1
	envelope, encoded, err := encodeEnvelope(next, event)
	if err != nil {
		return Envelope{}, err
	}
	writer := bufio.NewWriter(s.file)
	if _, err := writer.Write(encoded); err != nil {
		return Envelope{}, err
	}
	if err := writer.WriteByte('\n'); err != nil {
		return Envelope{}, err
	}
	if err := writer.Flush(); err != nil {
		return Envelope{}, err
	}
	if err := s.file.Sync(); err != nil {
		return Envelope{}, err
	}
	s.next = next
	return envelope, nil
}

func (s *FileStore) Replay(ctx context.Context, operationID string) (Replay, error) {
	if operationID == "" {
		return Replay{}, fmt.Errorf("operation id is required")
	}
	return s.read(ctx, operationID)
}

func (s *FileStore) All(ctx context.Context) (Replay, error) {
	return s.read(ctx, "")
}

func (s *FileStore) read(ctx context.Context, operationID string) (Replay, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Replay{}, fmt.Errorf("journal is closed")
	}
	reader, err := os.Open(s.path)
	if err != nil {
		return Replay{}, err
	}
	defer reader.Close()
	return decodeStream(ctx, reader, operationID)
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}
