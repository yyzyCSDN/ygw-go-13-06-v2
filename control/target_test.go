package control_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/control"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/operation"
	"example.com/segmentmerger/policy"
	"example.com/segmentmerger/segmentmerge"
)

type drainJournal struct {
	journal.Store
	entered chan struct{}
	release chan struct{}
}

func (store *drainJournal) Append(ctx context.Context, event journal.Event) (journal.Envelope, error) {
	if event.Kind == journal.KindStarted {
		select {
		case store.entered <- struct{}{}:
		default:
		}
		<-store.release
	}
	return store.Store.Append(ctx, event)
}

func TestShutdownDrainsRunBeforeReleasingLease(t *testing.T) {
	clock := lease.SystemClock{}
	events := &drainJournal{Store: journal.NewMemoryStore(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	records := catalog.New()
	leases := lease.NewManager(clock)
	service := operation.NewService(operation.Config{WorkerID: "drain-worker", LeaseTTL: time.Minute, Clock: clock}, records, events, leases, policy.NewAdmission(policy.DefaultLimits()))
	handler := control.NewHandler(service, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := control.NewServer("127.0.0.1:0", handler.Router())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Start() }()

	deadline := time.Now().Add(5 * time.Second)
	address := ""
	for time.Now().Before(deadline) {
		address = server.Address()
		if address != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if address == "" || address == "127.0.0.1:0" {
		t.Fatalf("server did not start, address=%q", address)
	}
	baseURL := "http://" + address

	first, second := []byte("a"), []byte("b")
	request := operation.Request{
		IdempotencyKey: "shutdown-drain",
		Segments:       map[string][]byte{"a": first, "b": second},
		Manifest: segmentmerge.Manifest{
			FileID:    "drain-artifact",
			Version:   "v1",
			TotalSize: 2,
			Segments: []segmentmerge.Segment{
				{ID: "a", Offset: 0, StoredSize: 1, LogicalSize: 1, Digest: segmentmerge.Digest(first), Encoding: segmentmerge.EncodingRaw},
				{ID: "b", Offset: 1, StoredSize: 1, LogicalSize: 1, Digest: segmentmerge.Digest(second), Encoding: segmentmerge.EncodingRaw},
			},
		},
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	submitResponse, err := http.Post(baseURL+"/v1/merges", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer submitResponse.Body.Close()
	var submitted struct {
		Operation operation.Operation `json:"operation"`
	}
	if err := json.NewDecoder(submitResponse.Body).Decode(&submitted); err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() {
		response, err := http.Post(baseURL+"/v1/merges/"+submitted.Operation.ID+"/run", "application/json", nil)
		if response != nil {
			response.Body.Close()
		}
		runErr <- err
	}()
	<-events.entered

	shutdownDone := make(chan error, 1)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { shutdownDone <- server.Shutdown(shutdownCtx) }()

	select {
	case err := <-shutdownDone:
		close(events.release)
		<-runErr
		t.Fatalf("shutdown returned while a run was still in flight: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	close(events.release)
	if err := <-runErr; err != nil {
		t.Fatalf("run request failed: %v", err)
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("shutdown did not complete after the run drained")
	}

	after, err := service.Get(context.Background(), submitted.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != operation.StateCompleted {
		t.Fatalf("operation state=%s after shutdown drain", after.State)
	}
	if _, ok := leases.Get("merge/" + request.Manifest.FileID); ok {
		t.Fatalf("lease still held after shutdown drain")
	}
}
