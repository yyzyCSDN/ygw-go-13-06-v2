package control_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

func TestSubmitRunStatusAndArtifactHTTPFlow(t *testing.T) {
	clock := lease.SystemClock{}
	service := operation.NewService(operation.Config{WorkerID: "test-worker", LeaseTTL: time.Minute, Clock: clock}, catalog.New(), journal.NewMemoryStore(), lease.NewManager(clock), policy.NewAdmission(policy.DefaultLimits()))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(control.NewHandler(service, logger).Router())
	defer server.Close()

	first, second := []byte("hello "), []byte("control plane")
	request := operation.Request{IdempotencyKey: "http-flow-1", Segments: map[string][]byte{"first": first, "second": second}, Manifest: segmentmerge.Manifest{
		FileID: "artifact-http", Version: "v1", TotalSize: int64(len(first) + len(second)),
		Segments: []segmentmerge.Segment{
			{ID: "first", Offset: 0, StoredSize: int64(len(first)), LogicalSize: int64(len(first)), Digest: segmentmerge.Digest(first), Encoding: segmentmerge.EncodingRaw},
			{ID: "second", Offset: int64(len(first)), StoredSize: int64(len(second)), LogicalSize: int64(len(second)), Digest: segmentmerge.Digest(second), Encoding: segmentmerge.EncodingRaw},
		},
	}}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(server.URL+"/v1/merges", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("submit status=%d body=%s", response.StatusCode, payload)
	}
	var submitted struct {
		Operation operation.Operation `json:"operation"`
	}
	if err := json.NewDecoder(response.Body).Decode(&submitted); err != nil {
		t.Fatal(err)
	}
	if submitted.Operation.ID == "" || submitted.Operation.State != operation.StateAccepted {
		t.Fatalf("unexpected submitted operation: %+v", submitted.Operation)
	}

	runResponse, err := http.Post(server.URL+"/v1/merges/"+submitted.Operation.ID+"/run", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runResponse.Body.Close()
	if runResponse.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(runResponse.Body)
		t.Fatalf("run status=%d body=%s", runResponse.StatusCode, payload)
	}

	statusResponse, err := http.Get(server.URL + "/v1/merges/" + submitted.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer statusResponse.Body.Close()
	var status struct {
		Operation operation.Operation `json:"operation"`
	}
	if err := json.NewDecoder(statusResponse.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Operation.State != operation.StateCompleted || status.Operation.Progress.CompletedSegments != 2 {
		t.Fatalf("unexpected completed state: %+v", status.Operation)
	}

	artifactResponse, err := http.Get(server.URL + "/v1/artifacts/artifact-http")
	if err != nil {
		t.Fatal(err)
	}
	defer artifactResponse.Body.Close()
	if artifactResponse.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(artifactResponse.Body)
		t.Fatalf("artifact status=%d body=%s", artifactResponse.StatusCode, payload)
	}
}
