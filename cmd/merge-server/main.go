package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/segmentmerger/catalog"
	"example.com/segmentmerger/control"
	"example.com/segmentmerger/journal"
	"example.com/segmentmerger/lease"
	"example.com/segmentmerger/operation"
	"example.com/segmentmerger/policy"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	journalPath := flag.String("journal", "", "optional durable journal path")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var events journal.Store = journal.NewMemoryStore()
	var closeJournal func() error
	if *journalPath != "" {
		store, err := journal.OpenFileStore(*journalPath)
		if err != nil {
			log.Fatal(err)
		}
		events = store
		closeJournal = store.Close
	}
	if closeJournal != nil {
		defer closeJournal()
	}
	clock := lease.SystemClock{}
	service := operation.NewService(operation.Config{WorkerID: "merge-server", PartitionCount: 64, LeaseTTL: 30 * time.Second, Clock: clock}, catalog.New(), events, lease.NewManager(clock), policy.NewAdmission(policy.DefaultLimits()))
	server := control.NewServer(*address, control.NewHandler(service, logger).Router())
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stopped
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	logger.Info("merge control server starting", "address", *address)
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
