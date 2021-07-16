package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/fsnotify/fsnotify"
	gokitlog "github.com/go-kit/log"
	"github.com/otel-loadbalancer/collector"
	"github.com/otel-loadbalancer/config"
	lbdiscovery "github.com/otel-loadbalancer/discovery"
	"github.com/otel-loadbalancer/sharder"

	"github.com/gorilla/mux"
)

// TODO: Make the following constants flags.

const (
	configDir  = "./conf/"
	listenAddr = ":3030"
)

func main() {
	ctx := context.Background()

	// watcher to monitor file changes in ConfigMap
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Can't start the watcher: %v", err)
	}
	defer watcher.Close()

	if err := watcher.Add(configDir); err != nil {
		log.Fatalf("Can't add directory to watcher: %v", err)
	}

	srv, err := newServer(listenAddr)
	if err != nil {
		log.Fatalf("Can't start the server: %v", err)
	}

	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Can't start the server: %v", err)
		}
	}()

	for {
		select {
		case <-interrupts:
			if err := srv.Shutdown(ctx); err != http.ErrServerClosed {
				log.Println(err)
				os.Exit(1)
			}
			os.Exit(0)
		case event := <-watcher.Events:
			switch event.Op {
			case fsnotify.Write:
				log.Println("ConfigMap updated!")
				// Restart the server to pickup the new config.
				if err := srv.Shutdown(ctx); err != http.ErrServerClosed {
					log.Fatalf("Cannot shutdown the server: %v", err)
				}
				srv, err = newServer(listenAddr)
				if err != nil {
					log.Fatalf("Error restarting the server with new config: %v", err)
				}
				if err := srv.Start(); err != nil {
					log.Fatalf("Can't restart the server: %v", err)
				}
			}
		case err := <-watcher.Errors:
			log.Printf("Watcher error: %v", err)
		}
	}
}

type server struct {
	sharder          *sharder.Sharder
	discoveryManager *lbdiscovery.Manager
	server           *http.Server
}

func newServer(addr string) (*server, error) {
	sharder, discoveryManager, err := newSharder(context.Background())
	if err != nil {
		return nil, err
	}
	s := &server{
		sharder:          sharder,
		discoveryManager: discoveryManager,
	}
	router := mux.NewRouter()
	router.HandleFunc("/jobs", s.jobHandler).Methods("GET")
	router.HandleFunc("/jobs/{job_id}/targets", s.targetHandler).Methods("GET")
	s.server = &http.Server{Addr: addr, Handler: router}
	return s, nil
}

func newSharder(ctx context.Context) (*sharder.Sharder, *lbdiscovery.Manager, error) {
	cfg, err := config.Load("")
	if err != nil {
		return nil, nil, err
	}

	// returns the list of collectors based on label selector
	collectors, err := collector.Get(ctx, cfg.LabelSelector)
	if err != nil {
		return nil, nil, err
	}

	// creates a new discovery manager
	discoveryManager := lbdiscovery.NewManager(ctx, gokitlog.NewNopLogger())

	// returns the list of targets
	if err := discoveryManager.ApplyConfig(cfg); err != nil {
		return nil, nil, err
	}

	sharder := sharder.NewSharder()
	discoveryManager.Watch(func(targets []lbdiscovery.TargetData) {
		sharder.SetTargets(targets)
		sharder.Reshard()
	})
	sharder.SetCollectors(collectors)
	return sharder, discoveryManager, nil
}

func (s *server) Start() error {
	log.Println("Starting server...")
	return s.server.ListenAndServe()
}

func (s *server) Shutdown(ctx context.Context) error {
	log.Println("Shutdowning server...")
	s.discoveryManager.Close()
	return s.server.Shutdown(ctx)
}

func (s *server) jobHandler(w http.ResponseWriter, r *http.Request) {
	displayData := s.sharder.Cache.DisplayJobMapping

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(displayData)
}

func (s *server) targetHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()["collector_id"]
	params := mux.Vars(r)
	if len(q) == 0 {
		targets := s.sharder.Cache.DisplayCollectorJson[params["job_id"]]
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(targets)

	} else {
		tgs := s.sharder.Cache.DisplayTargetMapping[params["job_id"]+q[0]]
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tgs)
	}
}

// TODO: Make sure there are no race conditions.
