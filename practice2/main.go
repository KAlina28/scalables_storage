package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Router struct {
	mux   *http.ServeMux
	nodes [][]string // Список Storage узлов
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	result := &Router{
		mux:   mux,
		nodes: nodes,
	}

	result.mux.Handle("/", http.FileServer(http.Dir("../front/dist")))
	result.mux.Handle("/select", http.RedirectHandler("/storage/select", http.StatusTemporaryRedirect))
	result.mux.Handle("/insert", http.RedirectHandler("/storage/insert", http.StatusTemporaryRedirect))
	result.mux.Handle("/replace", http.RedirectHandler("/storage/replace", http.StatusTemporaryRedirect))
	result.mux.Handle("/delete", http.RedirectHandler("/storage/delete", http.StatusTemporaryRedirect))
	result.mux.Handle("/checkpoint", http.RedirectHandler("/storage/checkpoint", http.StatusTemporaryRedirect))

	return result
}

func (r *Router) Run() {
	slog.Info("router is running")
}

func (r *Router) Stop() {
	slog.Info("router is stopped")
}

type MSG struct {
	action string
	name   string
	body   []byte
	cb     chan any
}

type Transaction struct {
	name    string
	action  string
	feature geojson.Feature
}

type Storage struct {
	name   string
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan MSG
}

func handle(action string, name string, queue chan MSG, r *http.Request) (interface{}, error) {
	slog.Info("Handling %s for %s", action, name)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	cb := make(chan interface{})
	queue <- MSG{
		name:   name,
		action: action,
		body:   body,
		cb:     cb,
	}

	res := <-cb
	close(cb)

	switch v := res.(type) {
	case error:
		return nil, v
	default:
		return v, nil
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request, action, name string, queue chan MSG) {
	res, err := handle(action, name, queue, r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	if action == "select" {
		collection, ok := res.(*geojson.FeatureCollection)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		jsonData, err := collection.MarshalJSON()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan MSG)
	result := &Storage{
		name:   name,
		ctx:    ctx,
		cancel: cancel,
		queue:  queue,
	}

	registerHandlers(mux, name, queue)

	return result
}

func registerHandlers(mux *http.ServeMux, name string, queue chan MSG) {
	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, "select", name, queue)
	})
	mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, "insert", name, queue)
	})
	mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, "replace", name, queue)
	})
	mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, "delete", name, queue)
	})
	mux.HandleFunc("/"+name+"/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, "checkpoint", name, queue)
	})
}

func (s *Storage) saveCheckpoint(features map[string]*geojson.Feature) error {
	collection := geojson.NewFeatureCollection()
	for _, f := range features {
		collection.Append(f)
	}

	json, err := collection.MarshalJSON()
	if err != nil {
		return err
	}

	if err := os.WriteFile(s.name+".ckp", json, 0600); err != nil {
		return err
	}

	return os.WriteFile(s.name+".wal", []byte{}, 0600)
}

func createTransaction(s *Storage, action string, feature *geojson.Feature) error {
	transaction := Transaction{name: s.name, action: action, feature: *feature}
	jsonData, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	walFile, err := os.OpenFile(s.name+".wal", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer walFile.Close()
	_, err = walFile.Write(jsonData)
	return err
}

func (s *Storage) LoadState() (map[string]*geojson.Feature, error) {
	state := make(map[string]*geojson.Feature)
	defer s.saveCheckpoint(state)

	walData, err := os.ReadFile(s.name + ".wal")
	if err != nil {
		return nil, err
	}
	if ckpData, err := os.ReadFile(s.name + ".ckp"); err == nil {
		collection, err := geojson.UnmarshalFeatureCollection(ckpData)
		if err != nil {
			return nil, err
		}
		for _, feature := range collection.Features {
			state[feature.ID.(string)] = feature
		}
	}

	d := json.NewDecoder(io.NopCloser(bytes.NewBuffer(walData)))
	for d.More() {
		var tt Transaction
		if err := d.Decode(&tt); err != nil {
			return nil, err
		}
		switch tt.action {
		case "insert":
			state[tt.feature.ID.(string)] = &tt.feature
		case "replace":
			state[tt.feature.ID.(string)] = &tt.feature
		case "delete":
			delete(state, tt.feature.ID.(string))
		}
	}

	return state, nil
}

func (s *Storage) Run() {
	features, err := s.LoadState()
	index := rtree.RTree{}
	for _, f := range features {
		index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
	}
	if err != nil {
		panic(err)
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case q := <-s.queue:
			if q.action == "select" {
				collection := geojson.NewFeatureCollection()
				for _, f := range features {
					collection.Append(f)
				}
				q.cb <- collection
			} else if q.action == "select" || q.action == "replace" || q.action == "delete" {
				f, err := geojson.UnmarshalFeature(q.body)
				if err != nil {
					q.cb <- err
					continue
				}
				id, ok := f.ID.(string)
				if !ok {
					slog.Error("Failed to get ID")
					q.cb <- errors.New("ID is not string")
					continue
				}
				if createTransaction(s, q.action, f) != nil {
					q.cb <- err
					continue
				}
				if q.action == "replace" {
					pred, status := features[id]
					if !status {
						q.cb <- errors.New(id + "is not exists")
						continue
					}
					index.Delete(pred.Geometry.Bound().Min, pred.Geometry.Bound().Max, pred)
				}
				if q.action != "delete" {
					index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
					features[id] = f
				} else {
					index.Delete(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
					delete(features, id)
				}
				q.cb <- nil
			} else if q.action == "checkpoint" {
				q.cb <- s.saveCheckpoint(features)
			} else {
				slog.Error("Uncorrect action!")
			}
		}
	}
}

func (s *Storage) Stop() {
	s.cancel()
}

func main() {
	mux := http.NewServeMux()

	router := NewRouter(mux, [][]string{{"NODE_O"}})
	storage := NewStorage(mux, "storage", nil, true)

	go router.Run()
	defer router.Stop()
	go storage.Run()
	defer storage.Stop()

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	go func() {
		slog.Info("Listen http://" + server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Info("HTTP server error: %v", err)
		}

	}()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	slog.Info("Listen http://127.0.0.1:8080")
	slog.Info("got", "signal", sig)

	server.Shutdown(context.Background())
	storage.Stop()
	router.Stop()
}
