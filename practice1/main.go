package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
	return result
}

func (r *Router) Run() {
	slog.Info("router is running")
}

func (r *Router) Stop() {
	slog.Info("router is stopped")
}

type Storage struct {
	name     string
	mux      *http.ServeMux
	replicas []string
	leader   bool
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	result := &Storage{
		mux:      mux,
		name:     name,
		replicas: replicas,
		leader:   leader,
	}

	result.mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		// Вернуть geojson объекты в формате feature collection
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"type": "FeatureCollection", "features": []}`))
	})

	result.mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		// Сохранить новый geojson объект из body запроса
		w.Write([]byte("insert"))
	})

	result.mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		// Заменить сохранённый ранее geojson объект из body запроса
		w.Write([]byte("replace"))
	})

	result.mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		// Удалить сохранённый ранее geojson объект id из body запроса
		w.Write([]byte("delete"))
	})
	return result
}

func (r *Storage) Run() {
	slog.Info("storage is running")
}

func (r *Storage) Stop() {
	slog.Info("storager is stopped")
}

func main() {
	mux := http.NewServeMux()

	router := NewRouter(mux, [][]string{{"NODE_O"}})
	storage := NewStorage(mux, "storage", nil, true)

	go router.Run()
	go storage.Run()

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Info("HTTP server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	slog.Info("got", "signal", sig)

	server.Shutdown(context.Background())
	storage.Stop()
	router.Stop()
}
