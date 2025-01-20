package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gorilla/websocket"

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
	result.mux.HandleFunc("/select", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/storage/select", http.StatusTemporaryRedirect)
	})
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
	vclock  VClock
}

type Storage struct {
	name        string
	ctx         context.Context
	cancel      context.CancelFunc
	queue       chan MSG
	replicas    map[string]*websocket.Conn
	leader      bool
	features    map[string]*geojson.Feature
	index       rtree.RTree
	mutex       sync.Mutex
	vclock      VClock
	nodeName    string
	requests    int
	maxRequests int
}

type VClock map[string]uint64

func (vc *VClock) Increment(node string) {
	(*vc)[node]++
}

func (vc *VClock) Merge(other VClock) {
	for node, clock := range other {
		if current, exists := (*vc)[node]; !exists || clock > current {
			(*vc)[node] = clock
		}
	}
}

func (vc *VClock) LessThan(other VClock) bool {
	for node, clock := range *vc {
		if otherClock, exists := other[node]; !exists || clock < otherClock {
			return true
		}
	}
	return false
}

func (s *Storage) Engine() {
	features, err := s.LoadState()
	if err != nil {
		slog.Error("Failed to load state:", err)
		return
	}

	for _, feature := range features {
		s.index.Insert(feature.Geometry.Bound().Min, feature.Geometry.Bound().Max, feature)
	}

	for _, conn := range s.replicas {
		go s.connectReplica(conn)
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.queue:
			s.mutex.Lock()
			defer s.mutex.Unlock()

			if msg.action == "select" {
				collection := geojson.NewFeatureCollection()
				for _, feature := range s.features {
					collection.Append(feature)
				}
				msg.cb <- collection
			} else {
				s.applyTransaction(msg)
			}
		}
	}
}

func (s *Storage) connectReplica(replica *websocket.Conn) {
	for {
		_, msg, err := replica.ReadMessage()
		if err != nil {
			slog.Error("Failed to read from replica:", err)
			break
		}

		if len(msg) == 0 {
			slog.Error("Received empty message from replica")
			continue
		}

		var txn Transaction
		if err := json.Unmarshal(msg, &txn); err != nil {
			slog.Error("Failed to unmarshal transaction:", err)
			continue
		}

		if !s.vclock.LessThan(txn.vclock) {
			s.applyTransaction(MSG{
				action: txn.action,
				name:   txn.name,
				body:   msg,
				cb:     nil,
			})
		}
	}
}

func (s *Storage) applyTransaction(msg MSG) {
	var txn Transaction
	if err := json.Unmarshal(msg.body, &txn); err != nil {
		slog.Error("Failed to unmarshal transaction:", err)
		return
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.vclock.LessThan(txn.vclock) {
		slog.Info("Skipping transaction due to vclock", slog.Any("txn", txn))
		return
	}

	s.vclock.Merge(txn.vclock)
	s.vclock.Increment(s.nodeName)

	switch txn.action {
	case "insert":
		id, ok := txn.feature.ID.(string)
		if !ok {
			slog.Error("Invalid ID type in insert action")
			return
		}
		s.features[id] = &txn.feature
	case "replace":
		id, ok := txn.feature.ID.(string)
		if !ok {
			slog.Error("Invalid ID type in replace action")
			return
		}
		s.features[id] = &txn.feature
	case "delete":
		id, ok := txn.feature.ID.(string)
		if !ok {
			slog.Error("Invalid ID type in delete action")
			return
		}
		delete(s.features, id)
	default:
		slog.Error("Uncorrect action!", slog.String("action", txn.action))
	}

	if s.leader {
		for _, conn := range s.replicas {
			log.Println("Sending message to replica:", conn.RemoteAddr())
			err := conn.WriteMessage(websocket.TextMessage, msg.body)
			if err != nil {
				log.Println("Error sending message to replica:", err)
			}
		}
	}
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

func NewStorage(mux *http.ServeMux, name string, leader bool, nodeName string, maxRequests int) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan MSG)
	replic := make(map[string]*websocket.Conn)

	result := &Storage{
		name:        name,
		ctx:         ctx,
		cancel:      cancel,
		queue:       queue,
		replicas:    replic,
		leader:      leader,
		features:    make(map[string]*geojson.Feature),
		index:       rtree.RTree{},
		vclock:      VClock{},
		nodeName:    nodeName,
		requests:    0,
		maxRequests: maxRequests,
	}

	if leader {
		registerHandlers(mux, name, queue, result)
	} else {
		go result.Engine()
	}

	return result
}

func registerHandlers(mux *http.ServeMux, name string, queue chan MSG, s *Storage) {
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
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	mux.HandleFunc("/"+name+"/replication", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error("Failed to accept WebSocket connection:", err)
			return
		}
		if conn == nil {
			slog.Error("Failed to establish WebSocket connection")
			return
		}
		s.replicas[r.RemoteAddr] = conn
		slog.Info("WebSocket connection established with replica", slog.String("remoteAddr", r.RemoteAddr))

		go s.connectReplica(conn)

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
	tt := Transaction{name: s.name, action: action, feature: *feature}
	jsonData, err := json.Marshal(tt)
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

	s.mutex.Lock()
	defer s.mutex.Unlock()

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
	if s.leader {
		features, err := s.LoadState()
		for _, f := range features {
			s.index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
		}
		if err != nil {
			panic(err)
		}
	} else {
		_, err := os.Create(s.name + ".wal")
		if err != nil {
			panic(err)
		}
	}
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case q := <-s.queue:
				s.mutex.Lock()
				defer s.mutex.Unlock()
				if q.action == "select" {
					collection := geojson.NewFeatureCollection()
					for _, f := range s.features {
						collection.Append(f)
					}
					q.cb <- collection
				} else if q.action == "insert" || q.action == "replace" || q.action == "delete" {
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
						pred, status := s.features[id]
						if !status {
							q.cb <- errors.New(id + "is not exists")
							continue
						}
						s.index.Delete(pred.Geometry.Bound().Min, pred.Geometry.Bound().Max, pred)
					}
					if q.action != "delete" {
						s.index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
						s.features[id] = f
					} else {
						s.index.Delete(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
						delete(s.features, id)
					}
					s.vclock.Increment(s.nodeName)
					q.cb <- nil
					if s.leader {
						tt := Transaction{name: q.name, action: q.action, feature: *f}
						json, err := json.Marshal(tt)
						if err == nil {
							for _, conn := range s.replicas {
								_ = conn.WriteMessage(websocket.TextMessage, json)
							}
						}
					}
				} else if q.action == "checkpoint" {
					q.cb <- s.saveCheckpoint(s.features)
				} else {
					slog.Error("Uncorrect action!" + q.action)
				}
			}
		}
	}()
}

func (s *Storage) Stop() {
	s.cancel()
	for _, conn := range s.replicas {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
			websocket.CloseNormalClosure, "ok",
		))

		conn.Close()
	}

}

func main() {
	mux := http.NewServeMux()

	// router := NewRouter(mux, [][]string{{"NODE_O"}})
	// storage := NewStorage(mux, "storage", true, "", 0) ////
	nodes := [][]string{
		{"node2:8081", "node3:8082"},
		{"node1:8080", "node3:8082"},
		{"node1:8080", "node2:8081"},
	}
	storage1 := NewStorage(mux, "node1", true, "node1", 3)
	storage2 := NewStorage(mux, "node2", false, "node2", 3)
	storage3 := NewStorage(mux, "node3", false, "node3", 3)
	router := NewRouter(mux, nodes)

	go router.Run()
	go storage1.Run()
	go storage2.Run()
	go storage3.Run()

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
	storage1.Stop()
	storage2.Stop()
	storage3.Stop()
	router.Stop()
}
