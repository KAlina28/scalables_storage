package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func TestStorageOperations(t *testing.T) {
	mux := http.NewServeMux()

	storage1 := NewStorage(mux, "node1", true, "node1", 3)
	storage2 := NewStorage(mux, "node2", false, "node2", 3)
	storage3 := NewStorage(mux, "node3", false, "node3", 3)

	nodes := [][]string{
		{"node2:8081", "node3:8082"},
		{"node1:8080", "node3:8082"},
		{"node1:8080", "node2:8081"},
	}
	router := NewRouter(mux, nodes)

	go router.Run()
	go storage1.Run()
	go storage2.Run()
	go storage3.Run()

	defer func() {
		storage1.Stop()
		storage2.Stop()
		storage3.Stop()
		router.Stop()
	}()

	server := httptest.NewServer(mux)
	defer server.Close()

	geometry := orb.Point([]float64{100.0, 100.0})
	feature := &geojson.Feature{
		ID:       "1",
		Geometry: geometry,
		Properties: map[string]interface{}{
			"name": "Test Point",
		},
	}

	t.Run("Insert Feature", func(t *testing.T) {
		body, _ := json.Marshal(feature)
		req, err := http.NewRequest("POST", server.URL+"/node1/insert", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status OK, got %v", resp.StatusCode)
		}
	})
}
