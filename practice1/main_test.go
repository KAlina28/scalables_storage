package main

import (
	"bytes"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func TestHTTPAPI(t *testing.T) {
	mux := http.NewServeMux()

	s := NewStorage(mux, "test", []string{}, true) // STORAGE
	go func() { s.Run() }()

	r := NewRouter(mux, [][]string{{"test"}}) //ROUTER
	go func() { r.Run() }()
	t.Cleanup(r.Stop)

	t.Cleanup(s.Stop)

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		Name           string
		Method         string
		URL            string
		Body           []byte
		ExpectedStatus int
	}{
		{
			Name:           "Test Select Redirect",
			Method:         "GET",
			URL:            "/test/select",
			Body:           nil,
			ExpectedStatus: http.StatusOK,
		},
		{
			Name:           "Test Insert Redirect",
			Method:         "POST",
			URL:            "/test/insert",
			Body:           body,
			ExpectedStatus: http.StatusOK,
		},
		{
			Name:           "Test Replace Redirect",
			Method:         "POST",
			URL:            "/test/replace",
			Body:           body,
			ExpectedStatus: http.StatusOK,
		},
		{
			Name:           "Test Delete Redirect",
			Method:         "POST",
			URL:            "/test/delete",
			Body:           body,
			ExpectedStatus: http.StatusOK,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			req, err := http.NewRequest(tc.Method, tc.URL, bytes.NewReader(tc.Body))
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code == http.StatusTemporaryRedirect {
				location := rr.Header().Get("Location")
				req, err := http.NewRequest(tc.Method, location, bytes.NewReader(tc.Body))
				if err != nil {
					t.Fatalf("Failed to create redirected request: %v", err)
				}

				rr = httptest.NewRecorder()
				mux.ServeHTTP(rr, req)

				if rr.Code != http.StatusOK {
					t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
				}
			} else if rr.Code != tc.ExpectedStatus {
				t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, tc.ExpectedStatus)
			}
		})
	}
}
