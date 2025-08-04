package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
	"github.com/julienschmidt/httprouter"
)

var backendServers = []string{
	"http://api1:8080",
	"http://api2:8080",
}

var backendProxies = make([]*httputil.ReverseProxy, len(backendServers))

var counter uint64

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var transport = &http.Transport{
	MaxIdleConns:        1000,
	MaxIdleConnsPerHost: 500,
	IdleConnTimeout:     90 * time.Second,
	DialContext: (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	TLSHandshakeTimeout: 5 * time.Second,
}

var httpClient = &http.Client{
	Transport: transport,
	Timeout:   10 * time.Second,
}

func getNextBackendIndex() int {
	nextIndex := atomic.AddUint64(&counter, 1)
	return int(nextIndex % uint64(len(backendServers)))
}

func handlePayments(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	_, err := io.Copy(buf, io.LimitReader(r.Body, 1*1024*1024))
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	r.Body.Close()

	w.WriteHeader(http.StatusNoContent)

	bodyCopy := make([]byte, buf.Len())
	copy(bodyCopy, buf.Bytes())

	go func(body []byte) {
		targetHost := backendServers[getNextBackendIndex()]
		proxyReq, err := http.NewRequest(http.MethodPost, targetHost+r.URL.Path, bytes.NewReader(body))
		if err != nil {
			return
		}

		proxyReq.Header = r.Header

		resp, err := httpClient.Do(proxyReq)
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}(bodyCopy)
}

func handleSummary(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	backendProxies[getNextBackendIndex()].ServeHTTP(w, r)
}

func main() {
	for i, targetStr := range backendServers {
		targetURL, err := url.Parse(targetStr)
		if err != nil {
			panic("Invalid backend URL: " + err.Error())
		}
		backendProxies[i] = httputil.NewSingleHostReverseProxy(targetURL)
	}

	router := httprouter.New()
	router.POST("/payments", handlePayments)
	router.GET("/payments-summary", handleSummary)

	server := &http.Server{
		Addr:    ":9999",
		Handler: router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		panic("Failed to start server: " + err.Error())
	}
}
