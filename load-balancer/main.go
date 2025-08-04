package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

var backendServers = []*url.URL{
	{Scheme: "http", Host: "api1:8080"},
	{Scheme: "http", Host: "api2:8080"},
}

var counter uint64

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

func getNextBackend() *url.URL {
	nextIndex := atomic.AddUint64(&counter, 1)
	return backendServers[nextIndex%uint64(len(backendServers))]
}

func handlePayments(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Erro ao ler o corpo da requisição: %v", err)
		http.Error(w, "Erro interno", http.StatusInternalServerError)
		return
	}
	r.Body.Close()

	target := getNextBackend()

	go func() {
		proxyReq, err := http.NewRequest(r.Method, target.String()+r.URL.Path, bytes.NewReader(body))
		if err != nil {
			log.Printf("Erro ao criar a requisição para o backend %s: %v", target, err)
			return
		}

		proxyReq.Header = r.Header

		resp, err := httpClient.Do(proxyReq)
		if err != nil {
			log.Printf("Erro ao encaminhar a requisição para o backend %s: %v", target, err)
			return
		}
		defer resp.Body.Close()

		log.Printf("Requisição encaminhada para %s, status recebido: %s", target, resp.Status)
	}()

	w.WriteHeader(http.StatusNoContent)
}

func handleSummary(w http.ResponseWriter, r *http.Request) {
	target := getNextBackend()

	proxy := httputil.NewSingleHostReverseProxy(target)

	r.URL.Host = target.Host
	r.URL.Scheme = target.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Host = target.Host

	proxy.ServeHTTP(w, r)
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/payments":
			handlePayments(w, r)
		case r.Method == http.MethodGet && r.URL.Path == "/payments-summary":
			handleSummary(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	log.Println("Balanceador de carga iniciado na porta :9999")
	if err := http.ListenAndServe(":9999", mux); err != nil {
		log.Fatalf("Não foi possível iniciar o servidor: %v", err)
	}
}
