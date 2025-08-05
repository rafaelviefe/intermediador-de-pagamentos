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

type Job struct {
    Request *http.Request
    Body    *bytes.Buffer
}

var jobQueue chan Job

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
       Timeout:   3 * time.Second,
       KeepAlive: 30 * time.Second,
    }).DialContext,
    TLSHandshakeTimeout: 3 * time.Second,
}

var httpClient = &http.Client{
    Transport: transport,
    Timeout:   10 * time.Second,
}

func getNextBackendIndex() int {
    nextIndex := atomic.AddUint64(&counter, 1)
    return int(nextIndex % uint64(len(backendServers)))
}

// ✅ VERSÃO "FIRE-AND-FORGET" OTIMIZADA ✅
func worker() {
    for job := range jobQueue {
        // A chamada de rede agora é feita em uma nova goroutine para não bloquear o worker.
        go func(job Job) {
            // O buffer agora deve ser devolvido ao pool DENTRO desta goroutine.
            defer bufferPool.Put(job.Body)

            targetHost := backendServers[getNextBackendIndex()]
            proxyReq, err := http.NewRequest(http.MethodPost, targetHost+job.Request.URL.Path, job.Body)
            if err != nil {
                return
            }
            proxyReq.Header = job.Request.Header

            resp, err := httpClient.Do(proxyReq)
            if err != nil {
                return
            }

            // IMPORTANTE: Ainda precisamos fechar o corpo da resposta para que o http.Transport
            // possa reutilizar a conexão (Keep-Alive). Se não fizermos isso, vazaremos conexões.
            io.Copy(io.Discard, resp.Body)
            resp.Body.Close()
        }(job)
    }
}

func handlePayments(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
    buf := bufferPool.Get().(*bytes.Buffer)
    buf.Reset()

    io.Copy(buf, r.Body)
    r.Body.Close()

    jobQueue <- Job{
       Request: r,
       Body:    buf,
    }

    w.WriteHeader(http.StatusNoContent)
}

func handleSummary(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
    backendProxies[getNextBackendIndex()].ServeHTTP(w, r)
}

func main() {
    numWorkers := 100
    queueSize := 1000
    jobQueue = make(chan Job, queueSize)

    for i := 1; i <= numWorkers; i++ {
       go worker()
    }

    for i, targetStr := range backendServers {
       targetURL, _ := url.Parse(targetStr)
       proxy := httputil.NewSingleHostReverseProxy(targetURL)
       proxy.Transport = transport
       backendProxies[i] = proxy
    }

    router := httprouter.New()
    router.POST("/payments", handlePayments)
    router.GET("/payments-summary", handleSummary)

    server := &http.Server{
       Addr:         ":9999",
       Handler:      router,
       ReadTimeout:  5 * time.Second,
       WriteTimeout: 5 * time.Second,
       IdleTimeout:  120 * time.Second,
    }

    server.ListenAndServe()
}
