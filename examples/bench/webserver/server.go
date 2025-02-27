package main

import (
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var fooCount = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "foo_total",
	Help: "Number of foo successfully processed.",
})

var hits = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "hits",
	Help: "Number of requests received.",
}, []string{"status", "path"})

var respTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "response_time_seconds",
	Help:    "Response time in seconds.",
	Buckets: prometheus.DefBuckets, // Стандартные бакеты: [0.005, 0.01, 0.025, ..., 10]
}, []string{"status", "path"})

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("server started")

	// Регистрируем метрики в Prometheus
	prometheus.MustRegister(fooCount, hits, respTime)

	// Обработчик метрик
	http.Handle("/metrics", promhttp.Handler())

	// Оборачиваем хендлеры в функцию измерения времени ответа
	http.Handle("/", instrumentHandler("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.WithLabelValues("200", r.URL.Path).Inc()
		fooCount.Add(1)
		fmt.Fprintf(w, "foo_total increased")
	})))

	http.Handle("/500", instrumentHandler("/500", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.WithLabelValues("500", r.URL.Path).Inc()
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("error"))
	})))

	log.Fatal(http.ListenAndServe(":8080", nil))
}

// instrumentHandler измеряет response time и статус-код
func instrumentHandler(path string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(rec, r)

		duration := time.Since(start).Seconds()
		respTime.WithLabelValues(fmt.Sprintf("%d", rec.statusCode), path).Observe(duration)
	})
}

// statusRecorder для записи статус-кода ответа
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}
