package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	echo "echo/pkg/api"
	prom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

var (
	// Create a metrics registry.
	reg = prometheus.NewRegistry()
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Инициализация метрик

	// Настройка прослушивания порта
	lis, err := net.Listen("tcp", ":8081")
	if err != nil {
		slog.Error("can't listen port", "error", err)
		os.Exit(1)
	}

	metric := prom.NewServerMetrics(prom.WithServerHandlingTimeHistogram(prom.WithHistogramBuckets(
		[]float64{0.00001, 0.00005, 0.0001, 0.0002, 0.0003, 0.0005, 0.0008, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 2.5, 3, 4, 5, 7, 8, 10, 15})),
	)
	reg.MustRegister(metric, collectors.NewGoCollector())

	// Запуск HTTP-сервера для метрик
	go func() {
		slog.Info("Metrics serving")
		httpServer := &http.Server{Handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), Addr: fmt.Sprintf("0.0.0.0:%d", 8080)}
		if err := httpServer.ListenAndServe(); err != nil {
			slog.Error("Unable to start a http server.")
		}
	}()

	// Создание gRPC-сервера с интеграцией с Prometheus
	server := grpc.NewServer(
		grpc.UnaryInterceptor(metric.UnaryServerInterceptor()), // Интерсептор для сбора метрик
	)

	metric.InitializeMetrics(server)

	echo.RegisterEchoServiceServer(server, NewEchoService())

	slog.Info("starting server at :8081")

	// Запуск gRPC-сервера
	err = server.Serve(lis)
	if err != nil {
		slog.Error("can't serve", "error", err)
		os.Exit(1)
	}
}

type EchoService struct {
	echo.UnimplementedEchoServiceServer
}

func NewEchoService() *EchoService {
	return &EchoService{}
}

func (e *EchoService) Echo(ctx context.Context, req *echo.EchoRequest) (*echo.EchoResponse, error) {
	t := time.Now()
	msg := req.GetMessage()
	slog.Info(msg)

	defer fmt.Println(time.Since(t))
	return &echo.EchoResponse{
		Message: msg,
	}, nil
}
