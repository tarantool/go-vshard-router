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
	"github.com/spf13/viper"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
	viper2 "github.com/tarantool/go-vshard-router/v2/providers/viper"
	"google.golang.org/grpc"
)

var (
	// Create a metrics registry.
	reg    = prometheus.NewRegistry()
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
)

type Config struct {
	Storages map[string]string `json:"storages"`
}

func main() {
	ctx := context.Background()
	slog.SetDefault(logger)

	// инциализация конфига

	configFile := "config.yaml"

	args := os.Args[1:]
	if len(args) == 1 {
		configFile = args[0]
	}

	slog.Info("reading config file", "from", configFile)

	cfg := &Config{}

	viper.SetConfigFile(configFile)
	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		slog.Error("viper cant read in such config file ")

		os.Exit(2)
	}

	err = viper.Unmarshal(cfg)
	if err != nil {
		slog.Error("failed to unmarshal config file", "error", err)

		os.Exit(2)
	}

	slog.Info("init with storages", "storages", cfg.Storages)

	provider := viper2.NewProvider(ctx, viper.Sub("storage"), viper2.ConfigTypeMoonlibs)

	r, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		Loggerf:          vshardrouter.NewSlogLogger(logger),
		TotalBucketCount: 100,
		TopologyProvider: provider,
	})
	if err != nil {
		slog.Error("failed to init router", "error", err)

		os.Exit(2)
	}

	// Инициализация метрик

	// Настройка прослушивания порта
	lis, err := net.Listen("tcp", ":8082")
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

	echoSrv, err := NewEchoService(ctx, r)
	if err != nil {
		slog.Error("can't create echo service", "error", err)

		os.Exit(1)
	}

	echo.RegisterEchoServiceServer(server, echoSrv)

	slog.Info("starting server at :8081")

	// Запуск gRPC-сервера
	err = server.Serve(lis)
	if err != nil {
		slog.Error("can't serve", "error", err)
		os.Exit(1)
	}
}

type EchoService struct {
	router *vshardrouter.Router

	echo.UnimplementedEchoServiceServer
}

func NewEchoService(ctx context.Context, r *vshardrouter.Router) (*EchoService, error) {
	return &EchoService{
		router: r,
	}, nil
}

func (e *EchoService) Echo(ctx context.Context, req *echo.EchoRequest) (*echo.EchoResponse, error) {
	t := time.Now()
	msg := req.GetMessage()
	slog.Info(msg)

	bucketID := vshardrouter.BucketIDStrCRC32(req.Message, 100)

	resp, err := e.router.Call(
		ctx,
		bucketID,
		vshardrouter.CallModeBRO,
		"echo",
		[]interface{}{msg},
		vshardrouter.CallOpts{
			Timeout: time.Second,
		},
	)
	if err != nil {
		return nil, err
	}

	str := new(string)

	err = resp.GetTyped(&[]interface{}{str})
	if err != nil {
		return nil, err
	}

	defer fmt.Println(time.Since(t))
	return &echo.EchoResponse{
		Message: *str,
	}, nil
}
