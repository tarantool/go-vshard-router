//go:build integration
// +build integration

package etcd

import (
	"fmt"
	"testing"
	"time"

	mocktopology "github.com/tarantool/go-vshard-router/v2/mocks/topology"
	"go.etcd.io/etcd/client/v2"
)

func TestNewProvider(t *testing.T) {
	provider := NewProvider(Config{
		EtcdConfig: client.Config{
			Endpoints: []string{"http://127.0.0.1:2379"},
			Transport: client.DefaultTransport,
			// set timeout per request to fail fast when the target endpoint is unavailable
			HeaderTimeoutPerRequest: time.Second,
		},
		Path: "/project/store/storage",
	})

	err := provider.Init(mocktopology.NewTopologyController(t))
	fmt.Println(err)
}
