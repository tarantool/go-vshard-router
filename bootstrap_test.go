package vshard_router_test //nolint:revive

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	vshard_router "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
	chelper "github.com/KaymeKaydex/go-vshard-router/test_helper"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

func TestRouter_ClusterBootstrap(t *testing.T) {
	ctx := context.Background()

	router, err := vshard_router.NewRouter(ctx, vshard_router.Config{
		TotalBucketCount: 100,
		TopologyProvider: static.NewProvider(topology),
		User:             "guest",
	})
	require.NotNil(t, router)
	require.NoError(t, err)

	err = router.ClusterBootstrap(ctx, false)
	require.NoError(t, err)
	for _, rs := range router.RouterRouteAll() {
		count, err := rs.BucketsCount(ctx)
		require.NoError(t, err)
		require.Equal(t, count, uint64(50))
	}
}

const instancesCount = 4

// init servers from our cluster
var serverNames = map[string]string{
	// shard 1
	"storage_1_a": "127.0.0.1:3301",
	"storage_1_b": "127.0.0.1:3302",
	// shard 2
	"storage_2_a": "127.0.0.1:3303",
	"storage_2_b": "127.0.0.1:3304",
}

var topology = map[vshard_router.ReplicasetInfo][]vshard_router.InstanceInfo{
	{
		Name:   "storage_1",
		UUID:   uuid.New(),
		Weight: 1,
	}: {
		{
			Name: "storage_1_a",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3301",
		},
		{
			Name: "storage_1_b",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3302",
		},
	},
	{
		Name:   "storage_2",
		UUID:   uuid.New(),
		Weight: 1,
	}: {
		{
			Name: "storage_2_a",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3303",
		},
		{
			Name: "storage_2_b",
			UUID: uuid.New(),
			Addr: "127.0.0.1:3304",
		},
	},
}

func runTestMain(m *testing.M) int {
	dialers := make([]tarantool.NetDialer, instancesCount)
	opts := make([]test_helpers.StartOpts, instancesCount)

	i := 0
	for name, addr := range serverNames {
		dialers[i] = tarantool.NetDialer{
			Address: addr,
			User:    "guest",
		}

		opts[i] = test_helpers.StartOpts{
			Dialer:       dialers[i],
			InitScript:   "config.lua",
			Listen:       addr,
			WaitStart:    100 * time.Millisecond,
			ConnectRetry: 100,
			RetryTimeout: 500 * time.Millisecond,
			WorkDir:      name, // this is not wrong
		}

		i++
	}

	instances, err := chelper.StartTarantoolInstances(opts)
	defer test_helpers.StopTarantoolInstances(instances)
	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}