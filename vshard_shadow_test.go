package vshard_router_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool/v3"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
	"github.com/tarantool/go-vshard-router/v2/providers/static"
)

type errorTopologyProvider struct{}

func (e *errorTopologyProvider) Init(_ vshardrouter.TopologyController) error {
	return fmt.Errorf("test error")
}
func (e *errorTopologyProvider) Close() {}

func TestNewRouter_ProviderError(t *testing.T) {
	ctx := context.TODO()
	_, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TotalBucketCount: 256000,
		TopologyProvider: &errorTopologyProvider{},
	})

	require.ErrorIs(t, err, vshardrouter.ErrTopologyProvider)
}

func TestNewRouter_EmptyReplicasets(t *testing.T) {
	ctx := context.TODO()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{})
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InvalidReplicasetUUID(t *testing.T) {
	ctx := context.TODO()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
			{
				Name: "123",
			}: {
				{Addr: "first.internal:1212"},
			},
		}),
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InstanceAddr(t *testing.T) {
	ctx := context.TODO()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
			{
				Name: "123",
				UUID: uuid.New(),
			}: {
				{Addr: "first.internal:1212"},
			},
		}),
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestRouterBucketIDStrCRC32(t *testing.T) {
	// required values from tarantool example
	require.Equal(t, uint64(103202), vshardrouter.BucketIDStrCRC32("2707623829", uint64(256000)))
	require.Equal(t, uint64(35415), vshardrouter.BucketIDStrCRC32("2706201716", uint64(256000)))
}

func BenchmarkRouterBucketIDStrCRC32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vshardrouter.BucketIDStrCRC32("test_bench_key", uint64(256000))
	}
	b.ReportAllocs()
}

func TestInstanceInfo_Validate(t *testing.T) {
	tCases := []struct {
		Name  string
		II    vshardrouter.InstanceInfo
		Valid bool
	}{
		{
			Name:  "no info",
			II:    vshardrouter.InstanceInfo{},
			Valid: false,
		},
		{
			Name:  "no uuid - ok",
			II:    vshardrouter.InstanceInfo{Addr: "first.internal:1212", Name: "instance_123"},
			Valid: true,
		},
		{
			Name:  "no addr",
			II:    vshardrouter.InstanceInfo{UUID: uuid.New()},
			Valid: false,
		},
		{
			Name:  "no instance name",
			II:    vshardrouter.InstanceInfo{UUID: uuid.New(), Addr: "first.internal:1212"},
			Valid: false,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.Name, func(t *testing.T) {
			err := tc.II.Validate()
			if tc.Valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestNewRouter_RejectsPoolOptsTheV3PoolRefuses(t *testing.T) {
	ctx := context.TODO()

	cases := []struct {
		name string
		opts tarantool.Opts
	}{
		{"reconnect", tarantool.Opts{Reconnect: time.Second}},
		{"max reconnects", tarantool.Opts{MaxReconnects: 5}},
		{"notify", tarantool.Opts{Notify: make(chan<- tarantool.ConnEvent, 1)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
				TotalBucketCount: 256000,
				TopologyProvider: &errorTopologyProvider{},
				PoolOpts:         tc.opts,
			})

			// Rejected by validateCfg, so the provider is never initialized and
			// the caller does not see ErrTopologyProvider. prepareCfg formats with
			// %v rather than %w, so match on the message.
			require.ErrorContains(t, err, "config invalid")
			require.ErrorContains(t, err, "must not be set")
			require.NotErrorIs(t, err, vshardrouter.ErrTopologyProvider)
			require.Nil(t, r)
		})
	}
}
