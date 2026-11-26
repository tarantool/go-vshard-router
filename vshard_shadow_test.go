package vshard_router_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

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
	// The values come from vshard: crc is vshard.hash.strcrc32(key), and the
	// bucket IDs are router:bucket_id_strcrc32(key) for the bucket count.
	for _, tc := range []struct {
		name       string
		key        string
		crc        uint64
		bucket256K uint64
		bucket3K   uint64
	}{
		// With nothing to hash, the CRC stays at its initial value: vshard
		// does not invert the result.
		{name: "empty", key: "", crc: 4294967295, bucket256K: 55296, bucket3K: 2296},
		{name: "one byte", key: "a", crc: 1043315919, bucket256K: 115920, bucket3K: 2920},
		{name: "digits", key: "2707623829", crc: 1662567201, bucket256K: 103202, bucket3K: 202},
		{name: "digits 2", key: "2706201716", crc: 1358883414, bucket256K: 35415, bucket3K: 415},
		{name: "ascii", key: "test_bench_key", crc: 3258524077, bucket256K: 156078, bucket3K: 2078},
		{name: "utf-8", key: "привет", crc: 302751245, bucket256K: 159246, bucket3K: 246},
		{name: "binary", key: "\x00\xff\x00", crc: 1014528006, bucket256K: 7, bucket3K: 7},
		{name: "uuid", key: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", crc: 841560429,
			bucket256K: 88430, bucket3K: 430},
		{name: "1 KiB", key: strings.Repeat("k", 1024), crc: 4206706862, bucket256K: 114863, bucket3K: 1863},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.bucket256K, vshardrouter.BucketIDStrCRC32(tc.key, 256000))
			require.Equal(t, tc.bucket3K, vshardrouter.BucketIDStrCRC32(tc.key, 3000))
			require.Equal(t, uint64(1), vshardrouter.BucketIDStrCRC32(tc.key, 1))
			// With 2^32 buckets the bucket ID is the CRC itself plus one.
			require.Equal(t, tc.crc+1, vshardrouter.BucketIDStrCRC32(tc.key, 1<<32))
		})
	}
}

func TestRouterBucketIDStrCRC32_NoAllocs(t *testing.T) {
	key := strings.Repeat("k", 1024)
	allocs := testing.AllocsPerRun(100, func() {
		vshardrouter.BucketIDStrCRC32(key, 256000)
	})
	require.Zero(t, allocs, "BucketIDStrCRC32 must not copy the key")
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
