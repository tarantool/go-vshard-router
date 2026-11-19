package vshard_router //nolint:revive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
	"github.com/vmihailenco/msgpack/v5"

	mockpool "github.com/tarantool/go-vshard-router/v2/mocks/pool"
)

const (
	sparseBucketA uint64 = 1<<35 + 7
	sparseBucketB uint64 = 1<<36 + 7
)

func newBucketTestRouter(t testing.TB, sparse bool, replicasets ...*Replicaset) *Router {
	t.Helper()
	router := &Router{cfg: Config{
		TotalBucketCount: 2,
		SparseBucketIDs:  sparse,
		Metrics:          emptyMetricsProvider,
	}}
	router.setEmptyRouteMap()
	names := make(map[string]*Replicaset, len(replicasets))
	for _, rs := range replicasets {
		names[rs.info.Name] = rs
	}
	require.NoError(t, router.swapNameToReplicaset(nil, &names))
	return router
}

// Use real iproto response bodies so the tests also exercise the connector's
// decoding, not just the router's internal response structs.
func bucketTestResponse(t testing.TB, req tarantool.Request, data []any) tarantool.Future {
	t.Helper()
	body, err := msgpack.Marshal(map[uint64]any{uint64(iproto.IPROTO_DATA): data})
	if err != nil {
		t.Errorf("encode response: %v", err)
		return tarantool.NewFutureWithErr(req, err)
	}
	future, err := tarantool.NewFutureWithResponse(req, tarantool.Header{}, bytes.NewReader(body))
	if err != nil {
		t.Errorf("create response future: %v", err)
		return tarantool.NewFutureWithErr(req, err)
	}
	return future
}

func bucketTestRequest(req tarantool.Request) (string, []msgpack.RawMessage, error) {
	var buf bytes.Buffer
	if err := req.Body(nil, msgpack.NewEncoder(&buf)); err != nil {
		return "", nil, err
	}
	var body map[uint64]msgpack.RawMessage
	if err := msgpack.Unmarshal(buf.Bytes(), &body); err != nil {
		return "", nil, err
	}
	var function string
	if err := msgpack.Unmarshal(body[uint64(iproto.IPROTO_FUNCTION_NAME)], &function); err != nil {
		return "", nil, err
	}
	var args []msgpack.RawMessage
	err := msgpack.Unmarshal(body[uint64(iproto.IPROTO_TUPLE)], &args)
	return function, args, err
}

func expectBucketTestID(t testing.TB, req tarantool.Request, function string, bucketID uint64) {
	t.Helper()
	actualFunction, args, err := bucketTestRequest(req)
	if err != nil {
		t.Errorf("decode request: %v", err)
		return
	}
	if actualFunction != function || len(args) == 0 {
		t.Errorf("unexpected request: function=%q, arguments=%d", actualFunction, len(args))
		return
	}
	var actualID uint64
	if err := msgpack.Unmarshal(args[0], &actualID); err != nil {
		t.Errorf("decode bucket id: %v", err)
		return
	}
	if actualID != bucketID {
		t.Errorf("bucket id on the wire: got %d, want %d", actualID, bucketID)
	}
}

func TestSparseBucketCache(t *testing.T) {
	t.Parallel()
	mp := mockpool.NewPooler(t)
	mp.On("Do", mock.Anything, mock.Anything).Return(
		func(req tarantool.Request, _ pool.Mode) tarantool.Future {
			return tarantool.NewFutureWithErr(req, errors.New("bucket is absent"))
		}).Maybe()
	first := &Replicaset{info: ReplicasetInfo{Name: "first"}, conn: mp}
	second := &Replicaset{info: ReplicasetInfo{Name: "second"}, conn: mp}
	router := newBucketTestRouter(t, true, first, second)
	ctx := context.Background()

	_, err := router.BucketSet(sparseBucketA, first.info.Name)
	require.NoError(t, err)
	_, err = router.BucketSet(sparseBucketB, second.info.Name)
	require.NoError(t, err)
	for id, expected := range map[uint64]*Replicaset{sparseBucketA: first, sparseBucketB: second} {
		actual, err := router.Route(ctx, id)
		require.NoError(t, err)
		require.Same(t, expected, actual)
	}
	require.Equal(t, uint64(2), router.BucketCount())

	router.BucketReset(sparseBucketA)
	actual, err := router.Route(ctx, sparseBucketA)
	require.Error(t, err)
	require.Nil(t, actual)
	actual, err = router.Route(ctx, sparseBucketB)
	require.NoError(t, err)
	require.Same(t, second, actual)

	// Removing an absent key and a previously removed key must not leave a
	// typed nil that later discovery mistakes for a live replicaset.
	router.BucketReset(sparseBucketA)
	router.BucketReset(^uint64(0))
	router.DiscoveryHandleBuckets(ctx, nil, []uint64{sparseBucketB})
	actual, err = router.Route(ctx, sparseBucketB)
	require.Error(t, err)
	require.Nil(t, actual)
	router.DiscoveryHandleBuckets(ctx, first, []uint64{sparseBucketA, sparseBucketB})
	actual, err = router.Route(ctx, sparseBucketB)
	require.NoError(t, err)
	require.Same(t, first, actual)

	router.RouteMapClean()
	for _, id := range []uint64{sparseBucketA, sparseBucketB} {
		actual, err := router.Route(ctx, id)
		require.Error(t, err)
		require.Nil(t, actual)
	}
}

func TestSparseBucketBounds(t *testing.T) {
	t.Parallel()
	for _, sparse := range []bool{false, true} {
		t.Run(fmt.Sprintf("sparse=%t", sparse), func(t *testing.T) {
			rs := &Replicaset{info: ReplicasetInfo{Name: "storage"}}
			router := newBucketTestRouter(t, sparse, rs)
			ctx := context.Background()
			invalid := []uint64{0}
			if !sparse {
				invalid = append(invalid, 3, sparseBucketA, ^uint64(0))
			}
			for _, id := range invalid {
				_, err := router.Route(ctx, id)
				require.ErrorContains(t, err, "out of range")
				_, err = router.Call(ctx, id, CallModeRW, "echo", nil, CallOpts{})
				require.ErrorContains(t, err, "out of range")
				_, err = router.BucketSet(id, rs.info.Name)
				require.ErrorContains(t, err, "out of range")
				router.BucketReset(id)
			}
			require.Panics(t, func() {
				router.DiscoveryHandleBuckets(ctx, rs, invalid)
			})
			valid := []uint64{1, 2}
			if sparse {
				valid = append(valid, sparseBucketA, ^uint64(0))
			}
			for _, id := range valid {
				_, err := router.BucketSet(id, rs.info.Name)
				require.NoError(t, err)
				actual, err := router.Route(ctx, id)
				require.NoError(t, err)
				require.Same(t, rs, actual)
			}
		})
	}
}

func TestSparseBucketDiscoveryAll(t *testing.T) {
	t.Parallel()
	mp := mockpool.NewPooler(t)
	page := 0
	mp.On("Do", mock.Anything, pool.ModePreferRO).Return(
		func(req tarantool.Request, _ pool.Mode) tarantool.Future {
			function, args, err := bucketTestRequest(req)
			if err != nil || function != "vshard.storage.buckets_discovery" || len(args) != 1 {
				t.Errorf("unexpected discovery request: %q, %v", function, err)
				return tarantool.NewFutureWithErr(req, errors.New("invalid discovery request"))
			}
			var pagination struct {
				From uint64 `msgpack:"from"`
			}
			if err := msgpack.Unmarshal(args[0], &pagination); err != nil {
				t.Errorf("decode pagination: %v", err)
				return tarantool.NewFutureWithErr(req, err)
			}
			page++
			if page == 1 {
				if pagination.From != 0 {
					t.Errorf("initial pagination from: %d", pagination.From)
				}
				return bucketTestResponse(t, req, []any{bucketsDiscoveryResp{
					Buckets: []uint64{0, sparseBucketA}, NextFrom: sparseBucketB,
				}})
			}
			if pagination.From != sparseBucketB {
				t.Errorf("next page lost the physical bucket id: %d", pagination.From)
			}
			return bucketTestResponse(t, req, []any{bucketsDiscoveryResp{Buckets: []uint64{sparseBucketB}}})
		}).Twice()
	rs := &Replicaset{info: ReplicasetInfo{Name: "storage"}, conn: mp}
	router := newBucketTestRouter(t, true, rs)
	require.NoError(t, router.DiscoveryAllBuckets(context.Background()))
	for _, id := range []uint64{sparseBucketA, sparseBucketB} {
		actual, err := router.Route(context.Background(), id)
		require.NoError(t, err)
		require.Same(t, rs, actual)
	}
}

func TestSparseBucketDiscoverySearch(t *testing.T) {
	t.Parallel()
	for _, mode := range []BucketsSearchMode{BucketsSearchLegacy, BucketsSearchBatchedQuick, BucketsSearchBatchedFull} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			mp := mockpool.NewPooler(t)
			mp.On("Do", mock.Anything, mock.Anything).Return(
				func(req tarantool.Request, _ pool.Mode) tarantool.Future {
					if mode == BucketsSearchLegacy {
						expectBucketTestID(t, req, "vshard.storage.bucket_stat", sparseBucketA)
						return bucketTestResponse(t, req, []any{map[string]any{
							"id": sparseBucketA, "status": "active",
						}})
					}
					return bucketTestResponse(t, req, []any{bucketsDiscoveryResp{Buckets: []uint64{0, sparseBucketA}}})
				}).Once()
			rs := &Replicaset{info: ReplicasetInfo{Name: "storage"}, conn: mp}
			router := newBucketTestRouter(t, true, rs)
			router.cfg.BucketsSearchMode = mode
			_, err := router.BucketSet(sparseBucketB, rs.info.Name)
			require.NoError(t, err)
			for _, id := range []uint64{sparseBucketA, sparseBucketB} {
				actual, err := router.Route(context.Background(), id)
				require.NoError(t, err)
				require.Same(t, rs, actual)
			}
		})
	}
}

func TestSparseBucketCallMigration(t *testing.T) {
	t.Parallel()
	for _, errorName := range []string{VShardErrNameWrongBucket, VShardErrNameTransferIsInProgress} {
		t.Run(errorName, func(t *testing.T) {
			sourcePool := mockpool.NewPooler(t)
			destinationPool := mockpool.NewPooler(t)
			source := &Replicaset{info: ReplicasetInfo{Name: "source"}, conn: sourcePool}
			destination := &Replicaset{info: ReplicasetInfo{
				Name: "destination", UUID: uuid.MustParse("00000000-0000-0000-0000-000000000002"),
			}, conn: destinationPool}
			sourcePool.On("Do", mock.Anything, pool.ModeRW).Return(
				func(req tarantool.Request, _ pool.Mode) tarantool.Future {
					expectBucketTestID(t, req, "vshard.storage.call", sparseBucketA)
					return bucketTestResponse(t, req, []any{nil, StorageCallVShardError{
						Name: errorName, BucketID: sparseBucketA, Destination: destination.info.UUID.String(),
					}})
				}).Once()
			destinationPool.On("Do", mock.Anything, pool.ModeRW).Return(
				func(req tarantool.Request, _ pool.Mode) tarantool.Future {
					expectBucketTestID(t, req, "vshard.storage.call", sparseBucketA)
					return bucketTestResponse(t, req, []any{true, "ok"})
				}).Once()
			router := newBucketTestRouter(t, true, source, destination)
			_, err := router.BucketSet(sparseBucketA, source.info.Name)
			require.NoError(t, err)
			response, err := router.Call(context.Background(), sparseBucketA, CallModeRW,
				"echo", []any{"payload"}, CallOpts{Timeout: time.Second})
			require.NoError(t, err)
			values, err := response.Get()
			require.NoError(t, err)
			require.Equal(t, []any{"ok"}, values)
			actual, err := router.Route(context.Background(), sparseBucketA)
			require.NoError(t, err)
			require.Same(t, destination, actual)
		})
	}
}

func TestSparseBucketStaleReplicaset(t *testing.T) {
	t.Parallel()
	old := &Replicaset{info: ReplicasetInfo{Name: "storage"}}
	router := newBucketTestRouter(t, true, old)
	_, err := router.BucketSet(sparseBucketA, old.info.Name)
	require.NoError(t, err)
	replacement := &Replicaset{info: old.info}
	names := map[string]*Replicaset{replacement.info.Name: replacement}
	require.NoError(t, router.swapNameToReplicaset(router.nameToReplicaset.Load(), &names))
	actual, err := router.Route(context.Background(), sparseBucketA)
	require.NoError(t, err)
	require.Same(t, replacement, actual)
	names = map[string]*Replicaset{}
	require.NoError(t, router.swapNameToReplicaset(router.nameToReplicaset.Load(), &names))
	actual, err = router.Route(context.Background(), sparseBucketA)
	require.Error(t, err)
	require.Nil(t, actual)
}

func TestSparseBucketBootstrapRejected(t *testing.T) {
	t.Parallel()
	router := newBucketTestRouter(t, true)
	require.Error(t, router.ClusterBootstrap(context.Background(), false))
}

func TestSparseBucketConcurrentCache(t *testing.T) {
	mp := mockpool.NewPooler(t)
	mp.On("Do", mock.Anything, mock.Anything).Return(
		func(req tarantool.Request, _ pool.Mode) tarantool.Future {
			return tarantool.NewFutureWithErr(req, errors.New("bucket is absent"))
		}).Maybe()
	rs := &Replicaset{info: ReplicasetInfo{Name: "storage"}, conn: mp}
	router := newBucketTestRouter(t, true, rs)
	ctx := context.Background()
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Go(func() {
			for i := 0; i < 250; i++ {
				if _, err := router.BucketSet(sparseBucketA, rs.info.Name); err != nil {
					t.Errorf("set bucket: %v", err)
					return
				}
				router.DiscoveryHandleBuckets(ctx, rs, []uint64{sparseBucketA, sparseBucketB})
				actual, err := router.Route(ctx, sparseBucketA)
				if err == nil && actual != rs {
					t.Errorf("unexpected replicaset: %v", actual)
				}
				router.BucketReset(sparseBucketA)
				router.DiscoveryHandleBuckets(ctx, nil, []uint64{sparseBucketB})
				router.RouteMapClean()
			}
		})
	}
	workers.Wait()
	_, err := router.BucketSet(sparseBucketA, rs.info.Name)
	require.NoError(t, err)
	actual, err := router.Route(ctx, sparseBucketA)
	require.NoError(t, err)
	require.Same(t, rs, actual)
}

func BenchmarkBucketRouteCached(b *testing.B) {
	for _, sparse := range []bool{false, true} {
		name := "dense"
		if sparse {
			name = "sparse"
		}
		for _, parallel := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/parallel=%t", name, parallel), func(b *testing.B) {
				const count = 1024
				rs := &Replicaset{info: ReplicasetInfo{Name: "storage"}}
				router := newBucketTestRouter(b, sparse, rs)
				router.cfg.TotalBucketCount = count
				router.RouteMapClean()
				ids := make([]uint64, count)
				for i := range ids {
					ids[i] = uint64(i + 1)
					if sparse {
						ids[i] += sparseBucketA
					}
					_, err := router.BucketSet(ids[i], rs.info.Name)
					require.NoError(b, err)
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						i := 0
						for pb.Next() {
							actual, err := router.Route(ctx, ids[i%count])
							if err != nil || actual != rs {
								b.Errorf("cached route: replicaset=%v, error=%v", actual, err)
								return
							}
							i++
						}
					})
				} else {
					for i := 0; i < b.N; i++ {
						actual, err := router.Route(ctx, ids[i%count])
						if err != nil || actual != rs {
							b.Fatalf("cached route: replicaset=%v, error=%v", actual, err)
						}
					}
				}
			})
		}
	}
}
