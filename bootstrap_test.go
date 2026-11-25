package vshard_router //nolint:revive

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
	"github.com/vmihailenco/msgpack/v5"

	mockpool "github.com/tarantool/go-vshard-router/v2/mocks/pool"
)

// bootstrapTestStorage answers vshard.storage.buckets_count with a fixed count
// and records the ranges passed to vshard.storage.bucket_force_create.
type bootstrapTestStorage struct {
	bucketCount uint64
	countErr    error
	createErr   error

	mu      sync.Mutex
	created [][2]uint64
}

func (s *bootstrapTestStorage) replicaset(t *testing.T, name string, weight float64) *Replicaset {
	t.Helper()
	mp := mockpool.NewPooler(t)
	mp.On("Do", mock.Anything, mock.Anything).Return(
		func(req tarantool.Request, _ pool.Mode) tarantool.Future {
			function, args, err := bucketTestRequest(req)
			if err != nil {
				t.Errorf("decode request: %v", err)
				return tarantool.NewFutureWithErr(req, err)
			}
			switch function {
			case "vshard.storage.buckets_count":
				if s.countErr != nil {
					return tarantool.NewFutureWithErr(req, s.countErr)
				}
				return bucketTestResponse(t, req, []any{s.bucketCount})
			case "vshard.storage.bucket_force_create":
				var first, count uint64
				if len(args) != 2 || msgpack.Unmarshal(args[0], &first) != nil ||
					msgpack.Unmarshal(args[1], &count) != nil {
					t.Errorf("unexpected bucket_force_create arguments: %v", args)
				}
				s.mu.Lock()
				s.created = append(s.created, [2]uint64{first, count})
				s.mu.Unlock()
				if s.createErr != nil {
					return tarantool.NewFutureWithErr(req, s.createErr)
				}
				return bucketTestResponse(t, req, []any{true})
			default:
				t.Errorf("unexpected call: %s", function)
				return tarantool.NewFutureWithErr(req, errors.New("unexpected call"))
			}
		}).Maybe()

	return &Replicaset{info: ReplicasetInfo{Name: name, Weight: weight}, conn: mp}
}

func newBootstrapTestRouter(t *testing.T, totalBucketCount uint64, replicasets ...*Replicaset) *Router {
	t.Helper()
	router := newBucketTestRouter(t, false, replicasets...)
	router.cfg.TotalBucketCount = totalBucketCount
	router.setEmptyRouteMap()

	return router
}

func TestClusterBootstrap_EmptyCluster(t *testing.T) {
	t.Parallel()
	storages := map[string]*bootstrapTestStorage{"a": {}, "b": {}, "c": {}}
	// Names are passed out of order on purpose: bucket ranges must follow the
	// sorted names, not the order of the replicaset map. A zero-weight
	// replicaset gets no buckets and no bucket_force_create call.
	router := newBootstrapTestRouter(t, 8,
		storages["c"].replicaset(t, "c", 0),
		storages["b"].replicaset(t, "b", 3),
		storages["a"].replicaset(t, "a", 1),
	)

	require.NoError(t, router.ClusterBootstrap(context.Background(), false))
	require.Equal(t, [][2]uint64{{1, 2}}, storages["a"].created)
	require.Equal(t, [][2]uint64{{3, 6}}, storages["b"].created)
	require.Empty(t, storages["c"].created)
}

func TestClusterBootstrap_AlreadyBootstrapped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		ifNotBootstrapped bool
	}{
		{name: "error", ifNotBootstrapped: false},
		{name: "if_not_bootstrapped", ifNotBootstrapped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			empty := &bootstrapTestStorage{}
			full := &bootstrapTestStorage{bucketCount: 4}
			router := newBootstrapTestRouter(t, 4,
				empty.replicaset(t, "empty", 1),
				full.replicaset(t, "full", 1),
			)

			err := router.ClusterBootstrap(context.Background(), tc.ifNotBootstrapped)
			if tc.ifNotBootstrapped {
				require.NoError(t, err)
			} else {
				var vshardErr *StorageCallVShardError
				require.ErrorAs(t, err, &vshardErr)
				require.Equal(t, VShardErrNameNonEmpty, vshardErr.Name)
			}
			require.Empty(t, empty.created)
			require.Empty(t, full.created)
		})
	}
}

func TestClusterBootstrap_BucketsCountError(t *testing.T) {
	t.Parallel()
	for _, ifNotBootstrapped := range []bool{false, true} {
		broken := &bootstrapTestStorage{countErr: errors.New("connection refused")}
		healthy := &bootstrapTestStorage{}
		router := newBootstrapTestRouter(t, 4,
			broken.replicaset(t, "broken", 1),
			healthy.replicaset(t, "healthy", 1),
		)

		err := router.ClusterBootstrap(context.Background(), ifNotBootstrapped)
		require.ErrorContains(t, err, "buckets_count on replicaset broken")
		require.Empty(t, broken.created)
		require.Empty(t, healthy.created)
	}
}

func TestClusterBootstrap_BucketsCountErrorIfNotBootstrapped(t *testing.T) {
	t.Parallel()
	// As in vshard.router.bootstrap, one non-empty replicaset is enough to
	// consider the cluster bootstrapped, even if another one did not answer.
	broken := &bootstrapTestStorage{countErr: errors.New("connection refused")}
	full := &bootstrapTestStorage{bucketCount: 4}
	router := newBootstrapTestRouter(t, 4,
		broken.replicaset(t, "broken", 1),
		full.replicaset(t, "full", 1),
	)

	require.NoError(t, router.ClusterBootstrap(context.Background(), true))
	require.Empty(t, broken.created)
	require.Empty(t, full.created)
}

func TestClusterBootstrap_BucketForceCreateError(t *testing.T) {
	t.Parallel()
	failing := &bootstrapTestStorage{createErr: errors.New("disk is full")}
	rest := &bootstrapTestStorage{}
	router := newBootstrapTestRouter(t, 4,
		failing.replicaset(t, "a", 1),
		rest.replicaset(t, "b", 1),
	)

	require.ErrorContains(t, router.ClusterBootstrap(context.Background(), false), "disk is full")
	require.Equal(t, [][2]uint64{{1, 2}}, failing.created)
	require.Empty(t, rest.created, "bootstrap must stop at the first failure")
}

func TestClusterBootstrap_NoReplicasets(t *testing.T) {
	t.Parallel()
	router := newBootstrapTestRouter(t, 4)
	require.Error(t, router.ClusterBootstrap(context.Background(), false))
}
