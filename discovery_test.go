package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	mockpool "github.com/tarantool/go-vshard-router/v2/mocks/pool"
)

// poolerWithError returns a Pooler that fails any request.
func poolerWithError(t *testing.T) Pooler {
	t.Helper()

	mPool := mockpool.NewPooler(t)
	mPool.On("Do", mock.Anything, mock.Anything).Return(func(req tarantool.Request, _ pool.Mode) *tarantool.Future {
		future := tarantool.NewFuture(req)
		future.SetError(fmt.Errorf("test: unreachable storage"))

		return future
	})

	return mPool
}

func TestRouter_route_KnownBucket(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(10, "rs_1")

	v := r.view()
	v.routes.set(bucketID, nameToRs["rs_1"])

	rs, err := v.route(context.Background(), bucketID)
	require.NoError(t, err)
	require.Same(t, nameToRs["rs_1"], rs)
}

func TestRouter_route_OutdatedReplicaset(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(10, "rs_1")

	outdatedRs := &Replicaset{info: ReplicasetInfo{Name: "rs_1"}}

	v := r.view()
	v.routes.set(bucketID, outdatedRs)

	rs, err := v.route(context.Background(), bucketID)
	require.NoError(t, err)
	require.Same(t, nameToRs["rs_1"], rs)
	require.Same(t, nameToRs["rs_1"], v.routes.get(bucketID))
}

func TestRouter_route_RemovedReplicaset(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, _ := testRouter(10, "rs_1")

	emptyNameToRs := nameToReplicasetMap{}

	v := r.view()
	v.routes.set(bucketID, &Replicaset{info: ReplicasetInfo{Name: "rs_removed"}})
	v.replicasets = emptyNameToRs

	rs, err := v.route(context.Background(), bucketID)
	require.Error(t, err)
	require.Nil(t, rs)
	require.Nil(t, v.routes.get(bucketID))
}

// A view is a snapshot: it keeps operating on the route map it was
// created with, even after the router has replaced it.
func TestRouter_view_RouteMapSnapshot(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(10, "rs_1")

	v := r.view()

	r.RouteMapClean()

	rs, err := v.bucketSet(bucketID, "rs_1")
	require.NoError(t, err)
	require.Same(t, nameToRs["rs_1"], rs)

	require.Same(t, nameToRs["rs_1"], v.routes.get(bucketID))
	require.Nil(t, r.getRouteMap().get(bucketID))
}

// A view keeps the replicaset map it was created with, so a topology
// change is not visible through an already created view.
func TestRouter_view_ReplicasetsSnapshot(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(10, "rs_1")

	v := r.view()

	newNameToRs := nameToReplicasetMap{}
	r.nameToReplicaset.Store(&newNameToRs)

	rs, err := v.bucketSet(bucketID, "rs_1")
	require.NoError(t, err)
	require.Same(t, nameToRs["rs_1"], rs)

	rs, err = r.BucketSet(bucketID, "rs_1")
	require.Error(t, err)
	require.Nil(t, rs)
}

func TestRouter_route_ConcurrentRouteMapClean(t *testing.T) {
	const (
		totalBucketCount = uint64(100)
		goroutines       = 8
		iterations       = 200
	)

	r, nameToRs := testRouter(totalBucketCount, "rs_1", "rs_2")

	for _, rs := range nameToRs {
		rs.conn = poolerWithError(t)
	}

	routeMap := r.getRouteMap()
	for bucketID := uint64(1); bucketID <= totalBucketCount; bucketID++ {
		routeMap.set(bucketID, nameToRs["rs_1"])
	}

	var (
		wg        sync.WaitGroup
		cleanerWg sync.WaitGroup
	)

	done := make(chan struct{})

	cleanerWg.Add(1)

	go func() {
		defer cleanerWg.Done()

		for {
			select {
			case <-done:
				return
			default:
			}

			r.RouteMapClean()

			newNameToRs := nameToReplicasetMap{"rs_1": nameToRs["rs_1"], "rs_2": nameToRs["rs_2"]}
			r.nameToReplicaset.Store(&newNameToRs)
		}
	}()

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()

			for j := uint64(0); j < iterations; j++ {
				bucketID := j%totalBucketCount + 1

				v := r.view()

				_, _ = v.route(context.Background(), bucketID)
				_, _ = v.bucketSet(bucketID, "rs_2")
				v.bucketReset(bucketID)

				_, _ = r.Route(context.Background(), bucketID)
				_, _ = r.BucketSet(bucketID, "rs_1")
				r.BucketReset(bucketID)
			}
		}()
	}

	wg.Wait()
	close(done)
	cleanerWg.Wait()
}
