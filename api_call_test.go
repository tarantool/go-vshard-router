package vshard_router //nolint:revive

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
	"github.com/vmihailenco/msgpack/v5"

	mockpool "github.com/tarantool/go-vshard-router/v2/mocks/pool"
)

func finishedFuture(t *testing.T, data any) *tarantool.Future {
	t.Helper()

	body := bytes.NewBuffer(nil)
	require.NoError(t, msgpack.NewEncoder(body).Encode(data))

	future := tarantool.NewFuture(test_helpers.NewMockRequest())
	require.NoError(t, future.SetResponse(tarantool.Header{}, body))

	return future
}

// vshardErrorResp is the array[nil, vshard_error] answer of
// vshard.storage.call.
func vshardErrorResp(name, destination string) []any {
	return []any{nil, StorageCallVShardError{Name: name, Destination: destination}}
}

// okResp is the array[true, value] answer of vshard.storage.call.
func okResp(value any) []any {
	return []any{true, value}
}

// storagePooler returns a Pooler mock for a single replicaset.
func storagePooler(t *testing.T, discoveryBuckets []uint64, callResp func(callNo int) any) Pooler {
	t.Helper()

	var calls atomic.Int64

	mPool := mockpool.NewPooler(t)
	mPool.On("Do", mock.Anything, mock.Anything).Return(
		func(_ tarantool.Request, mode pool.Mode) *tarantool.Future {
			switch mode { //nolint:exhaustive
			case pool.PreferRO: // bucketsDiscoveryAsync
				return finishedFuture(t, []any{
					bucketsDiscoveryResp{Buckets: discoveryBuckets},
				})
			case pool.RW: // vshard.storage.call for CallModeRW
				return finishedFuture(t, callResp(int(calls.Add(1))-1))
			default:
				future := tarantool.NewFuture(test_helpers.NewMockRequest())
				future.SetError(fmt.Errorf("test: unexpected pool mode %v", mode))

				return future
			}
		})

	return mPool
}

func TestRouter_Call_BucketIDOutOfRange(t *testing.T) {
	t.Parallel()

	r, _ := testRouter(testRouterUpperBound, "rs_1")

	_, err := r.Call(context.Background(), testRouterUpperBound+1, CallModeRW, "echo", nil, CallOpts{})
	require.ErrorContains(t, err, "bucket id is out of range")
}

func TestRouter_Call_UnsupportedMode(t *testing.T) {
	t.Parallel()

	r, _ := testRouter(testRouterUpperBound, "rs_1")

	_, err := r.Call(context.Background(), 1, CallModeRE, "echo", nil, CallOpts{})
	require.ErrorContains(t, err, "mode CallModeRE is not supported yet")

	_, err = r.Call(context.Background(), 1, CallMode(42), "echo", nil, CallOpts{})
	require.ErrorContains(t, err, "unknown CallMode(42)")
}

// The storage answers WRONG_BUCKET and names the replicaset the bucket
// moved to, so Call must reroute the bucket and retry there.
func TestRouter_Call_DestinationIsReplicasetName(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(testRouterUpperBound, "rs_1", "rs_2")

	nameToRs["rs_1"].conn = storagePooler(t, nil, func(int) any {
		return vshardErrorResp(VShardErrNameWrongBucket, "rs_2")
	})
	nameToRs["rs_2"].conn = storagePooler(t, nil, func(int) any {
		return okResp("done")
	})

	r.getRouteMap().set(bucketID, nameToRs["rs_1"])

	resp, err := r.Call(context.Background(), bucketID, CallModeRW, "echo", nil, CallOpts{Timeout: time.Second})
	require.NoError(t, err)

	result, err := resp.Get()
	require.NoError(t, err)
	require.Equal(t, []any{"done"}, result)

	require.Same(t, nameToRs["rs_2"], r.getRouteMap().get(bucketID))
}

func TestRouter_Call_DestinationIsUUID(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(testRouterUpperBound, "rs_1", "rs_2")

	destination := nameToRs["rs_2"].info.UUID.String()

	nameToRs["rs_1"].conn = storagePooler(t, nil, func(int) any {
		return vshardErrorResp(VShardErrNameWrongBucket, destination)
	})
	nameToRs["rs_2"].conn = storagePooler(t, nil, func(int) any {
		return okResp("done")
	})

	r.getRouteMap().set(bucketID, nameToRs["rs_1"])

	resp, err := r.Call(context.Background(), bucketID, CallModeRW, "echo", nil, CallOpts{Timeout: time.Second})
	require.NoError(t, err)

	result, err := resp.Get()
	require.NoError(t, err)
	require.Equal(t, []any{"done"}, result)

	require.Same(t, nameToRs["rs_2"], r.getRouteMap().get(bucketID))
}

// The destination is not in the topology at all, so Call keeps polling until
// the request timeout and then returns the error the storage sent.
func TestRouter_Call_DestinationNotInTopology(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(testRouterUpperBound, "rs_1")

	nameToRs["rs_1"].conn = storagePooler(t, nil, func(int) any {
		return vshardErrorResp(VShardErrNameBucketIsLocked, "rs_unknown")
	})

	r.getRouteMap().set(bucketID, nameToRs["rs_1"])

	_, err := r.Call(context.Background(), bucketID, CallModeRW, "echo", nil,
		CallOpts{Timeout: 150 * time.Millisecond})

	var vshardError *StorageCallVShardError
	require.ErrorAs(t, err, &vshardError)
	require.Equal(t, VShardErrNameBucketIsLocked, vshardError.Name)
	require.Equal(t, "rs_unknown", vshardError.Destination)

	require.Nil(t, r.getRouteMap().get(bucketID))
}

// The destination shows up in the topology only after Call has already
// entered the polling loop, so Call must re-read the router state to
// notice it.
func TestRouter_Call_DestinationAppearsAfterTopologyChange(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(testRouterUpperBound, "rs_1")

	rs2 := &Replicaset{
		info: ReplicasetInfo{Name: "rs_2", UUID: uuid.New()},
		conn: storagePooler(t, nil, func(int) any {
			return okResp("done")
		}),
	}

	nameToRs["rs_1"].conn = storagePooler(t, nil, func(int) any {
		return vshardErrorResp(VShardErrNameTransferIsInProgress, "rs_2")
	})

	r.getRouteMap().set(bucketID, nameToRs["rs_1"])

	go func() {
		time.Sleep(150 * time.Millisecond)

		newNameToRs := nameToReplicasetMap{"rs_1": nameToRs["rs_1"], "rs_2": rs2}
		r.nameToReplicaset.Store(&newNameToRs)
	}()

	resp, err := r.Call(context.Background(), bucketID, CallModeRW, "echo", nil,
		CallOpts{Timeout: 5 * time.Second})
	require.NoError(t, err)

	result, err := resp.Get()
	require.NoError(t, err)
	require.Equal(t, []any{"done"}, result)

	require.Same(t, rs2, r.getRouteMap().get(bucketID))
}

// A vshard error without a destination gives Call no hint where the bucket
// went, so it just resets the bucket and rediscovers it before the retry.
func TestRouter_Call_WrongBucketWithoutDestination(t *testing.T) {
	t.Parallel()

	const bucketID = uint64(1)

	r, nameToRs := testRouter(testRouterUpperBound, "rs_1")
	r.cfg.BucketsSearchMode = BucketsSearchBatchedQuick

	nameToRs["rs_1"].conn = storagePooler(t, []uint64{bucketID}, func(callNo int) any {
		if callNo == 0 {
			return vshardErrorResp(VShardErrNameWrongBucket, "")
		}

		return okResp("done")
	})

	r.getRouteMap().set(bucketID, nameToRs["rs_1"])

	resp, err := r.Call(context.Background(), bucketID, CallModeRW, "echo", nil, CallOpts{Timeout: time.Second})
	require.NoError(t, err)

	result, err := resp.Get()
	require.NoError(t, err)
	require.Equal(t, []any{"done"}, result)

	require.Same(t, nameToRs["rs_1"], r.getRouteMap().get(bucketID))
}
