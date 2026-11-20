package vshard_router //nolint:revive

import (
	"sync"
	"sync/atomic"
)

// routeMap is a concurrent bucket-to-replicaset cache. Storing or swapping a
// nil replicaset removes the route. RouteMapClean replaces the whole cache so
// in-flight discovery can finish using its original cache.
// Bucket IDs passed to load, store, and swap must be in 1..maxBucketID().
type routeMap interface {
	maxBucketID() uint64
	load(bucketID uint64) *Replicaset
	store(bucketID uint64, rs *Replicaset)
	swap(bucketID uint64, rs *Replicaset) *Replicaset
}

func isValidBucketID(bucketID, maxBucketID uint64) bool {
	return bucketID > 0 && bucketID <= maxBucketID
}

type denseRouteMap struct {
	buckets []atomic.Pointer[Replicaset]
}

func (m *denseRouteMap) maxBucketID() uint64 {
	return uint64(len(m.buckets)) - 1
}

func (m *denseRouteMap) load(bucketID uint64) *Replicaset {
	return m.buckets[bucketID].Load()
}

func (m *denseRouteMap) store(bucketID uint64, rs *Replicaset) {
	m.buckets[bucketID].Store(rs)
}

func (m *denseRouteMap) swap(bucketID uint64, rs *Replicaset) *Replicaset {
	return m.buckets[bucketID].Swap(rs)
}

// maxSparseBucketID matches Tarantool's luaL_pushuint64 number limit (DBL_INT_MAX).
// Tarantool 2.x and 3.x decode unsigned MessagePack integers >= 1e14 as Lua
// cdata, but vshard's bucket_stat and storage.call require a Lua number.
// This is a storage compatibility limit, not the uint64 or float64 maximum.
// https://github.com/tarantool/tarantool/blob/3.9.0/src/lua/utils.c#L596-L613
const maxSparseBucketID uint64 = 99_999_999_999_999

type sparseRouteMap struct {
	buckets sync.Map
}

func (m *sparseRouteMap) maxBucketID() uint64 {
	return maxSparseBucketID
}

func (m *sparseRouteMap) load(bucketID uint64) *Replicaset {
	value, ok := m.buckets.Load(bucketID)
	if !ok {
		return nil
	}

	return value.(*Replicaset)
}

func (m *sparseRouteMap) store(bucketID uint64, rs *Replicaset) {
	if rs == nil {
		m.buckets.Delete(bucketID)
	} else {
		m.buckets.Store(bucketID, rs)
	}
}

func (m *sparseRouteMap) swap(bucketID uint64, rs *Replicaset) *Replicaset {
	var value any
	var loaded bool
	if rs == nil {
		value, loaded = m.buckets.LoadAndDelete(bucketID)
	} else {
		value, loaded = m.buckets.Swap(bucketID, rs)
	}
	if !loaded {
		return nil
	}

	return value.(*Replicaset)
}
