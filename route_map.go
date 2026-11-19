package vshard_router //nolint:revive

import (
	"sync"
	"sync/atomic"
)

// routeMap is a concurrent bucket-to-replicaset cache. Storing or swapping a
// nil replicaset removes the route. RouteMapClean replaces the whole cache so
// in-flight discovery can finish using its original cache.
type routeMap interface {
	load(bucketID uint64) *Replicaset
	store(bucketID uint64, rs *Replicaset)
	swap(bucketID uint64, rs *Replicaset) *Replicaset
}

type denseRouteMap struct {
	buckets []atomic.Pointer[Replicaset]
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

type sparseRouteMap struct {
	buckets sync.Map
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
