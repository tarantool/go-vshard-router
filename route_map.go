package vshard_router //nolint:revive

import (
	"sync"
	"sync/atomic"
)

// routeMap keeps dense buckets in an array and sparse buckets in a concurrent
// map. A routeMap must not be copied after use. RouteMapClean replaces the whole
// object so in-flight discovery can finish using its original cache.
type routeMap struct {
	dense  []atomic.Pointer[Replicaset]
	sparse sync.Map
}

func (m *routeMap) load(bucketID uint64) *Replicaset {
	if m.dense != nil {
		return m.dense[bucketID].Load()
	}

	return m.loadSparse(bucketID)
}

func (m *routeMap) loadSparse(bucketID uint64) *Replicaset {
	value, ok := m.sparse.Load(bucketID)
	if !ok {
		return nil
	}

	return value.(*Replicaset)
}

func (m *routeMap) store(bucketID uint64, rs *Replicaset) {
	if m.dense != nil {
		m.dense[bucketID].Store(rs)
		return
	}

	m.storeSparse(bucketID, rs)
}

func (m *routeMap) storeSparse(bucketID uint64, rs *Replicaset) {
	if rs == nil {
		m.sparse.Delete(bucketID)
	} else {
		m.sparse.Store(bucketID, rs)
	}
}

func (m *routeMap) swap(bucketID uint64, rs *Replicaset) *Replicaset {
	if m.dense != nil {
		return m.dense[bucketID].Swap(rs)
	}

	return m.swapSparse(bucketID, rs)
}

func (m *routeMap) swapSparse(bucketID uint64, rs *Replicaset) *Replicaset {
	var value any
	var loaded bool
	if rs == nil {
		value, loaded = m.sparse.LoadAndDelete(bucketID)
	} else {
		value, loaded = m.sparse.Swap(bucketID, rs)
	}
	if !loaded {
		return nil
	}

	return value.(*Replicaset)
}
