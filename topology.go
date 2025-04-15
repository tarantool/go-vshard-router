package vshard_router //nolint:revive

import (
	"context"
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

var (
	ErrReplicasetExists    = fmt.Errorf("replicaset already exists")
	ErrReplicasetNotExists = fmt.Errorf("replicaset not exists")

	ErrConcurrentTopologyChangeDetected = fmt.Errorf("concurrent topology change detected")
)

// TopologyController is an entity that allows you to interact with the topology.
// TopologyController is not concurrent safe.
// This decision is made intentionally because there is no point in providing concurrence safety for this case.
// In any case, a caller can use his own external synchronization primitive to handle concurrent access.
type TopologyController interface {
	AddInstance(ctx context.Context, rsName string, info InstanceInfo) error
	RemoveReplicaset(ctx context.Context, rsName string) []error
	RemoveInstance(ctx context.Context, rsName, instanceName string) error
	AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error
	AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error
}

func copyMap[K comparable, V any](m map[K]V) map[K]V {
	copy := make(map[K]V)
	for k, v := range m {
		copy[k] = v
	}
	return copy
}

func (r *Router) setEmptyNameToReplicaset() {
	var nameToReplicasetRef map[string]*Replicaset
	_ = r.swapNameToReplicaset(nil, &nameToReplicasetRef)
}

func (r *Router) swapNameToReplicaset(old, new *map[string]*Replicaset) error {
	if swapped := r.nameToReplicaset.CompareAndSwap(old, new); !swapped {
		return ErrConcurrentTopologyChangeDetected
	}
	return nil
}

func (r *Router) getNameToReplicaset() map[string]*Replicaset {
	ptr := r.nameToReplicaset.Load()
	return *ptr
}

func (r *Router) Topology() TopologyController {
	return r
}

func (r *Router) AddInstance(ctx context.Context, rsName string, info InstanceInfo) error {
	r.log().Debugf(ctx, "Trying to add instance %s to router topology in rs %s", info, rsName)

	err := info.Validate()
	if err != nil {
		return err
	}

	instance := pool.Instance{
		Name: info.Name,
		Dialer: tarantool.NetDialer{
			Address:  info.Addr,
			User:     r.cfg.User,
			Password: r.cfg.Password,
		},
		Opts: r.cfg.PoolOpts,
	}

	nameToReplicasetRef := r.getNameToReplicaset()

	rs := nameToReplicasetRef[rsName]
	if rs == nil {
		return ErrReplicasetNotExists
	}

	return rs.conn.Add(ctx, instance)
}

// RemoveInstance removes a specific instance from the router topology within a replicaset.
// It takes a context, the replicaset name (rsName), and the instance name (instanceName) as inputs.
// If the replicaset name is empty, it searches through all replica sets to locate the instance.
// Returns an error if the specified replicaset does not exist or if any issue occurs during removal.
func (r *Router) RemoveInstance(ctx context.Context, rsName, instanceName string) error {
	r.log().Debugf(ctx, "Trying to remove instance %s from router topology in rs %s", instanceName, rsName)

	nameToReplicasetRef := r.getNameToReplicaset()

	var rs *Replicaset

	if rsName == "" {
		r.log().Debugf(ctx, "Replicaset name is not provided for instance %s, attempting to find it",
			instanceName)

		for _, trs := range nameToReplicasetRef {
			_, exists := trs.conn.GetInfo()[instanceName]
			if exists {
				r.log().Debugf(ctx, "Replicaset found for instance %s, removing it", instanceName)

				rs = trs
			}
		}
	} else {
		rs = nameToReplicasetRef[rsName]
	}

	if rs == nil {
		return ErrReplicasetNotExists
	}

	return rs.conn.Remove(instanceName)
}

func (r *Router) AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error {
	r.log().Debugf(ctx, "Trying to add replicaset %s to router topology", rsInfo)

	err := rsInfo.Validate()
	if err != nil {
		return err
	}

	nameToReplicasetOldPtr := r.nameToReplicaset.Load()
	if _, ok := (*nameToReplicasetOldPtr)[rsInfo.Name]; ok {
		return ErrReplicasetExists
	}

	rsInstances := make([]pool.Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.Name == "" {
			instance.Name = instance.UUID.String()

			r.log().Warnf(ctx, "Instance name is empty, using uuid (%s) instead", instance.Name)
		}

		rsInstances = append(rsInstances, pool.Instance{
			Name: instance.Name,
			Dialer: tarantool.NetDialer{
				Address:  instance.Addr,
				User:     r.cfg.User,
				Password: r.cfg.Password,
			},
			Opts: r.cfg.PoolOpts,
		})
	}

	conn, err := pool.Connect(ctx, rsInstances)
	if err != nil {
		return err
	}

	poolInfo := conn.GetInfo()
	for instName, instConnInfo := range poolInfo {
		connectStatus := "connected now"
		if !instConnInfo.ConnectedNow {
			connectStatus = "not connected"
		}

		r.log().Infof(ctx, "[replicaset %s ] instance %s %s in role %s", rsInfo, instName, connectStatus, instConnInfo.ConnRole)
	}

	switch isConnected, err := conn.ConnectedNow(pool.RW); {
	case err != nil:
		r.log().Errorf(ctx, "cant check rs pool conntected rw now with error: %v", err)
	case !isConnected:
		r.log().Errorf(ctx, "got connected now as false to pool.RW")
	}

	replicaset := &Replicaset{
		info: rsInfo,
		conn: conn,
	}

	// Create an entirely new map object
	nameToReplicasetNew := copyMap(*nameToReplicasetOldPtr)
	nameToReplicasetNew[rsInfo.Name] = replicaset // add when conn is ready

	if err = r.swapNameToReplicaset(nameToReplicasetOldPtr, &nameToReplicasetNew); err != nil {
		// replicaset has not added, so just close it
		_ = replicaset.conn.Close()
		return err
	}

	return nil
}

func (r *Router) AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error {
	for rsInfo, rsInstances := range replicasets {
		// We assume that AddReplicasets is called only once during initialization.
		// We also expect that cluster configuration changes very rarely,
		// so we prefer more simple code rather than the efficiency of this part of logic.
		// Even if there are 1000 replicasets, it is still cheap.
		err := r.AddReplicaset(ctx, rsInfo, rsInstances)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Router) RemoveReplicaset(ctx context.Context, rsName string) []error {
	r.log().Debugf(ctx, "Trying to remove replicaset %s from router topology", rsName)

	nameToReplicasetOldPtr := r.nameToReplicaset.Load()
	rs := (*nameToReplicasetOldPtr)[rsName]
	if rs == nil {
		return []error{ErrReplicasetNotExists}
	}

	// Create an entirely new map object
	nameToReplicasetNew := copyMap(*nameToReplicasetOldPtr)
	delete(nameToReplicasetNew, rsName)

	if err := r.swapNameToReplicaset(nameToReplicasetOldPtr, &nameToReplicasetNew); err != nil {
		return []error{err}
	}

	return rs.conn.CloseGraceful()
}
