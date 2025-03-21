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

func (r *Router) getNameToReplicaset() map[string]*Replicaset {
	r.nameToReplicasetMutex.RLock()
	nameToReplicasetRef := r.nameToReplicaset
	r.nameToReplicasetMutex.RUnlock()

	return nameToReplicasetRef
}

func (r *Router) setNameToReplicaset(nameToReplicasetNew map[string]*Replicaset) {
	r.nameToReplicasetMutex.Lock()
	r.nameToReplicaset = nameToReplicasetNew
	r.nameToReplicasetMutex.Unlock()
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

	nameToReplicasetOld := r.getNameToReplicaset()

	if _, ok := nameToReplicasetOld[rsInfo.Name]; ok {
		return ErrReplicasetExists
	}

	replicaset := &Replicaset{
		info: rsInfo,
	}

	rsInstances := make([]pool.Instance, 0, len(instances))
	for _, instance := range instances {
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

	replicaset.conn = conn

	// Create an entirely new map object
	nameToReplicasetNew := make(map[string]*Replicaset)
	for k, v := range nameToReplicasetOld {
		nameToReplicasetNew[k] = v
	}
	nameToReplicasetNew[rsInfo.Name] = replicaset // add when conn is ready

	// We could detect concurrent access to the TopologyController interface
	// by comparing references to r.idToReplicaset and idToReplicasetOld.
	// But it requires reflection which I prefer to avoid.
	// See: https://stackoverflow.com/questions/58636694/how-to-know-if-2-go-maps-reference-the-same-data.
	r.setNameToReplicaset(nameToReplicasetNew)

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

	nameToReplicasetOld := r.getNameToReplicaset()

	rs := nameToReplicasetOld[rsName]
	if rs == nil {
		return []error{ErrReplicasetNotExists}
	}

	// Create an entirely new map object
	nameToReplicasetNew := make(map[string]*Replicaset)
	for k, v := range nameToReplicasetOld {
		nameToReplicasetNew[k] = v
	}
	delete(nameToReplicasetNew, rsName)

	r.setNameToReplicaset(nameToReplicasetNew)

	return rs.conn.CloseGraceful()
}
