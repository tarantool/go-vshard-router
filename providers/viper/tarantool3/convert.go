package tarantool3

import (
	"fmt"

	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

func (cfg *Config) Convert() (map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo, error) {
	if cfg.Groups.Storages == nil {
		return nil, fmt.Errorf("cant get groups storage from etcd")
	}

	if cfg.Groups.Storages.Replicasets == nil {
		return nil, fmt.Errorf("cant get storage replicasets from etcd")
	}

	m := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	for rsName, rs := range cfg.Groups.Storages.Replicasets {
		rsInfo := vshardrouter.ReplicasetInfo{
			Name: rsName,
			Weight: shardingWeight(
				rs.Sharding.Weight,
				cfg.Groups.Storages.Sharding.Weight,
				cfg.Sharding.Weight,
			),
		}
		instances := make([]vshardrouter.InstanceInfo, 0, len(rs.Instances))

		for instanceName, instance := range rs.Instances {
			instances = append(instances, vshardrouter.InstanceInfo{
				Name: instanceName,
				Addr: instance.IProto.Listen[0].URI,
			})
		}

		m[rsInfo] = instances
	}

	return m, nil
}

// defaultShardingWeight is Tarantool's default for sharding.weight.
const defaultShardingWeight = 1

// shardingWeight returns the first weight set, from the most specific scope
// to the least specific one.
func shardingWeight(weights ...*float64) float64 {
	for _, weight := range weights {
		if weight != nil {
			return *weight
		}
	}

	return defaultShardingWeight
}
