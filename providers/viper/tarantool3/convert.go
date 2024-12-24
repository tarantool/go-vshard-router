package tarantool3

import (
	vshardrouter "github.com/tarantool/go-vshard-router"
)

func (cfg *Config) Convert() map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {
	m := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	for rsName, rs := range cfg.Groups.Storages.Replicasets {
		rsInfo := vshardrouter.ReplicasetInfo{Name: rsName}
		instances := make([]vshardrouter.InstanceInfo, 0, len(rs.Instances))

		for instanceName, instance := range rs.Instances {
			instances = append(instances, vshardrouter.InstanceInfo{
				Name: instanceName,
				Addr: instance.IProto.Listen[0].URI,
			})
		}

		m[rsInfo] = instances
	}

	return m
}
