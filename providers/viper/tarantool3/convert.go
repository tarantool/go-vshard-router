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

	return m, nil
}
