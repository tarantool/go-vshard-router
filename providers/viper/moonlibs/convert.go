package moonlibs

import (
	"log"

	"github.com/google/uuid"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

func (cfg *Config) Convert() map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {
	if cfg.Topology.Instances == nil {
		panic("instances is nil")
	}

	if cfg.Topology.Clusters == nil {
		panic("clusters is nil")
	}

	// prepare vshard router config
	vshardRouterTopology := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	for rsName, rs := range cfg.Topology.Clusters {
		rsUUID, err := uuid.Parse(rs.ReplicasetUUID)
		if err != nil {
			panic("Can't parse replicaset uuid: %s")
		}

		rsInstances := make([]vshardrouter.InstanceInfo, 0)

		for _, instInfo := range cfg.Topology.Instances {
			if instInfo.Cluster != rsName {
				continue
			}

			instUUID, err := uuid.Parse(instInfo.Box.InstanceUUID)
			if err != nil {
				log.Printf("Can't parse replicaset uuid: %s", err)

				panic(err)
			}

			rsInstances = append(rsInstances, vshardrouter.InstanceInfo{
				Addr: instInfo.Box.Listen,
				UUID: instUUID,
			})
		}

		vshardRouterTopology[vshardrouter.ReplicasetInfo{
			Name: rsName,
			UUID: rsUUID,
		}] = rsInstances
	}

	return vshardRouterTopology
}
