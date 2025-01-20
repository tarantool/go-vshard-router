package moonlibs

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

func (cfg *Config) Convert() (map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo, error) {
	if cfg.Topology.Instances == nil {
		return nil, fmt.Errorf("no topology instances found")
	}

	if cfg.Topology.Clusters == nil {
		return nil, fmt.Errorf("no topology clusters found")
	}

	// prepare vshard router config
	vshardRouterTopology := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	for rsName, rs := range cfg.Topology.Clusters {
		rsUUID, err := uuid.Parse(rs.ReplicasetUUID)
		if err != nil {
			return nil, fmt.Errorf("invalid topology replicaset UUID: %s", rs.ReplicasetUUID)
		}

		rsInstances := make([]vshardrouter.InstanceInfo, 0)

		for _, instInfo := range cfg.Topology.Instances {
			if instInfo.Cluster != rsName {
				continue
			}

			instUUID, err := uuid.Parse(instInfo.Box.InstanceUUID)
			if err != nil {
				log.Printf("Can't parse replicaset uuid: %s", err)

				return nil, fmt.Errorf("invalid topology instance UUID: %s", instInfo.Box.InstanceUUID)
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

	return vshardRouterTopology, nil
}
