package moonlibs

// ----- Moonlibs configuration -----

// Config is a representation of the topology configuration for tarantool version below 3.
// based on https://github.com/moonlibs/config?tab=readme-ov-file#example-of-etcd-configuration-etcdclustermaster.
type Config struct {
	Topology SourceTopologyConfig `json:"topology"`
}

type SourceTopologyConfig struct {
	Clusters  map[string]ClusterInfo  `json:"clusters,omitempty" yaml:"clusters" `
	Instances map[string]InstanceInfo `json:"instances,omitempty" yaml:"instances"`
}

type ClusterInfo struct {
	ReplicasetUUID string `yaml:"replicaset_uuid" mapstructure:"replicaset_uuid"`
}

type InstanceInfo struct {
	Cluster string
	Box     struct {
		Listen       string `json:"listen,omitempty" yaml:"listen" mapstructure:"listen"`
		InstanceUUID string `yaml:"instance_uuid" mapstructure:"instance_uuid" json:"instanceUUID,omitempty"`
	}
}
