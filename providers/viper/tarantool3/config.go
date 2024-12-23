package tarantool3

// ----- Tarantool 3 configuration -----

// Config - configuration for all groups
// based on https://www.tarantool.io/en/doc/latest/getting_started/vshard_quick/#step-4-defining-the-cluster-topology.
type Config struct {
	Groups Group `yaml:"groups"`
}

// Group is a structure for each group configuration
type Group struct {
	Storages *Storages `yaml:"storages,omitempty"`
}

// Storages configuration
type Storages struct {
	App         App                   `yaml:"app"`
	Sharding    Sharding              `yaml:"sharding"`
	Replication Replication           `yaml:"replication"`
	Replicasets map[string]Replicaset `yaml:"replicasets"`
}

// App - general information about the module
type App struct {
	Module string `yaml:"module"`
}

// Sharding configuration
type Sharding struct {
	Roles []string `yaml:"roles"`
}

// Replication configuration
type Replication struct {
	Failover string `yaml:"failover"`
}

// Replicaset configuration
type Replicaset struct {
	Leader    string              `yaml:"leader"`
	Instances map[string]Instance `yaml:"instances"`
}

// Instance in the Replicaset
type Instance struct {
	IProto IProto `yaml:"iproto"`
}

// IProto configuration
type IProto struct {
	Listen []Listen `yaml:"listen"`
}

// Listen configuration (URI for connection)
type Listen struct {
	URI string `yaml:"uri"`
}
