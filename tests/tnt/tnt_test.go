package tnt

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"

	vshardrouter "github.com/tarantool/go-vshard-router/v2"
)

const (
	totalBucketCount = 100

	envNreplicasetsKey = "NREPLICASETS"
	envStartPortKey    = "START_PORT"

	defaultTntUser     = "storage"
	defaultTntPassword = "storage"
)

func getEnvInt(key string) int {
	vStr := os.Getenv(key)

	v, err := strconv.Atoi(vStr)
	if err != nil {
		panic(err)
	}

	if v <= 0 {
		panic(fmt.Sprintf("ENV '%s' invalied: '%s'", key, vStr))
	}

	return v
}

func isCorrectRun() bool {
	if len(os.Getenv(envNreplicasetsKey)) == 0 || len(os.Getenv(envStartPortKey)) == 0 {
		return false
	}

	return true
}

func skipOnInvalidRun(t testing.TB) {
	if !isCorrectRun() {
		log.Printf("Incorrect run of tnt-test framework")

		t.Skip("skipped cause env invalid")
	}
}

func getCfg() map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {
	c := cfgmaker{
		nreplicasets: getEnvInt(envNreplicasetsKey),
		startPort:    getEnvInt(envStartPortKey),
	}

	return c.clusterCfg()
}

func randBucketID(totalBucketCount uint64) uint64 {
	//nolint:gosec
	return (rand.Uint64() % totalBucketCount) + 1
}
