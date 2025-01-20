## Unreleased

BUG FIXES:
- Fixed go.mod and go.sum files with go mod tidy.

CHANGES:
* DiscoveryAllBuckets: don't cancel requests to other replicasets if request to some replicaset failed.
* AddReplicaset: don't fail if there are no available RW instances.
* NewRouter: don't fail if DiscoveryAllBuckets has returned an error.

TESTS:

- Added etcd v2 provider tests.
- Add test for degraded cluster (TestDegradedCluster).
- Add more tests for etcd v3 viper provider.
- Add tests and correct panics for invalid topology config from viper.

## v2.0.1

in the v2.0.0 release there was a bug that did not allow go-get. Updating to v2 requires changing the module name

## v2.0.0

We are excited to announce the release of version v2!
This major update was necessary due to the initial design of interfaces that were either incorrect or redundant.
In addition, we have fully adapted to work with Tarantool 3,
eliminating the dependency on UUID-based replica sets and instances.
While you can still use these elements for supplementary information, they are no longer mandatory.
The update also includes changes to several previously public methods,
which were not intended to be exposed in the original vshard implementation.
We would like to express our gratitude to everyone who shared their migration issues with us,
as your feedback was invaluable in shaping the current state of the interfaces.

FEATURES:

* Implemented Pooler method that returns go-tarantool Pooler interface. That can be used by some go-tarantool modules(
  like box).

BUG FIXES:

* ClusterBootstrap: eliminate direct access to r.idToReplicaset.

CHANGES:

* Instance UUID no more required, use instance name instead.
* Removed toolchain go1.23.3.
* Refactored GetTyped interface and logic. Now we use raw msg buffer instead raw messages. Interface works and looks
  like go-tarantool response.
* ReplicaCall, RouterCallImpl, RouterMapCallRWImpl, BucketResolve, BucketDiscovery, RouterBucketID, RouterBucketIDMPCRC32 methods were removed cause they work invalid, look useless or redundant.
* All PR, issue references in #XYZ format in commits older than 42f363775dfb9eaf7ec2a6ed7a999847752cec00 refer to https://github.com/KaymeKaydex/go-vshard-router.
* VshardRouterCallMode type renamed to CallMode for simplicity.
* StorageResultTypedFunc type removed as useless type.
* Updated msgpack version from v5.3.5 to v5.4.1.
* Replicaset identifier now is replicaset name instead uuid.
* Remove unused Makefile target.
* Update README.md, README_ru.md.
* Rename RouterRouteAll -> RouteAll, RouterRoute -> Route, RouterBucketIDStrCRC32 -> BucketIDStrCRC32.
* Replace github links to master branch with permalinks.

TESTS:

* Write tests in tests/tnt folder are deprecated.
* Removed empty todo tests from tests/tnt.
* Moved TestReplicasetReplicaCall and Go benches  from tests/tnt to tarantool_test.go .
* TestRouterCallProto rewrote.
* Start using constants in tarantool_test.go instead duplicate variables.
* TestRouterMapCall moved to tarantool_test.go and renamed to TestRouter_RouterMapCallRWImpl.
* Remove discovery_test.go, move tests/tnt/discovery_test.go -> tarantool_test.go

## v1.3.2

CHANGES:

* Updated Go version from 1.20 to 1.22.
* Update go-tarantool to 2.2.1 and testify to v1.10.0.

TESTS:

* Added a test for using etcd v3 with Viper.

REFACTOR:

* Added information about available providers to the README.

## v1.3.1

REFACTOR:

* Change already exists rs error "replicaset exists" to "replicaset already exists".
* Rewrite some comments to en.

FEATURES:

* Added viper tarantool3 topology provider implementation.

EXAMPLES:

* Logger usage in customer example fixed.

## v1.3.0

BUG FIXES:

* Fix decoding fields for StorageCallVShardError (MasterUUID, ReplicasetUUID).

CHANGES:

* Add comment why and how we handle "NON_MASTER" vshard error.
* Don't support 'type Error struct' anymore.
* Linter: don't capitalize error strings and capitalize log.
* Fix misspellings.
* Handle vshard error the same way as lua vshard router (resolve issue #77).
* Decode 'vshard.storage.call' response manually into struct vshardStorageCallResponseProto using DecodeMsgpack
  interface to reduce allocations (partially #61, #100).
* Remove `mapstructure` tag from StorageCallVShardError.
* Update benchmarks in README files.
* Package `mapstructure` is completely removed from direct dependencies list.

FEATURES:

* Add pause between requests in buckets discovering. Configured by config DiscoveryWorkStep, default is 10ms.
* Add ReplicaUUID to the StorageCallVShardError struct.
* New method 'RouterMapCallRW[T]' to replace the deprecated one 'RouterMapCallRWImpl'.
* New method 'Router.Call' to replace the deprecated one 'RouterCallImpl'.

REFACTOR:

* Use constants for vshard error names and codes.
* Reduce SLOC by using CallAsync method.
* BucketForceCreate optimization: don't decode tnt response.
* Remove bucketStatError type, use StorageCallVShardError type instead.
* Add custom msgpackv5 decoder for 'vshard.storage.bucket_stat' response (partially #100).
* Add custom msgpackv5 decoder for 'BucketStatInfo', since msgpackv5 library has an issue (see commit content).
* Add custom msgpackv5 decoder for 'RouterMapCallRW'.
* New backward-compatible signature for StorageResultTypedFunc to fix interface for RouterCallImpl.

TESTS:

* Rename bootstrap_test.go -> tarantool_test.go and new test in this file.
* Test for new 'RouterMapCallRW[T]'.

## v1.2.0

CHANGES:

* We don't support LogProvider interface anymore, only LogfProvider should be used.

BUG FIXES:

* RouterCallImpl: retry on BucketResolve error.
* RouterCallImpl: do not retry on vshard error "TRANSFER_IS_IN_PROGRESS".
* RouterCallImpl: remove misleading RetryOnCall.
* AddInstance bugfix: pass r.cfg.PoolOpts to new instance.

FEATURES:

* Support StdoutLoggerf that allows control log level (resolve issue #84).
* Support new BucketsSearchMode config to set policy for BucketDiscovery (resolve #71).
* Implemented CalculateEtalonBalance based on lua router (part of #32).
* Implement go-tarantool Logger interface to use the same logger as router uses (resolve issue #79).
* Implemented ClusterBootstrap function based on lua router (#32).

REFACTOR:

* Func bucketSearchLegacy: log error from bucketStatWait (except bucketStatError).
* New bucketsDiscoveryAsync, bucketsDiscoveryWait, bucketsDiscovery methods for buckets discovery pagination.
* Support bucketSearchBatched method for batched buckets discovery (resolve #71).

TESTS:

* Tests for BucketsSearchMode (tnt/discovery_test.go).
* The tests have been rewritten in such a way that now you can configure the cluster to suit any needs, the
  configuration of the clusters may differ and the launch is controlled from go.
* Legacy integration tests removed (tests/integration).

## v1.1.0

CHANGES:

* now if there is no etcd connection - etcd provider returns you error instead panic.
* etcd provider also accepts context as input for working with the logger.

BUG FIXES:

* fixed the problem of re-creating a new context when starting discovery, which led to a lack of discovery logging.

TESTS:

* Simplify test/tnt/{Makefile,router.lua).
* Indent fix for test/tnt/storage.lua.

FEATURES:

* now we write topology changes to debug logs.
* now we write info logs about new replicaset adding and nodes state.
* added pull request template to repository.

## v1.0.1

BUG FIXES:

* fix invalid go-tarantool backward compatibility: NewCallRequest not method of router.

## 1.0.0

BUG FIXES:

* RouterCallImpl: fix decoding response from storage_ref (partially #42)
* RouterCallImpl: fix decoding response from storage_map (partially #42)
* BucketDiscovery: check res for nil
* BucketStat: decode bsInfo by ptr
* ReplicaCall: fix decoding response (#42)
* ReplicaCall: fix ignoring timeout while waiting for future.Get()
* Fix retry on Client or LuaJit error

FEATURES:

* Support new Sprintf-like logging interface (#48)
* DiscoveryTimeout by default is 1 minute (zero DiscoveryTimeout is not allowed #60)
* All discovering logs has new prefix [DISCOVERY]
* Introduce Replicaset.CallAsync, it is usefull to send concurrent requests to replicasets;
  additionally, CallAsync provides new interface to interact with replicaset without cons of interface of ReplicaCall
* Retry tarantool request only on some vshard errors (#66).
* Added call interfaces backwards compatible with go-tarantool (#31).

REFACTOR:

* resolve issue #38: simplify DiscoveryAllBuckets and remove suspicious if
* resolve issue #46: drastically simplify RouterMapCallRWImpl and added tests with real tnt
* Use typed nil pointers instead of memory allocation for EmptyMetrics and emptyLogger structs
* resolve issue #44: remove bucketCount field from struct Replicaset
* rename startCronDiscovery to cronDiscovery and make it panic-tolerant
* BucketStat: split into bucketStatAsync and bucketStatWait parts
* BucketDiscovery: do not spawn goroutines, just use futures in the single goroutine
* BucketResolve: make it alias for BucketDiscovery
* do not export anymore storageCallAssertError, bucketStatError

TESTS:

* New test for RouterCallImpl (and fix the old one)
* New tnt tests for discovery logic
* New tnt tests for RouterMapCallRWImpl
* New tnt tests for topology logic
* Big CI update
    * 2 sections for CI: static checks and tests
    * integration tests run on ci with Tarantool cluster on vshard
    * implemented luacheck for static checks
* New tnt tests for ReplicaCall
* New tnt tests for CallAsync
* Init Go benches for tests/tnt

EXAMPLES:

* customer go mod fixed
* add customer example listen addr log

## 0.0.12

BUG FIXES:

* RouterCallImpl: fix decoding the response from vshard.storage.call
* RouterCallImpl: do not return nil error when StorageCallAssertError has happened
* BucketStat: always returns non-nil err, fixed
* DiscoveryAllBuckets returns nil even if errGr.Wait() returns err, fixed
* DiscoveryHandleBuckets: misusage of atomics, fixed
* race when accessing to idToReplicaset, fixed: idToReplicaset is immutable object now
* RouterMapCallRWImpl: fix misusage of refID atomic
* RouterMapCallRWImpl: decode bucketCount into 32 bit integer instead of 16 bit
* RouterMapCallRWImpl: fix concurrent access to idToResult map
* BucketDiscovery: fix possible concurrent access to resultRs and err vars
* RouterMapCallRWImpl: compare totalBucketCount against r.cfg.TotalBucketCount
* issue #39: fixed concurrent access to routeMap: use consistent view (immutable object) + atomics

FEATURES:

* Added etcd v2 topology provider implementation (#16)
* Add TopologyController mock for testing improve
* Add linter job (#33)
* New test framework with real tarantools

REFACTOR:

* Refactored docs (add QuickStart doc) and that library base on vhsard router
* Several linters are enabled because they are usefull
* Ignore .tmp files
* Refactored provider creation test caused by golang-ci lint (#33)
* Router implements directly TopologyController, no proxy object is used now
* Makefile refactored for racetests
* Tests coverage up 22% -> 33%

## 0.0.11

BUG FIXES:

* Fix buckets discovery (it doesn't freeze now)

FEATURES:

* BucketStat has become a public method (#21)
* Add golang-ci logic

REFACTOR:

* WSL lint providers fix
* Lint refactor with spaces
* Split tests for shadow and not vshard module
* Update Makefile with cover & lint
* Add more tests for providers
* TopologyController now is an interface
* Pool struct has been replaced with the Pooler interface, which has improved coverage (#21)

## 0.0.10

BUG FIXES:

* fix empty and multiple tnt procedure responses

FEATURES:

* start write CHANGELOG file
* use TopologyProvider instead direct config topology; if us uses old schema just change it to static provider
* add go report card

REFACTOR:

* refactored place for rs method
