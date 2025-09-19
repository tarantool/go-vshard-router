package vshard_router //nolint:revive

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tarantool/go-tarantool/v2"
)

// --------------------------------------------------------------------------------
// -- Discovery
// --------------------------------------------------------------------------------

type DiscoveryMode int

const (
	// DiscoveryModeOn is cron discovery with cron timeout
	DiscoveryModeOn DiscoveryMode = iota
	DiscoveryModeOnce
)

// BucketsSearchMode a type, that used to define policy for Router.Route method.
// See type Config for further details.
type BucketsSearchMode int

const (
	// BucketsSearchLegacy implements the same logic as lua router:
	// send bucket_stat request to every replicaset,
	// return a response immediately if any of them succeed.
	BucketsSearchLegacy BucketsSearchMode = iota
	// BucketsSearchBatchedQuick and BucketsSearchBatchedFull implement another logic:
	// send buckets_discovery request to every replicaset with from=bucketID,
	// seek our bucketID in their responses.
	// Additionally, store other bucketIDs in the route map.
	// BucketsSearchBatchedQuick stops iterating over replicasets responses as soon as our bucketID is found.
	BucketsSearchBatchedQuick
	// BucketsSearchBatchedFull implements the same logic as BucketsSearchBatchedQuick,
	// but doesn't stop iterating over replicasets responses as soon as our bucketID is found.
	// Instead, it always iterates over all replicasets responses even bucketID is found.
	BucketsSearchBatchedFull
)

// Route get replicaset object by bucket identifier.
func (r *Router) Route(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		return nil, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	routeMap := r.getRouteMap()
	nameToReplicasetRef := r.getNameToReplicaset()

	return r.route(ctx, nameToReplicasetRef, routeMap, bucketID)
}

func (r *Router) route(ctx context.Context, nameToReplicasetRef map[string]*Replicaset,
	routeMap routeMap, bucketID uint64) (*Replicaset, error) {

	rs := routeMap[bucketID].Load()
	if rs != nil {
		actualRs := nameToReplicasetRef[rs.info.Name]
		switch {
		case actualRs == nil:
			// rs is outdated, can't use it -- let's discover bucket again
			r.bucketReset(routeMap, bucketID)
		case actualRs == rs:
			return rs, nil
		default: // actualRs != rs
			// update rs -> actualRs for this bucket
			_, _ = r.bucketSet(nameToReplicasetRef, routeMap, bucketID, actualRs.info.Name)
			return actualRs, nil
		}
	}

	// it`s ok if in the same time we have few active searches
	r.log().Infof(ctx, "Discovering bucket %d", bucketID)

	if r.cfg.BucketsSearchMode == BucketsSearchLegacy {
		return r.bucketSearchLegacy(ctx, nameToReplicasetRef, routeMap, bucketID)
	}

	return r.bucketSearchBatched(ctx, nameToReplicasetRef, routeMap, bucketID)
}

func (r *Router) bucketSearchLegacy(ctx context.Context,
	nameToReplicasetRef map[string]*Replicaset, routeMap routeMap, bucketID uint64) (*Replicaset, error) {

	type rsFuture struct {
		rsName string
		future *tarantool.Future
	}

	var rsFutures = make([]rsFuture, 0, len(nameToReplicasetRef))
	// Send a bunch of parallel requests
	for rsName, rs := range nameToReplicasetRef {
		rsFutures = append(rsFutures, rsFuture{
			rsName: rsName,
			future: rs.bucketStatAsync(ctx, bucketID),
		})
	}

	for _, rsFuture := range rsFutures {
		if _, err := bucketStatWait(rsFuture.future); err != nil {
			var vshardError StorageCallVShardError
			if !errors.As(err, &vshardError) {
				r.log().Errorf(ctx, "bucketSearchLegacy: bucketStatWait call error for %v: %v", rsFuture.rsName, err)
			}
			// just skip, bucket may not belong to this replicaset
			continue
		}

		// It's ok if several replicasets return ok to bucket_stat command for the same bucketID, just pick any of them.
		rs, err := r.bucketSet(nameToReplicasetRef, routeMap, bucketID, rsFuture.rsName)
		if err != nil {
			r.log().Errorf(ctx, "bucketSearchLegacy: can't set rsID %v for bucketID %d: %v", rsFuture.rsName, bucketID, err)
			return nil, newVShardErrorNoRouteToBucket(bucketID)
		}

		// TODO: should we release resources for unhandled futures?
		return rs, nil
	}

	// All replicasets were scanned, but a bucket was not found anywhere, so most likely it does not exist.
	// It can be wrong, if rebalancing is in progress, and a bucket was found to be RECEIVING on one replicaset,
	// and was not found on other replicasets (it was sent during discovery).

	return nil, newVShardErrorNoRouteToBucket(bucketID)
}

// The approach in bucketSearchLegacy is very ineffective because
// we use N vshard.storage.bucket_stat requests over the network
// to find out the location of a single bucket. So, we use here vshard.storage.buckets_discovery request instead.
// As a result, we find the location for N * 1000 buckets while paying almost the same cost (CPU, network).
// P.S. 1000 is a batch size in response of buckets_discovery, see:
// https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/vshard/storage/init.lua#L1700
// https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/vshard/consts.lua#L37
func (r *Router) bucketSearchBatched(ctx context.Context,
	nameToReplicasetRef map[string]*Replicaset, routeMap routeMap, bucketIDToFind uint64) (*Replicaset, error) {

	type rsFuture struct {
		rs     *Replicaset
		rsName string
		future *tarantool.Future
	}

	var rsFutures = make([]rsFuture, 0, len(nameToReplicasetRef))
	// Send a bunch of parallel requests
	for rsName, rs := range nameToReplicasetRef {
		rsFutures = append(rsFutures, rsFuture{
			rs:     rs,
			rsName: rsName,
			future: rs.bucketsDiscoveryAsync(ctx, bucketIDToFind),
		})
	}

	var rs *Replicaset

	for _, rsFuture := range rsFutures {
		resp, err := bucketsDiscoveryWait(rsFuture.future)
		if err != nil {
			r.log().Errorf(ctx, "bucketSearchBatched: bucketsDiscoveryWait error for %v: %v", rsFuture.rsName, err)
			// just skip, we still may find our bucket in another replicaset
			continue
		}

		for _, bucketID := range resp.Buckets {
			if bucketID == bucketIDToFind {
				// We found where bucketIDToFind is located
				rs = rsFuture.rs
			}

			routeMap[bucketID].Store(rsFuture.rs)
		}

		if bucketIDWasFound := rs != nil; !bucketIDWasFound {
			continue
		}

		if r.cfg.BucketsSearchMode == BucketsSearchBatchedQuick {
			return rs, nil
		}
	}

	if rs == nil {
		return nil, newVShardErrorNoRouteToBucket(bucketIDToFind)
	}

	return rs, nil
}

// DiscoveryHandleBuckets arrange downloaded buckets to the route map so as they reference a given replicaset.
func (r *Router) DiscoveryHandleBuckets(ctx context.Context, rs *Replicaset, buckets []uint64) {
	routeMap := r.getRouteMap()
	removedFrom := make(map[string]int)

	var newRsName string
	if rs != nil {
		newRsName = rs.info.Name
	}

	for _, bucketID := range buckets {
		// We don't validate bucketID intentionally, because we can't return an error here using the current API.
		// We can't silence this error too, the caller should know about invalid arguments.
		// So the only way to inform the caller is to panic here,
		// that should happen if bucketID is out of the array bounds.
		// if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		// 	continue
		// }

		oldRs := routeMap[bucketID].Swap(rs)

		var oldRsName string
		if oldRs != nil {
			oldRsName = oldRs.info.Name
		}

		if oldRsName != newRsName {
			// Something has changed.
			// NOTE: We don't check oldRsName for empty string ("") here,
			// because it's a valid key too (it means bucketID became known)
			removedFrom[oldRsName]++
		}
	}

	var addedToNewRsTotal int
	for removedFromRsName, removedFromCount := range removedFrom {
		addedToNewRsTotal += removedFromCount

		if removedFromRsName == "" {
			// newRsName cannot be an empty string here due to the previous for-loop above.
			r.log().Debugf(ctx, "Added new %d buckets to the cluster map", removedFromCount)
		} else {
			r.log().Debugf(ctx, "Removed %d buckets from replicaset '%s'", removedFromCount, removedFromRsName)
		}
	}

	if newRsName == "" {
		r.log().Infof(ctx, "Removed %d buckets from the cluster map", addedToNewRsTotal)
	} else {
		r.log().Infof(ctx, "Added %d buckets to replicaset '%s'", addedToNewRsTotal, newRsName)
	}
}

func (r *Router) DiscoveryAllBuckets(ctx context.Context) error {
	t := time.Now()

	r.log().Infof(ctx, "Start discovery all buckets")

	var errGr errgroup.Group

	routeMap := r.getRouteMap()
	nameToReplicasetRef := r.getNameToReplicaset()

	for _, rs := range nameToReplicasetRef {
		rs := rs

		errGr.Go(func() error {
			var bucketsDiscoveryPaginationFrom uint64

			for {
				resp, err := rs.bucketsDiscovery(ctx, bucketsDiscoveryPaginationFrom)
				if err != nil {
					r.log().Errorf(ctx, "Can't bucketsDiscovery for rs %s: %v", rs.info, err)
					return err
				}

				for _, bucketID := range resp.Buckets {
					if bucketID > r.cfg.TotalBucketCount {
						r.log().Errorf(ctx, "Ignoring got bucketID is out of range: %d (length %d)",
							bucketID, r.cfg.TotalBucketCount)
						continue
					}

					routeMap[bucketID].Store(rs)
				}

				// There are no more buckets
				// https://github.com/tarantool/vshard/blob/8d299bfe/vshard/storage/init.lua#L1730
				// vshard.storage returns { buckets = [], next_from = nil } if there are no more buckets.
				// Since next_from is always > 0. NextFrom = 0 means that we got next_from = nil, that has not been decoded.
				if resp.NextFrom == 0 {
					return nil
				}

				bucketsDiscoveryPaginationFrom = resp.NextFrom

				// Don't spam many requests at once. Give storages time to handle them and other requests.
				// https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L308
				time.Sleep(r.cfg.DiscoveryWorkStep)
			}
		})
	}

	err := errGr.Wait()
	if err != nil {
		return fmt.Errorf("errGr.Wait() err: %w", err)
	}
	r.log().Infof(ctx, "Discovery done since: %s", time.Since(t))

	return nil
}

// cronDiscovery is discovery_service_f analog with goroutines instead fibers
func (r *Router) cronDiscovery(ctx context.Context) {
	var iterationCount uint64

	for {
		select {
		case <-ctx.Done():
			r.metrics().CronDiscoveryEvent(false, 0, "ctx-cancel")
			r.log().Infof(ctx, "[DISCOVERY] cron discovery has been stopped after %d iterations", iterationCount)
			return
		case <-time.After(r.cfg.DiscoveryTimeout):
			iterationCount++
		}

		// Since the current for loop should not stop until ctx->Done() event fires,
		// we should be able to continue execution even a panic occures.
		// Therefore, we should wrap everything into anonymous function that recovers after panic.
		// (Similar to pcall in lua/tarantool)
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					// Another one panic may happen due to log function below (e.g. bug in log().Errorf), in this case we have two options:
					// 1. recover again and log nothing: panic will be muted and lost
					// 2. don't try to recover, we hope that the second panic will be logged somehow by go runtime
					// So, we desided to combine them in the third behavior: log in another goroutin
					iterationCount := iterationCount
					// get stacktrace in the current goroutine
					debugStack := string(debug.Stack())

					go func() {
						r.log().Errorf(ctx, "[DISCOVERY] something unexpected has happened in cronDiscovery(%d): panic %v, stacktrace: %s",
							iterationCount, recovered, debugStack)
					}()
				}
			}()

			r.log().Infof(ctx, "[DISCOVERY] started cron discovery iteration %d", iterationCount)

			tStartDiscovery := time.Now()

			if err := r.DiscoveryAllBuckets(ctx); err != nil {
				r.metrics().CronDiscoveryEvent(false, time.Since(tStartDiscovery), "discovery-error")
				r.log().Errorf(ctx, "[DISCOVERY] cant do cron discovery iteration %d with error: %s", iterationCount, err)
				return
			}

			r.log().Infof(ctx, "[DISCOVERY] finished cron discovery iteration %d", iterationCount)

			r.metrics().CronDiscoveryEvent(true, time.Since(tStartDiscovery), "ok")
		}()
	}
}
