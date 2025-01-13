package vshard_router //nolint:revive

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// --------------------------------------------------------------------------------
// -- API
// --------------------------------------------------------------------------------

type VshardMode string

const (
	ReadMode  VshardMode = "read"
	WriteMode VshardMode = "write"

	// callTimeoutDefault is a default timeout when no timeout is provided
	callTimeoutDefault = 500 * time.Millisecond
)

func (c VshardMode) String() string {
	return string(c)
}

type vshardStorageCallResponseProto struct {
	AssertError *assertError            // not nil if there is assert error
	VshardError *StorageCallVShardError // not nil if there is vshard response
	CallResp    VshardRouterCallResp
}

func (r *vshardStorageCallResponseProto) DecodeMsgpack(d *msgpack.Decoder) error {
	/* vshard.storage.call(func) response has the next 4 possbile formats:
	See: https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/vshard/storage/init.lua#L3130
	1. vshard error has occurred:
		array[nil, vshard_error]
	2. User method has finished with some error:
		array[false, assert error]
	3. User mehod has finished successfully
		a) but has not returned anything
			array[true]
		b) has returned 1 element
			array[true, elem1]
		c) has returned 2 element
			array[true, elem1, elem2]
		d) has returned 3 element
			array[true, elem1, elem2, elem3]
	*/

	// Ensure it is an array and get array len for protocol violation check
	respArrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if respArrayLen == 0 {
		return fmt.Errorf("protocol violation: invalid array length: %d", respArrayLen)
	}

	// we need peek code to make our check faster than decode interface
	// later we will check if code nil or bool
	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	// this is storage error
	if code == msgpcode.Nil {
		err = d.DecodeNil()
		if err != nil {
			return err
		}

		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on vshard error case", respArrayLen)
		}

		var vshardError StorageCallVShardError

		err = d.Decode(&vshardError)
		if err != nil {
			return fmt.Errorf("failed to decode storage vshard error: %w", err)
		}

		r.VshardError = &vshardError

		return nil
	}

	isVShardRespOk, err := d.DecodeBool()
	if err != nil {
		return err
	}

	if !isVShardRespOk {
		// that means we have an assert errors and response is not ok
		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on assert error case", respArrayLen)
		}

		var assertError assertError
		err = d.Decode(&assertError)
		if err != nil {
			return fmt.Errorf("failed to decode storage assert error: %w", err)
		}

		r.AssertError = &assertError

		return nil
	}

	// isVShardRespOk is true
	buf := bytes.NewBuffer(nil)

	buf.WriteByte(msgpcode.FixedArrayLow | byte(respArrayLen-1))

	_, err = buf.ReadFrom(d.Buffered())
	if err != nil {
		return err
	}

	r.CallResp.buf = buf

	return nil
}

type assertError struct {
	Code     int         `msgpack:"code"`
	BaseType string      `msgpack:"base_type"`
	Type     string      `msgpack:"type"`
	Message  string      `msgpack:"message"`
	Trace    interface{} `msgpack:"trace"`
}

func (s assertError) Error() string {
	// Just print struct as is, use hack with alias type to avoid recursion:
	// %v attempts to call Error() method for s, which is recursion.
	// This alias doesn't have method Error().
	type alias assertError
	return fmt.Sprintf("%+v", alias(s))
}

type StorageCallVShardError struct {
	BucketID uint64 `msgpack:"bucket_id"`
	Reason   string `msgpack:"reason"`
	Code     int    `msgpack:"code"`
	Type     string `msgpack:"type"`
	Message  string `msgpack:"message"`
	Name     string `msgpack:"name"`
	// These 3 fields below are send as string by vshard storage, so we decode them into string, not uuid.UUID type
	// Example: 00000000-0000-0002-0002-000000000000
	MasterUUID     string `msgpack:"master"`
	ReplicasetUUID string `msgpack:"replicaset"`
	ReplicaUUID    string `msgpack:"replica"`
	Destination    string `msgpack:"destination"`
}

func (s StorageCallVShardError) Error() string {
	// Just print struct as is, use hack with alias type to avoid recursion:
	// %v attempts to call Error() method for s, which is recursion.
	// This alias doesn't have method Error().
	type alias StorageCallVShardError
	return fmt.Sprintf("%+v", alias(s))
}

type CallOpts struct {
	Timeout time.Duration
}

// CallMode is a type to represent call mode for Router.Call method.
type CallMode int

const (
	// CallModeRO sets a read-only mode for Router.Call.
	CallModeRO CallMode = iota
	// CallModeRW sets a read-write mode for Router.Call.
	CallModeRW
	// CallModeRE acts like CallModeRO
	// with preference for a replica rather than a master.
	// This mode is not supported yet.
	CallModeRE
	// CallModeBRO acts like CallModeRO with balancing.
	CallModeBRO
	// CallModeBRE acts like CallModeRO with balancing
	// and preference for a replica rather than a master.
	CallModeBRE
)

// VshardRouterCallResp represents a response from Router.Call[XXX] methods.
type VshardRouterCallResp struct {
	buf *bytes.Buffer
}

// Get returns a response from user defined function as []interface{}.
func (r VshardRouterCallResp) Get() ([]interface{}, error) {
	var result []interface{}
	err := r.GetTyped(&result)

	return result, err
}

// GetTyped decodes a response from user defined function into custom values.
func (r VshardRouterCallResp) GetTyped(result interface{}) error {
	return msgpack.Unmarshal(r.buf.Bytes(), result)
}

// Call calls the function identified by 'fnc' on the shard storing the bucket identified by 'bucket_id'.
func (r *Router) Call(ctx context.Context, bucketID uint64, mode CallMode,
	fnc string, args interface{}, opts CallOpts) (VshardRouterCallResp, error) {
	const vshardStorageClientCall = "vshard.storage.call"

	if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		return VshardRouterCallResp{}, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	var poolMode pool.Mode
	var vshardMode VshardMode

	switch mode {
	case CallModeRO:
		poolMode, vshardMode = pool.RO, ReadMode
	case CallModeRW:
		poolMode, vshardMode = pool.RW, WriteMode
	case CallModeRE:
		// poolMode, vshardMode = pool.PreferRO, ReadMode
		// since go-tarantool always use balance=true politic,
		// we can't support this case until: https://github.com/tarantool/go-tarantool/issues/400
		return VshardRouterCallResp{}, fmt.Errorf("mode VshardCallModeRE is not supported yet")
	case CallModeBRO:
		poolMode, vshardMode = pool.ANY, ReadMode
	case CallModeBRE:
		poolMode, vshardMode = pool.PreferRO, ReadMode
	default:
		return VshardRouterCallResp{}, fmt.Errorf("unknown VshardCallMode(%d)", mode)
	}

	timeout := callTimeoutDefault
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tntReq := tarantool.NewCallRequest(vshardStorageClientCall).
		Context(ctx).
		Args([]interface{}{
			bucketID,
			vshardMode,
			fnc,
			args,
		})

	requestStartTime := time.Now()

	var err error

	for {
		if spent := time.Since(requestStartTime); spent > timeout {
			r.metrics().RequestDuration(spent, false, false)

			r.log().Debugf(ctx, "Return result on timeout; spent %s of timeout %s", spent, timeout)
			if err == nil {
				err = fmt.Errorf("cant get call cause call impl timeout")
			}

			return VshardRouterCallResp{}, err
		}

		var rs *Replicaset

		rs, err = r.Route(ctx, bucketID)
		if err != nil {
			r.metrics().RetryOnCall("bucket_resolve_error")

			// this error will be returned to a caller in case of timeout
			err = fmt.Errorf("cant resolve bucket %d: %w", bucketID, err)

			// TODO: lua vshard router just yields here and retires, no pause is applied.
			// https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L713
			// So we also retry here. But I guess we should add some pause here.
			continue
		}

		r.log().Infof(ctx, "Try call %s on replicaset %s for bucket %d", fnc, rs.info.Name, bucketID)

		var storageCallResponse vshardStorageCallResponseProto
		err = rs.conn.Do(tntReq, poolMode).GetTyped(&storageCallResponse)
		if err != nil {
			return VshardRouterCallResp{}, fmt.Errorf("got error on future.GetTyped(): %w", err)
		}

		r.log().Debugf(ctx, "Got call result response data %+v", storageCallResponse)

		if storageCallResponse.AssertError != nil {
			return VshardRouterCallResp{}, fmt.Errorf("%s: %s failed: %+v", vshardStorageClientCall, fnc, storageCallResponse.AssertError)
		}

		if storageCallResponse.VshardError != nil {
			vshardError := storageCallResponse.VshardError

			switch vshardError.Name {
			case VShardErrNameWrongBucket, VShardErrNameBucketIsLocked:
				// We reproduce here behavior in https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L663
				r.BucketReset(bucketID)

				if destination := vshardError.Destination; destination != "" {
					var loggedOnce bool
					for {
						nameToReplicasetRef := r.getNameToReplicaset()

						_, destinationExists := nameToReplicasetRef[destination]

						if !destinationExists {
							// for older logic with uuid we must support backward compatibility
							// if destination is uuid and not name, lets find it too
							for _, rsRef := range nameToReplicasetRef {
								if rsRef.info.UUID.String() == destination {
									destinationExists = true
									break
								}
							}
						}

						if destinationExists {
							_, err := r.BucketSet(bucketID, destination)
							if err == nil {
								break // breaks loop
							}
							r.log().Warnf(ctx, "Failed set bucket %d to %v (possible race): %v", bucketID, destination, err)
						}

						if !loggedOnce {
							r.log().Warnf(ctx, "Replicaset '%v' was not found, but received from storage as destination - please "+
								"update configuration", destination)
							loggedOnce = true
						}

						const defaultPoolingPause = 50 * time.Millisecond
						time.Sleep(defaultPoolingPause)

						if spent := time.Since(requestStartTime); spent > timeout {
							return VshardRouterCallResp{}, vshardError
						}
					}
				}

				// retry for VShardErrNameWrongBucket, VShardErrNameBucketIsLocked

				r.metrics().RetryOnCall("bucket_migrate")

				r.log().Debugf(ctx, "Retrying fnc '%s' cause got vshard error: %v", fnc, vshardError)

				// this vshardError will be returned to a caller in case of timeout
				err = vshardError
				continue
			case VShardErrNameTransferIsInProgress:
				// Since lua vshard router doesn't retry here, we don't retry too.
				// There is a comment why lua vshard router doesn't retry:
				// https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L697
				r.BucketReset(bucketID)
				return VshardRouterCallResp{}, vshardError
			case VShardErrNameNonMaster:
				// vshard.storage has returned NON_MASTER error, lua vshard router updates info about master in this case:
				// See: https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L704.
				// Since we use go-tarantool library, and go-tarantool library doesn't provide API to update info about current master,
				// we just return this error as is.
				return VshardRouterCallResp{}, vshardError
			default:
				return VshardRouterCallResp{}, vshardError
			}
		}

		r.metrics().RequestDuration(time.Since(requestStartTime), true, false)

		return storageCallResponse.CallResp, nil
	}
}

// CallRO is an alias for Call with CallModeRO.
func (r *Router) CallRO(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts CallOpts) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, CallModeRO, fnc, args, opts)
}

// CallRW is an alias for Call with CallModeRW.
func (r *Router) CallRW(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts CallOpts) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, CallModeRW, fnc, args, opts)
}

// CallRE is an alias for Call with CallModeRE.
func (r *Router) CallRE(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts CallOpts) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, CallModeRE, fnc, args, opts)
}

// CallBRO is an alias for Call with CallModeBRO.
func (r *Router) CallBRO(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts CallOpts) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, CallModeBRO, fnc, args, opts)
}

// CallBRE is an alias for Call with CallModeBRE.
func (r *Router) CallBRE(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts CallOpts) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, CallModeBRE, fnc, args, opts)
}

// RouterMapCallRWOptions sets options for RouterMapCallRW.
type RouterMapCallRWOptions struct {
	// Timeout defines timeout for RouterMapCallRW.
	Timeout time.Duration
}

type storageMapResponseProto[T any] struct {
	ok    bool
	value T
	err   StorageCallVShardError
}

func (r *storageMapResponseProto[T]) DecodeMsgpack(d *msgpack.Decoder) error {
	// proto for 'storage_map' method
	// https://github.com/tarantool/vshard/blob/8d299bfecff8bc656056658350ad48c829f9ad3f/vshard/storage/init.lua#L3158
	respArrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if respArrayLen == 0 {
		return fmt.Errorf("protocol violation: invalid array length: %d", respArrayLen)
	}

	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if code == msgpcode.Nil {
		err = d.DecodeNil()
		if err != nil {
			return err
		}

		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on vshard error case", respArrayLen)
		}

		err = d.Decode(&r.err)
		if err != nil {
			return fmt.Errorf("failed to decode storage vshard error: %w", err)
		}

		return nil
	}

	isOk, err := d.DecodeBool()
	if err != nil {
		return err
	}

	if !isOk {
		return fmt.Errorf("protocol violation: isOk=false")
	}

	switch respArrayLen {
	case 1:
		break
	case 2:
		err = d.Decode(&r.value)
		if err != nil {
			return fmt.Errorf("can't decode value %T: %w", r.value, err)
		}
	default:
		return fmt.Errorf("protocol violation: invalid array length when no vshard error: %d", respArrayLen)
	}

	r.ok = true

	return nil
}

type storageRefResponseProto struct {
	err         error
	bucketCount uint64
}

func (r *storageRefResponseProto) DecodeMsgpack(d *msgpack.Decoder) error {
	respArrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if respArrayLen == 0 {
		return fmt.Errorf("protocol violation: invalid array length: %d", respArrayLen)
	}

	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if code == msgpcode.Nil {
		err = d.DecodeNil()
		if err != nil {
			return err
		}

		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on error case", respArrayLen)
		}

		// The possible variations of error here are fully unknown yet for us, e.g:
		// vshard error, assert error or some other type of error. So this question requires research.
		// So we do not decode it to some known error format, because we don't use it anyway.
		decodedError, err := d.DecodeInterface()
		if err != nil {
			return err
		}

		// convert empty interface into error
		r.err = fmt.Errorf("%v", decodedError)

		return nil
	}

	r.bucketCount, err = d.DecodeUint64()
	if err != nil {
		return err
	}

	return nil
}

type replicasetFuture struct {
	// replicaset name
	name   string
	future *tarantool.Future
}

// RouterMapCallRW is a consistent Map-Reduce. The given function is called on all masters in the
// cluster with a guarantee that in case of success it was executed with all
// buckets being accessible for reads and writes.
// T is a return type of user defined function 'fnc'.
// We define it as a distinct function, not a Router method, because golang limitations,
// see: https://github.com/golang/go/issues/49085.
func RouterMapCallRW[T any](r *Router, ctx context.Context,
	fnc string, args interface{}, opts RouterMapCallRWOptions,
) (map[string]T, error) {
	const vshardStorageServiceCall = "vshard.storage._call"

	timeout := callTimeoutDefault
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	timeStart := time.Now()
	refID := r.refID.Add(1)

	nameToReplicasetRef := r.getNameToReplicaset()

	defer func() {
		// call function "storage_unref" if map_callrw is failed or successed
		storageUnrefReq := tarantool.NewCallRequest(vshardStorageServiceCall).
			Args([]interface{}{"storage_unref", refID})

		for _, rs := range nameToReplicasetRef {
			future := rs.conn.Do(storageUnrefReq, pool.RW)
			future.SetError(nil) // TODO: does it cancel the request above or not?
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// ref stage

	storageRefReq := tarantool.NewCallRequest(vshardStorageServiceCall).
		Context(ctx).
		Args([]interface{}{"storage_ref", refID, timeout})

	var rsFutures = make([]replicasetFuture, 0, len(nameToReplicasetRef))

	// ref stage: send concurrent ref requests
	for name, rs := range nameToReplicasetRef {
		rsFutures = append(rsFutures, replicasetFuture{
			name:   name,
			future: rs.conn.Do(storageRefReq, pool.RW),
		})
	}

	// ref stage: get their responses
	var totalBucketCount uint64
	// proto for 'storage_ref' method:
	// https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/vshard/storage/init.lua#L3137
	for _, rsFuture := range rsFutures {
		var storageRefResponse storageRefResponseProto

		if err := rsFuture.future.GetTyped(&storageRefResponse); err != nil {
			return nil, fmt.Errorf("rs {%s} storage_ref err: %v", rsFuture.name, err)
		}

		if storageRefResponse.err != nil {
			return nil, fmt.Errorf("storage_ref failed on %v: %v", rsFuture.name, storageRefResponse.err)
		}

		totalBucketCount += storageRefResponse.bucketCount
	}

	if totalBucketCount != r.cfg.TotalBucketCount {
		return nil, fmt.Errorf("total bucket count got %d, expected %d", totalBucketCount, r.cfg.TotalBucketCount)
	}

	// map stage

	storageMapReq := tarantool.NewCallRequest(vshardStorageServiceCall).
		Context(ctx).
		Args([]interface{}{"storage_map", refID, fnc, args})

	// reuse the same slice again
	rsFutures = rsFutures[0:0]

	// map stage: send concurrent map requests
	for name, rs := range nameToReplicasetRef {
		rsFutures = append(rsFutures, replicasetFuture{
			name:   name,
			future: rs.conn.Do(storageMapReq, pool.RW),
		})
	}

	// map stage: get their responses
	nameToResult := make(map[string]T)
	for _, rsFuture := range rsFutures {
		var storageMapResponse storageMapResponseProto[T]

		err := rsFuture.future.GetTyped(&storageMapResponse)
		if err != nil {
			return nil, fmt.Errorf("rs {%s} storage_map err: %v", rsFuture.name, err)
		}

		if !storageMapResponse.ok {
			return nil, fmt.Errorf("storage_map failed on %v: %+v", rsFuture.name, storageMapResponse.err)
		}

		nameToResult[rsFuture.name] = storageMapResponse.value
	}

	r.metrics().RequestDuration(time.Since(timeStart), true, true)

	return nameToResult, nil
}

// RouteAll return map of all replicasets.
func (r *Router) RouteAll() map[string]*Replicaset {
	nameToReplicasetRef := r.getNameToReplicaset()

	// Do not expose the original map to prevent unauthorized modification.
	nameToReplicasetCopy := make(map[string]*Replicaset)

	for k, v := range nameToReplicasetRef {
		nameToReplicasetCopy[k] = v
	}

	return nameToReplicasetCopy
}
