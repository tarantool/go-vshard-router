#!/usr/bin/env tarantool

-- Storages of a separate vshard cluster that nobody bootstraps. The
-- integration tests use it to check Router.ClusterBootstrap on an empty
-- cluster, since the main cluster from config.lua is bootstrapped at start.

local vshard = require('vshard')

local name = os.getenv('TEST_TNT_WORK_DIR'):match('[^/]+$')

local uuids = {
    bootstrap_1_a = 'b0000000-0001-0001-0000-000000000000',
    bootstrap_2_a = 'b0000000-0002-0001-0000-000000000000',
}

local cfg = {
    -- Must match TotalBucketCount in the test: the rebalancer relies on it.
    bucket_count = 10,
    sharding = {
        ['b0000000-0001-0000-0000-000000000000'] = {
            replicas = {
                [uuids.bootstrap_1_a] = {
                    uri = 'storage:storage@127.0.0.1:3311',
                    name = 'bootstrap_1_a',
                    master = true,
                },
            },
        },
        ['b0000000-0002-0000-0000-000000000000'] = {
            replicas = {
                [uuids.bootstrap_2_a] = {
                    uri = 'storage:storage@127.0.0.1:3312',
                    name = 'bootstrap_2_a',
                    master = true,
                },
            },
        },
    },
}

rawset(_G, 'vshard', vshard)
vshard.storage.cfg(cfg, uuids[name])

box.once('access:v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)
