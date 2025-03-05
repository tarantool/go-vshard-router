#!/usr/bin/env tarantool

require('strict').on()

local uuid = require('uuid')
local vshard = require 'vshard'
local yaml = require('yaml')
local fio = require('fio')
local errno = require('errno')

-- METRICS
local metrics = require('metrics')
local http_server = require('http.server')

-- Define helper functions
local http_metrics_handler = require('metrics.plugins.prometheus').collect_http


-- CONFIGURATION
function read_file(path)
    local file = fio.open(path)
    if file == nil then
        return nil, string.format('Failed to open file %s: %s', path, errno.strerror())
    end
    local buf = {}
    while true do
        local val = file:read(1024)
        if val == nil then
            return nil, string.format('Failed to read from file %s: %s', path, errno.strerror())
        elseif val == '' then
            break
        end
        table.insert(buf, val)
    end
    file:close()
    return table.concat(buf, '')
end



local function parse_yaml(data)
    local sections = {'default'}

    if app_name ~= nil then
        table.insert(sections, app_name)

        if instance_name ~= nil then
            table.insert(sections, app_name .. '.' .. instance_name)
        end
    end

    local ok, cfg = pcall(yaml.decode, data)
    if not ok then
        return nil, cfg
    end

    local res = {}

    for _, section in ipairs(sections) do
        for argname, argvalue in pairs(cfg[section] or {}) do
            res[argname] = argvalue
        end
    end

    return res
end

-- Get instance name
local fiber = require('fiber')
local NAME = os.getenv("TNT_NAME")
local CONFIG_FILE = os.getenv("CONFIG_FILE")



local d = yaml.decode(read_file(CONFIG_FILE))
local storages = d.storages

-- Call a configuration provider
local cfg = {
     sharding = {
          ['cbf06940-0790-498b-948d-042b62cf3d29'] = { -- replicaset #1
              replicas = {
                  ['8a274925-a26d-47fc-9e1b-af88ce939412'] = {
                      uri = 'storage:storage@' .. storages["storage_1_a"],
                      name = 'storage_1_a',
                      master = true,
                  },
                  ['3de2e3e1-9ebe-4d0d-abb1-26d301b84633'] = {
                      uri = 'storage:storage@' .. storages["storage_1_b"],
                      name = 'storage_1_b'
                  },
              },
          }, -- replicaset #1
          ['ac522f65-aa94-4134-9f64-51ee384f1a54'] = { -- replicaset #2
              replicas = {
                 ['1e02ae8a-afc0-4e91-ba34-843a356b8ed7'] = {
                     uri = 'storage:storage@' .. storages["storage_2_a"],
                     name = 'storage_2_a',
                     master = true,
                 },
                 ['001688c3-66f8-4a31-8e19-036c17d489c2'] = {
                      uri = 'storage:storage@' .. storages["storage_2_b"],
                      name = 'storage_2_b'
                 }
              },
          }, -- replicaset #2
     }, -- sharding
     replication_connect_quorum = 0, -- its oke if we havent some replicas
}


-- Name to uuid map
local names = {
    ['storage_1_a'] = '8a274925-a26d-47fc-9e1b-af88ce939412',
    ['storage_1_b'] = '3de2e3e1-9ebe-4d0d-abb1-26d301b84633',
    ['storage_1_c'] = '3de2e3e1-9ebe-4d0d-abb1-26d301b84635',
    ['storage_2_a'] = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    ['storage_2_b'] = '001688c3-66f8-4a31-8e19-036c17d489c2',
}

replicasets = {'cbf06940-0790-498b-948d-042b62cf3d29',
               'ac522f65-aa94-4134-9f64-51ee384f1a54'}



rawset(_G, 'vshard', vshard) -- set as global variable

-- Если мы являемся роутером, то применяем другой конфиг
if NAME == "router" then
    cfg.listen = "0.0.0.0:12000"
    cfg['bucket_count'] = 100
    local router = vshard.router.new('router', cfg)
    rawset(_G, 'router', router) -- set as global variable

    local api = {}
    rawset(_G, 'api', api)

    router:bootstrap({timeout = 4, if_not_bootstrapped = true})

    function api.echo(id, ...)
        local bucket_id = router:bucket_id_strcrc32(id)


        return router:call(bucket_id, 'read', 'echo', {...})
    end

else
    vshard.storage.cfg(cfg, names[NAME])

    function echo(...)
        return ...
    end
end

box.once('access:v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

-- Configure the metrics module
metrics.cfg{labels = {alias = 'my-tnt-app'}}

-- Run the web server
local server = http_server.new('0.0.0.0', 8081)
server:route({path = '/metrics'}, http_metrics_handler)
server:start()