#!/usr/bin/env tarantool

require('strict').on()

local uuid = require('uuid')
local vshard = require 'vshard'


function echo(...)
    return ...
end