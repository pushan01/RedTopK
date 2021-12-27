package topk

import "github.com/go-redis/redis"

var script = redis.NewScript(`

-- local metaKey = "{test_meta}"
-- local testKey = "{test_split}"
-- local score = 10.
-- local member = 10024
local metaKey = KEYS[1]
local cmd = ARGV[1]
local hashShardTotal = 499
local shardLimit = 4000

local lshift = bit.lshift
local rshift = bit.rshift
local bxor = bit.bxor

local byte = string.byte
local sub = string.sub
local len = string.len

local function JSHash(str)
    local l = len(str)
    local h = l
    local step = rshift(l, 5) + 1

    for i=l,step,-step do
        h = bxor(h, (lshift(h, 5) + byte(sub(str, i, i)) + rshift(h, 2)))
    end

    return h
end

local function getNewTargetKey(metaZSetCounterKey)
    local cnt = redis.call("incr", metaZSetCounterKey)
    return metaKey .. ":data_shard:" .. cnt
end

local function getMaximiumScore(zSetKey)
  local maxMember = redis.call("zrevrangebyscore", zSetKey, "inf", "-inf", "withscores", "limit", 0, 1)
  return maxMember[2]
end

local addMemberInChunks = function (key, members)
    local step = 1000
    for i = 0, #members - 1, step do
        redis.call("zadd", key, unpack(members, i, math.min(i + step - 1, #members - 1)))
    end
end

local function splitShard(metaKey, targetKey, shardLimit)
        local metaZSetCounterKey = metaKey .. ":shard_cnt"
        -- split. TODO 合并
        -- TODO. 极端情况，一个shard全是score相等的member
        local maxScore = getMaximiumScore(targetKey)
        local maxMemberScores = redis.call("zrangebyscore", targetKey, maxScore, maxScore, "withscores")
        local splitKey = ""
        local nextZsets = redis.call("zrangebyscore", metaKey, "("..maxScore, "inf", "limit", 0, 1)
        if #nextZsets > 0 then
          local nextShardCounter = redis.call("zcard", nextZsets[1])
          if nextShardCounter + #maxMemberScores / 2 <= shardLimit then
            splitKey = nextZsets[1]
          else
            splitKey = getNewTargetKey(metaZSetCounterKey)
          end
        else 
          splitKey = getNewTargetKey(metaZSetCounterKey)
        end
        maxMemberScores[0] = maxMemberScores[#maxMemberScores]
        -- local copyRes = redis.call("zadd", splitKey, unpack(maxMemberScores, 0, #maxMemberScores - 1))
        addMemberInChunks(splitKey, maxMemberScores)
        local removeRes = redis.call("zremrangebyscore", targetKey, maxScore, maxScore)
        local metaAddRes = redis.call("zadd", metaKey, getMaximiumScore(splitKey), splitKey)
        local shardCounter = redis.call("zcard", targetKey)
        for i = 0, #maxMemberScores - 1, 2 do
          local member = maxMemberScores[i + 1]
          local hashCodeOfMember = JSHash(member)
          local memberZSetKey = metaKey .. ":m_to_z:" .. (hashCodeOfMember % hashShardTotal)
          redis.call("hset", memberZSetKey, member, splitKey)
        end
        
        if shardCounter <= 0 then
          local metaRemRes = redis.call("zrem", metaKey, targetKey)
        else
          maxScore = getMaximiumScore(targetKey)
          metaAddRes = redis.call("zadd", metaKey, maxScore, targetKey)
        end
end

local function DelMember(metaKey, member, memberZsetKey)
    local zremRes = redis.call("zrem", memberZsetKey, member)
    local shardMemberCounter = redis.call("zcard", memberZsetKey)
    if shardMemberCounter <= 0 then
      local removeCounterRes =  redis.call("zrem", metaKey, memberZsetKey)
    else
      -- 更换最大值
      local maxScore = getMaximiumScore(memberZsetKey)
      local resetMaxRes = redis.call("zadd", metaKey, maxScore, memberZsetKey)
    end
    return 1
end

local function RemoveIfExists(metaKey, member)
  if member == "" then
    return
  end
  
  local hashCodeOfMember = JSHash(member)
  local memberToZsetKey = metaKey .. ":m_to_z:" .. (hashCodeOfMember % hashShardTotal)
  local targetZsetKey = redis.call("hget", memberToZsetKey, member)
  if targetZsetKey ~= false then
    DelMember(metaKey, member, targetZsetKey)
    local delMemberRes = redis.call("hdel", memberToZsetKey, member)
  end
end


local function AddMember(metaKey, score, member,  shardLimit)
    if member == "" then
      return
    end
    local hashCodeOfMember = JSHash(member)
    local memberToZsetKey = metaKey .. ":m_to_z:" .. (hashCodeOfMember % hashShardTotal)
    local metaZSetCounterKey = metaKey .. ":shard_cnt"
    local targetKey = ""
    
    -- 获取最大值大于score的第一个key
    local zSetToAdd = redis.call("zrangebyscore", metaKey,  score, "inf", "limit", 0, 1)
    if #zSetToAdd <= 0 then
      -- unTODO. 找不到放到最后一个shard, 而不是重新生成一个shard
      zSetToAdd = redis.call("zrevrangebyscore", metaKey, score, "-inf", "limit", 0, 1)
    end
    
    if #zSetToAdd <= 0 then
        targetKey = getNewTargetKey(metaZSetCounterKey)
    else
        targetKey = zSetToAdd[1]
    end
    local addRes = redis.call("zadd", targetKey, score, member)
    -- 添加到hash
    redis.call("hset", memberToZsetKey, member, targetKey)
    local shardCounter = redis.call("zcard", targetKey)
    if shardCounter > shardLimit then
      splitShard(metaKey, targetKey, shardLimit)
        -- split. TODO 合并
        -- TODO. 极端情况，一个shard全是score相等的member
        --[[
        local maxScore = getMaximiumScore(targetKey)
        local maxMemberScores = redis.call("zrangebyscore", targetKey, maxScore, maxScore, "withscores")
        local splitKey = getNewTargetKey(metaZSetCounterKey)
        maxMemberScores[0] = maxMemberScores[#maxMemberScores]
        local moveRes = redis.call("zadd", splitKey, unpack(maxMemberScores, 0, #maxMemberScores - 1))
        local metaAddRes = redis.call("zadd", metaKey, maxScore, splitKey)
        local removeRes = redis.call("zremrangebyscore", targetKey, maxScore, maxScore)
        shardCounter = redis.call("zcard", targetKey)
        if shardCounter <= 0 then
          local metaRemRes = redis.call("zrem", metaKey, targetKey)
        else
          maxScore = getMaximiumScore(targetKey)
          metaAddRes = redis.call("zadd", metaKey, maxScore, targetKey)
        end
        ]]
        -- TODO. 移除一部分非最大值数据到新的set
    else
      local maxScore = getMaximiumScore(targetKey)
      local addRes = redis.call("zadd", metaKey, maxScore, targetKey)
    end
    return 1
end

local function getTopKNoScore(metaKey, k)
  local cnt = 0
  local score = "-inf"
  local ans = ""
  while cnt < k do
    local nextZset = redis.call("zrangebyscore", metaKey, "("..score, "inf", "withscores", "limit", 0, 1)
    if #nextZset == 2 then
      score = nextZset[2]
      local l = redis.call("zrangebyscore", nextZset[1], "-inf", "inf", "limit", 0, k - cnt)
      if #l > 0 then
          cnt = cnt + #l
          ans = ans .. table.concat(l, ",") .. ","
      end
    
    else
      return ans
    end
    
  end
  return ans
end

local function getTopKWithScore(metaKey, k)
  local cnt = 0
  local score = "-inf"
  local ans = ""
  
  while cnt < k do
    local nextZset = redis.call("zrangebyscore", metaKey, "("..score, "inf", "withscores", "limit", 0, 1)
    if #nextZset == 2 then
      score = nextZset[2]
      local l = redis.call("zrangebyscore", nextZset[1], "-inf", "inf", "withscores", "limit", 0, k - cnt)
      if #l > 0 then
          cnt = cnt + #l / 2
          ans = ans .. table.concat(l, ",")..","
      end
    

    else
      return ans
    end
    
  end
  return ans
end
--[[
local ans = ""
for i = 0, 10000, 1 do
  AddMember(metaKey, i, i, 20)
  -- RemoveIfExists(metaKey, i)
  -- ans = ans .. AddMember(metaKey, i, i, 20)
end
ans = getTopKWithScore(metaKey, 101)
return ans
]]

-- return AddMember(metaKey, 998, 998, 20)

if cmd == "add" then 
  local score = ARGV[2]
  local member = ARGV[3]
  return AddMember(metaKey, score, member, shardLimit)
elseif cmd == "del" then
  local member = ARGV[2]
  return RemoveIfExists(metaKey, member)
elseif cmd == "topks" then
  local k = tonumber(ARGV[2])
  return getTopKWithScore(metaKey, k)
else
  local k = tonumber(ARGV[2])
  return getTopKNoScore(metaKey, k)
end
`)
