package util

import (
	"log"
	"time"

	"github.com/go-redis/redis"
)

var unlockScript = redis.NewScript(`
	local key = KEYS[1]
	local lockID = ARGV[1]
	if redis.call("exists", key) and redis.call("get", key) == lockID then
		redis.call("del", key)
		return 1
	else
		return 0
	end
`)

func NewRedisLock(cli *redis.Client, key string, lockID string, lockTimeMs uint) *RedisLock {
	if cli == nil {
		return nil
	}
	return &RedisLock{
		cli,
		key,
		lockID,
		lockTimeMs,
	}
}

type RedisLock struct {
	cli        *redis.Client
	key        string
	lockID     string
	lockTimeMs uint
}

func (rl *RedisLock) Lock() bool {
	res, err := rl.cli.SetNX(rl.key, rl.lockID, time.Millisecond*time.Duration(rl.lockTimeMs)).Result()
	if err != nil {
		log.Printf("acquire lock failed. (key = %s, lockID = %s), err=%s", rl.key, rl.lockID, err)
		return false
	}
	if !res {
		log.Printf("acquire lock failed. (key = %s, lockID = %s)", rl.key, rl.lockID)
	}
	// log.Printf("acquire lock success. (key = %s, lockID = %s)", rl.key, rl.lockID)
	return true

}

func (rl *RedisLock) UnLock() bool {
	res, err := unlockScript.Run(rl.cli, []string{rl.key}, rl.lockID).Result()
	if err != nil {
		log.Printf("release lock failed. (key = %s, lockID = %s), err=%s", rl.key, rl.lockID, err)
		return false
	}
	if res.(int64) != 1 {
		log.Printf("release lock failed. (key = %s, lockID = %s)", rl.key, rl.lockID)
		return false
	}
	return true
}
