package topk

import (
	"fmt"
	"hash/adler32"
	"log"
	"math"
	"pushan/RedTopK/util"

	"github.com/go-basic/uuid"
	"github.com/go-redis/redis"
)

const (
	ShardLimit           = 4000
	LockTimeMs           = 10 * 1000
	HashShardCnt         = 499
	VersionLock          = "v1.0"
	MetaZSetLockTemplate = "topk_lock_meta::%s:%s"
)

func NewLockTopKProvider(cli *redis.Client) TopKProvider {
	if cli == nil {
		panic("invalid param: cli")
	}

	return zSetLockTopKProvider{cli: cli}
}

type zSetLockTopKProvider struct {
	cli *redis.Client
}

func (z zSetLockTopKProvider) init() error {
	return nil
}

func (z zSetLockTopKProvider) makeMetaKey(key string) string {
	return fmt.Sprintf(MetaZSetTemplate, key, Version)
}

func (z zSetLockTopKProvider) makeLockKey(key string) string {
	return z.makeMetaKey(key) + "::lock"
}

func (z zSetLockTopKProvider) allocShard(metaKey string) string {
	// 在持有锁的情况下执行, 如果出错，则直接panic
	shardCntKey := metaKey + ":shard_cnt"
	shardCnt, err := z.cli.Incr(shardCntKey).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	return fmt.Sprintf("%s:data_shard:%d", metaKey, shardCnt)
}

func (z zSetLockTopKProvider) getTargetShard(metaKey string, score float64) (targetShard string) {
	// 在持有锁的情况下执行, 如果出错，则直接panic
	// 获取最大值大于等于score的第一个zset
	rangeRes, err := z.cli.ZRangeByScore(metaKey, redis.ZRangeBy{
		Min:    fmt.Sprintf("%f", score),
		Max:    "inf",
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	if len(rangeRes) == 0 {
		// 不存在则放到最后一个shard
		rangeRes, err = z.cli.ZRevRangeByScore(metaKey, redis.ZRangeBy{
			Min:    "-inf",
			Max:    "inf",
			Offset: 0,
			Count:  1,
		}).Result()
	}
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	if len(rangeRes) == 0 {
		return z.allocShard(metaKey)
	}
	return rangeRes[0]
}

func (z zSetLockTopKProvider) getMaximiumScoreOfShard(shard string) float64 {
	scores, err := z.cli.ZRevRangeByScoreWithScores(shard, redis.ZRangeBy{
		Min:    "-inf",
		Max:    "inf",
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	if len(scores) <= 0 {
		panic(fmt.Errorf("no member in shard:%s", shard))
	}
	return scores[0].Score
}

func (z zSetLockTopKProvider) splitTrans(metaKey, srcShard, targetShard string, srcMax, tgtMax float64,
	transMembers []redis.Z, removeScoreMin, removeScoreMax string) {
	pl := z.cli.TxPipeline()
	pl.ZAdd(targetShard, transMembers...)
	pl.ZRemRangeByScore(srcShard, removeScoreMin, removeScoreMax)
	pl.ZAdd(metaKey, redis.Z{
		Score:  srcMax,
		Member: srcShard,
	},
		redis.Z{
			Score:  tgtMax,
			Member: targetShard,
		})
	for i := range transMembers {
		member := transMembers[i].Member.(string)
		pl.HSet(z.getExistsKey(metaKey, member), member, targetShard)
	}
	_, err := pl.Exec()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
}

func (z zSetLockTopKProvider) splitShard(targetShard, metakey string) {
	maxScoreOfTargetShard := z.getMaximiumScoreOfShard(targetShard)
	var splitShard string
	var splitMax float64 = -math.MaxFloat64
	maxScoreStr := fmt.Sprintf("%f", maxScoreOfTargetShard)
	maxAfterRemove, err := z.cli.ZRevRangeByScoreWithScores(targetShard, redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("(%s", maxScoreStr),
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	if len(maxAfterRemove) <= 0 {
		// empty after remove
		return
	}
	maxMemberScores, err := z.cli.ZRangeByScoreWithScores(targetShard, redis.ZRangeBy{
		Min: maxScoreStr,
		Max: maxScoreStr,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	nextZset, err := z.cli.ZRangeByScoreWithScores(metakey, redis.ZRangeBy{
		Min:    fmt.Sprintf("(%f", maxScoreOfTargetShard),
		Max:    "inf",
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	if len(nextZset) > 0 {
		splitShard = nextZset[0].Member.(string)
		nextMemberCnt, err := z.cli.ZCard(splitShard).Result()
		if err != nil {
			log.Printf("%s\n", err)
			panic(err)
		}
		if nextMemberCnt+int64(len(maxMemberScores)) > ShardLimit {
			splitShard = z.allocShard(metakey)
		} else {
			splitMax = nextZset[0].Score
		}
	} else {
		splitShard = z.allocShard(metakey)
	}
	splitMax = math.Max(splitMax, maxScoreOfTargetShard)
	z.splitTrans(metakey, targetShard, splitShard, maxAfterRemove[0].Score,
		splitMax, maxMemberScores, maxScoreStr, maxScoreStr)
}

func (z zSetLockTopKProvider) addElementToTargetShard(targetShard, metaKey, id string, score float64) {
	// 在持有锁的情况下执行, 如果出错，则直接panic
	var targetShardMaxScore float64 = score
	scores, err := z.cli.ZRevRangeByScoreWithScores(targetShard, redis.ZRangeBy{
		Min:    "-inf",
		Max:    "inf",
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	if len(scores) > 0 && targetShardMaxScore < scores[0].Score {
		targetShardMaxScore = scores[0].Score
	}
	pl := z.cli.Pipeline()
	_ = pl.ZAdd(targetShard, redis.Z{
		Score:  score,
		Member: id,
	})
	pl.ZAdd(metaKey, redis.Z{
		Score:  targetShardMaxScore,
		Member: targetShard,
	}).Err()
	pl.HSet(z.getExistsKey(metaKey, id), id, targetShard)

	shardMemberCmd := pl.ZCard(targetShard)
	_, err = pl.Exec()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	shardMemberCnt, err := shardMemberCmd.Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	// 判断是否需要分裂
	if shardMemberCnt > ShardLimit {
		// 分裂
		z.splitShard(targetShard, metaKey)
	}
}

func (z zSetLockTopKProvider) AddElement(key string, id string, score float64) (ansErr error) {
	ansErr = nil
	defer func() {
		if err := recover(); err != nil {
			ansErr = err.(error)
		}
	}()
	lockKey := z.makeLockKey(key)
	lock := util.NewRedisLock(z.cli, lockKey, uuid.New(), LockTimeMs)
	if lock == nil {
		return fmt.Errorf("create lock for (%s, %s, %f) failed", key, id, score)
	}
	// 为了测试，循环抢锁
	for !lock.Lock() {
	}
	/*
		if !lock.Lock() {
			return fmt.Errorf("acquire lock for (%s, %s, %f) failed", key, id, score)
		}
	*/
	defer lock.UnLock()

	metaKey := z.makeMetaKey(key)
	z.deleteMember(metaKey, id)
	targetShard := z.getTargetShard(metaKey, score)
	// 添加到targetshard
	z.addElementToTargetShard(targetShard, metaKey, id, score)
	return
}

func (z zSetLockTopKProvider) getExistsKey(metaKey string, id string) string {
	return fmt.Sprintf("%s:m_to_z:%d", metaKey, adler32.Checksum([]byte(id))%HashShardCnt)
}

func (z zSetLockTopKProvider) deleteMember(metaKey string, id string) {
	// 在持有锁的情况下执行, 如果出错，则直接panic
	hashKey := z.getExistsKey(metaKey, id)
	exists, err := z.cli.HExists(hashKey, id).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	if exists {
		targetZSet, err := z.cli.HGet(hashKey, id).Result()
		if err != nil {
			log.Printf("%s\n", err)
			panic(err)
		}
		top2, err := z.cli.ZRevRangeByScoreWithScores(targetZSet, redis.ZRangeBy{
			Min:    "-inf",
			Max:    "inf",
			Offset: 0,
			Count:  2,
		}).Result()
		if err != nil {
			log.Printf("%s\n", err)
			panic(err)
		}
		pl := z.cli.TxPipeline()
		// 所有的修改放到一个pipeline，防止部分失败
		pl.ZRem(targetZSet, id)
		pl.HDel(hashKey, id)
		if len(top2) <= 1 {
			// 只有唯一一个元素, 删除该zset
			pl.ZRem(metaKey, targetZSet)
		} else {
			// 多余1个元素
			if top2[0].Member == id {
				// 最大值为删除的元素
				pl.ZAdd(metaKey, redis.Z{
					Score:  top2[1].Score,
					Member: targetZSet,
				})
			} /* else {
				// 最大值删除后不会改变, Do nothing
			} */
		}
		_, err = pl.Exec()
		if err != nil {
			log.Printf("%s\n", err)
			panic(err)
		}
	}
}

func (z zSetLockTopKProvider) GetTopK(key string, k int) (ansErr error, ans []Element) {
	ansErr = nil
	defer func() {
		if err := recover(); err != nil {
			ansErr = err.(error)
		}
	}()
	lockKey := z.makeLockKey(key)
	lock := util.NewRedisLock(z.cli, lockKey, uuid.New(), LockTimeMs)
	if lock == nil {
		panic(fmt.Errorf("create lock for (%s) failed", key))
	}
	if !lock.Lock() {
		panic(fmt.Errorf("acquire lock for (%s) failed", key))
	}
	defer lock.UnLock()
	metaKey := z.makeMetaKey(key)
	metaZSets, err := z.cli.ZRangeByScore(metaKey, redis.ZRangeBy{
		Min:    "-inf",
		Max:    "inf",
		Offset: 0,
		Count:  0,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	ans = make([]Element, 0, k)
	for i := 0; i < len(metaZSets) && k > 0; i++ {
		members, err := z.cli.ZRangeByScore(metaZSets[i], redis.ZRangeBy{
			Min:    "-inf",
			Max:    "inf",
			Offset: 0,
			Count:  int64(k),
		}).Result()
		if err != nil {
			log.Printf("%s\n", err)
			panic(err)
		}
		k -= len(members)
		for j := range members {
			ele := Element{Id: members[j]}
			ans = append(ans, ele)
		}
	}
	return
}
func (z zSetLockTopKProvider) GetTopKS(key string, k int) (ansErr error, ans []Element) {
	ansErr = nil
	defer func() {
		if err := recover(); err != nil {
			ansErr = err.(error)
		}
	}()
	lockKey := z.makeLockKey(key)
	lock := util.NewRedisLock(z.cli, lockKey, uuid.New(), LockTimeMs)
	if lock == nil {
		panic(fmt.Errorf("create lock for (%s) failed", key))
	}
	if !lock.Lock() {
		panic(fmt.Errorf("acquire lock for (%s) failed", key))
	}
	defer lock.UnLock()
	metaKey := z.makeMetaKey(key)
	metaZSets, err := z.cli.ZRangeByScore(metaKey, redis.ZRangeBy{
		Min:    "-inf",
		Max:    "inf",
		Offset: 0,
		Count:  0,
	}).Result()
	if err != nil {
		log.Printf("%s\n", err)
		panic(err)
	}
	ans = make([]Element, 0, k)
	for i := 0; i < len(metaZSets) && k > 0; i++ {
		members, err := z.cli.ZRangeByScoreWithScores(metaZSets[i], redis.ZRangeBy{
			Min:    "-inf",
			Max:    "inf",
			Offset: 0,
			Count:  int64(k),
		}).Result()
		if err != nil {
			log.Printf("%s\n", err)
			panic(err)
		}
		k -= len(members)
		for j := range members {
			ele := Element{Id: members[j].Member.(string), Score: members[j].Score}
			ans = append(ans, ele)
		}
	}
	return
}

func (z zSetLockTopKProvider) DeleteElement(key string, id string) (ansErr error) {
	ansErr = nil
	defer func() {
		if err := recover(); err != nil {
			ansErr = err.(error)
		}
	}()
	lockKey := z.makeLockKey(key)
	lock := util.NewRedisLock(z.cli, lockKey, uuid.New(), LockTimeMs)
	if lock == nil {
		return fmt.Errorf("create lock for (%s, %s) failed", key, id)
	}
	if !lock.Lock() {
		return fmt.Errorf("acquire lock for (%s, %s) failed", key, id)
	}
	defer lock.UnLock()
	z.deleteMember(z.makeMetaKey(key), id)
	return
}
