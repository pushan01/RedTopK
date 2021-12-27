package topk

import "github.com/go-redis/redis"

type ZSetTopKProvider struct {
	cli *redis.Client
}

func NewZSetProvider(cli *redis.Client) TopKProvider {
	if cli == nil {
		panic("invalid param: cli")
	}
	return ZSetTopKProvider{
		cli: cli,
	}
}

func (z ZSetTopKProvider) AddElement(key string, id string, score float64) error {
	return z.cli.ZAdd(key, redis.Z{
		Score:  score,
		Member: id,
	}).Err()
}

func (z ZSetTopKProvider) DeleteElement(key string, id string) error {
	return z.cli.ZRem(key, id).Err()
}
func (z ZSetTopKProvider) GetTopK(key string, k int) (error, []Element) {
	if k <= 0 {
		return nil, make([]Element, 0)
	}
	res, err := z.cli.ZRangeByScore(key, redis.ZRangeBy{
		Min:    "-inf",
		Max:    "inf",
		Offset: 0,
		Count:  int64(k),
	}).Result()
	if err != nil {
		return err, nil
	}
	ans := make([]Element, len(res))
	for i := range res {
		ans[i].Id = res[i]
	}
	return nil, ans
}
func (z ZSetTopKProvider) GetTopKS(key string, k int) (error, []Element) {
	if k <= 0 {
		return nil, make([]Element, 0)
	}
	res, err := z.cli.ZRangeByScoreWithScores(key, redis.ZRangeBy{
		Min:    "-inf",
		Max:    "inf",
		Offset: 0,
		Count:  int64(k),
	}).Result()
	if err != nil {
		return err, nil
	}
	ans := make([]Element, len(res))
	for i := range res {
		ans[i].Id = res[i].Member.(string)
		ans[i].Score = res[i].Score
	}
	return nil, ans
}
