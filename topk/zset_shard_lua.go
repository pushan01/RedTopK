package topk

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

type Element struct {
	Id    string
	Score float64
}

type TopKProvider interface {
	AddElement(key string, id string, score float64) error
	GetTopK(key string, k int) (error, []Element)
	GetTopKS(key string, k int) (error, []Element)
	DeleteElement(key string, id string) error
}

const (
	Version          = "v1.0"
	MetaZSetTemplate = "{topk_meta::%s}:%s"
)

func NewTopKProvider(cli *redis.Client) TopKProvider {
	if cli == nil {
		panic("invalid param: cli")
	}

	return zSetShardTopKProvider{cli: cli}
}

type zSetShardTopKProvider struct {
	cli *redis.Client
}

func (z zSetShardTopKProvider) init() error {
	return nil
}

func (z zSetShardTopKProvider) makeMetaKey(key string) string {
	return fmt.Sprintf(MetaZSetTemplate, key, Version)
}

func (z zSetShardTopKProvider) AddElement(key string, id string, score float64) error {
	cmd := script.Run(z.cli, []string{z.makeMetaKey(key)}, "add", score, id)
	_, err := cmd.Result()
	return err
}

func (z zSetShardTopKProvider) GetTopK(key string, k int) (error, []Element) {
	cmd := script.Run(z.cli, []string{z.makeMetaKey(key)}, "topk", k)
	res, err := cmd.Result()
	if err != nil {
		return err, nil
	}
	resStr := res.(string)
	resArr := strings.Split(resStr, ",")
	k = len(resArr) - 1
	ans := make([]Element, k)
	for i := 0; i < k; i++ {
		ans[i].Id = resArr[i]
	}
	return nil, ans
}

func (z zSetShardTopKProvider) GetTopKS(key string, k int) (error, []Element) {
	cmd := script.Run(z.cli, []string{z.makeMetaKey(key)}, "topks", k)
	res, err := cmd.Result()
	if err != nil {
		return err, nil
	}
	resStr := res.(string)
	resArr := strings.Split(resStr, ",")
	k = (len(resArr) - 1) >> 1
	ans := make([]Element, k)
	for i := 0; i < k; i++ {
		ans[i].Id = resArr[i<<1]
		ans[i].Score, _ = strconv.ParseFloat(resArr[(i<<1)+1], 64)
	}
	return nil, ans
}

func (z zSetShardTopKProvider) DeleteElement(key string, id string) error {
	cmd := script.Run(z.cli, []string{z.makeMetaKey(key)}, "del", id)
	if cmd.Err() != nil && strings.Contains(cmd.Err().Error(), "redis: nil") {
		return nil
	}
	return cmd.Err()
}
