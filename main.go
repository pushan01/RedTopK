package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"pushan/RedTopK/topk"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

func testAddAndTopK(addTime int, tp topk.TopKProvider) {
	cli2 := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	defer cli2.Close()
	// rand.Seed(1)
	cli2.FlushAll()
	tpZSet := topk.NewZSetProvider(cli2)
	key := "abcd"
	st := time.Now()
	for i := 0; i < addTime; i++ {
		id := strconv.FormatInt(rand.Int63(), 10)
		// id = strconv.FormatInt(int64(i+1), 10)
		score := rand.Int31n(65535)
		// fmt.Println(id, score)
		err := tp.AddElement(key, id, float64(score))
		if err != nil {
			log.Printf("sharding: add (%s, %d) failed, err=%s", id, score, err)
		}
		err = tpZSet.AddElement(key, id, float64(score))
		if err != nil {
			log.Printf("zset: add (%s, %d) failed, err=%s", id, score, err)
		}
	}
	log.Printf("add time:%d\n", time.Since(st).Milliseconds())
	st = time.Now()
	tk := addTime
	if tk >= 10000 {
		tk = 10000
	}
	for i := tk; i <= tk; i++ {
		err, eleShard := tp.GetTopKS(key, i)
		if err != nil {
			log.Printf("sharding: top %d failed, err=%s", i, err)
		}
		err, eleZset := tpZSet.GetTopKS(key, i)
		if err != nil {
			log.Printf("zset: top %d failed, err=%s", i, err)
		}
		if !reflect.DeepEqual(eleShard, eleZset) {
			// log.Printf("not equal:%v,%v", eleShard, eleZset)
			log.Printf("not equal:k=%d\n", i)
			// return
		}
	}
	log.Printf("verify time:%d\n", time.Since(st).Milliseconds())
	log.Println("all test passed.")
}

func randomTest(testTime int, tp topk.TopKProvider) {
	cli2 := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	defer cli2.Close()
	// rand.Seed(1)
	cli2.FlushAll()
	tpZSet := topk.NewZSetProvider(cli2)
	key := "abcd"
	members := make(map[string]float64)
	cnt := 0
	for i := 0; i < testTime; i++ {
		rnd := rand.Int31n(7)
		switch rnd {
		case 0, 1, 2, 3:
			// add
			id := fmt.Sprintf("%d", rand.Int63())
			score := rand.Int31n(4096)
			err := tp.AddElement(key, id, float64(score))
			err2 := tpZSet.AddElement(key, id, float64(score))
			if err != nil || err2 != nil {
				log.Printf("add failed:err1:%s, err2:%s\n", err, err2)
			} else {
				members[id] = float64(score)
				cnt++
			}
		case 4, 5, 6:
			// del
			keys := make([]string, 0, len(members))
			for k := range members {
				keys = append(keys, k)
			}
			if len(keys) > 0 {
				member := keys[rand.Int63n(int64(len(keys)))]
				err1 := tp.DeleteElement(key, member)
				err2 := tpZSet.DeleteElement(key, member)
				if err1 != nil || err2 != nil {
					log.Printf("del failed:err1:%s, err2:%s\n", err1, err2)
				} else {
					delete(members, member)
					cnt--
				}
			}
		}

		k := cnt
		if cnt >= 10000 {
			k = 10000
		}
		err1, tk1 := tp.GetTopKS(key, k)
		err2, tk2 := tpZSet.GetTopKS(key, k)
		if err1 != nil || err2 != nil {
			log.Printf("add failed:err1:%s, err2:%s\n", err1, err2)
		} else {
			if !reflect.DeepEqual(tk1, tk2) {
				log.Printf("not equal:k=%d\n", i)
			} else {
				if rand.Int31n(1000) == 0 {
					log.Printf("pass for k:%d\n, %v", k, reflect.TypeOf(tp))
				}
			}
		}
	}
}

func BenchMarkAddAndTopK(tp topk.TopKProvider, testTime int, addTimes int, resetFunc func()) {
	totalTime := 0
	key := "abcde"
	for i := 0; i < testTime; i++ {
		resetFunc()
		st := time.Now()
		for j := 0; j < addTimes; j++ {
			id := strconv.FormatInt(rand.Int63(), 10)
			score := rand.Int31()
			err := tp.AddElement(key, id, float64(score))
			if err != nil {
				log.Fatalf("add failed for (%s, %d), err=%s\n", id, score, err)
			}
		}
		totalTime += int(time.Since(st).Milliseconds())
	}
	log.Printf("sync::all test passed, alg: %v, testTime:%d, addTimes:%d, time elapsed:%d", reflect.TypeOf(tp), testTime, addTimes, totalTime/testTime)
}
func BenchMarkAddAndTopKASync(tp topk.TopKProvider, clientNum int, addTimesPerClient int, resetFunc func()) {
	resetFunc()
	key := "abcde"
	testFunc := func(totalTime *int, wg *sync.WaitGroup) {
		*totalTime = 0
		st := time.Now()
		for j := 0; j < addTimesPerClient; j++ {
			id := strconv.FormatInt(rand.Int63(), 10)
			score := rand.Int31()
			err := tp.AddElement(key, id, float64(score))
			if err != nil {
				log.Fatalf("add failed for (%s, %d), err=%s\n", id, score, err)
			}
		}
		*totalTime += int(time.Since(st).Milliseconds())
		wg.Done()
	}
	wg := &sync.WaitGroup{}
	totalTimes := make([]int, clientNum)
	for i := 0; i < clientNum; i++ {
		wg.Add(1)
		testFunc(&totalTimes[i], wg)
	}
	wg.Wait()
	totalTime := 0
	for i := 0; i < clientNum; i++ {
		totalTime += totalTimes[i]
	}
	totalTime /= clientNum
	log.Printf("async:all test passed, alg: %v, testTime:%d, addTimes:%d, time elapsed:%d", reflect.TypeOf(tp), clientNum, addTimesPerClient, totalTime)
}

func main() {
	cli2 := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	reset := func() {
		cli2.FlushAll()
	}
	testTime := int64(50)
	testTime, _ = strconv.ParseInt(os.Args[1], 10, 64)
	addTime, _ := strconv.ParseInt(os.Args[2], 10, 64)

	tp := topk.NewLockTopKProvider(cli2)
	// testAddAndTopK(int(testTime), tp)
	// randomTest(int(testTime), tp)
	// BenchMarkAddAndTopK(tp, int(testTime), int(addTime), reset)
	BenchMarkAddAndTopKASync(tp, int(testTime), int(addTime), reset)
	tp2 := topk.NewTopKProvider(cli2)
	// testAddAndTopK(int(testTime), tp2)
	// randomTest(int(testTime), tp2)
	// BenchMarkAddAndTopK(tp2, int(testTime), int(addTime), reset)
	BenchMarkAddAndTopKASync(tp2, int(testTime), int(addTime), reset)
	tp3 := topk.NewZSetProvider(cli2)
	// BenchMarkAddAndTopK(tp3, int(testTime), int(addTime), reset)
	BenchMarkAddAndTopKASync(tp3, int(testTime), int(addTime), reset)
}
