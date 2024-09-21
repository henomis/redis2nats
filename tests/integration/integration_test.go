package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	redisnats "github.com/henomis/redis2nats"
	"github.com/stretchr/testify/suite"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type IntegrationTestSuite struct {
	suite.Suite
	redisClient      *redis.Client
	redis2natsClient *redis.Client
	redisServer      *redisnats.RedisServer
}

func (suite *IntegrationTestSuite) SetupTest() {
	compose, err := tc.NewDockerCompose("docker-compose.yml")
	suite.NoError(err, "NewDockerComposeAPI()")

	suite.T().Cleanup(func() {
		suite.NoError(compose.Down(context.Background(), tc.RemoveOrphans(true), tc.RemoveImagesLocal), "compose.Down()")
	})

	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	suite.NoError(compose.Up(ctx, tc.Wait(true)), "compose.Up()")

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "0.0.0.0:6379", // Redis server address (localhost and default Redis port)
		Password: "",             // No password set
		DB:       0,              // Use default DB
	})

	suite.T().Cleanup(func() {
		redisClient.Close()
	})

	// Create and start the fake Redis server using environment variable for Redis URL
	redisServer := redisnats.NewRedisServer(
		&redisnats.Config{
			NATSURL:          "nats://0.0.0.0:4222",
			NATSTimeout:      10 * time.Second,
			NATSBucketPrefix: "test",
			NATSPersist:      false,
			RedisAddress:     ":6400",
			RedisNumDB:       16,
		},
	)

	go func() {
		err = redisServer.Start(ctx)
		if err != nil {
			suite.T().Log(err)
		}
	}()

	suite.T().Cleanup(func() {
		redisServer.Stop()
	})

	redis2natsClient := redis.NewClient(&redis.Options{
		Addr:     "0.0.0.0:6400", // Redis server address (localhost and default Redis port)
		Password: "",             // No password set
		DB:       0,              // Use default DB
	})

	suite.T().Cleanup(func() {
		redis2natsClient.Close()
	})

	suite.redisClient = redisClient
	suite.redisServer = redisServer
	suite.redis2natsClient = redis2natsClient
}

func (suite *IntegrationTestSuite) TestPing() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test PING
	setRedisResult, err := suite.redisClient.Ping(ctx).Result()
	suite.NoError(err)

	setRedis2natsResult, err := suite.redis2natsClient.Ping(ctx).Result()
	suite.NoError(err)

	suite.Equal(setRedisResult, setRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestSet() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test SET
	setRedisResult, err := suite.redisClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	setRedis2natsResult, err := suite.redis2natsClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	suite.Equal(setRedisResult, setRedis2natsResult)

	// Test SET with different value
	setRedisResult, err = suite.redisClient.Set(ctx, "key", "value1", 0).Result()
	suite.NoError(err)

	setRedis2natsResult, err = suite.redis2natsClient.Set(ctx, "key", "value1", 0).Result()
	suite.NoError(err)

	suite.Equal(setRedisResult, setRedis2natsResult)

	// Test SET with same value
	setRedisResult, err = suite.redisClient.Set(ctx, "key", "value1", 0).Result()
	suite.NoError(err)

	setRedis2natsResult, err = suite.redis2natsClient.Set(ctx, "key", "value1", 0).Result()
	suite.NoError(err)

	suite.Equal(setRedisResult, setRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestSetXX() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test SET with XX not exists
	setxxRedisResult, err := suite.redisClient.SetXX(ctx, "key1", "value1", 0).Result()
	suite.NoError(err)

	setxxRedis2natsResult, err := suite.redis2natsClient.SetXX(ctx, "key1", "value1", 0).Result()
	suite.NoError(err)

	suite.Equal(setxxRedisResult, setxxRedis2natsResult)

	// Test SET with XX  exists
	setxxRedisResult, err = suite.redisClient.SetXX(ctx, "key", "value1", 0).Result()
	suite.NoError(err)

	setxxRedis2natsResult, err = suite.redis2natsClient.SetXX(ctx, "key", "value1", 0).Result()
	suite.NoError(err)

	suite.Equal(setxxRedisResult, setxxRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestSetNX() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)
	// Test SET with NX not exists
	setnxRedisResult, err := suite.redisClient.SetNX(ctx, "key1", "value1", 0).Result()
	suite.NoError(err)

	setnxRedis2natsResult, err := suite.redis2natsClient.SetNX(ctx, "key1", "value1", 0).Result()
	suite.NoError(err)

	suite.Equal(setnxRedisResult, setnxRedis2natsResult)

	_, err = suite.redisClient.Set(ctx, "key2", "value2", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key2", "value2", 0).Result()
	suite.NoError(err)

	// Test SET with NX  exists
	setnxRedisResult, err = suite.redisClient.SetNX(ctx, "key2", "value1", 0).Result()
	suite.NoError(err)

	setnxRedis2natsResult, err = suite.redis2natsClient.SetNX(ctx, "key2", "value1", 0).Result()
	suite.NoError(err)

	suite.Equal(setnxRedisResult, setnxRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestSetEX() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test SET
	setRedisResult, err := suite.redisClient.Set(ctx, "key", "value", 2*time.Second).Result()
	suite.NoError(err)

	setRedis2natsResult, err := suite.redis2natsClient.Set(ctx, "key", "value", 2*time.Second).Result()
	suite.NoError(err)

	suite.Equal(setRedisResult, setRedis2natsResult)

	<-time.After(3 * time.Second)

	// Test SET with different value
	setRedisResult, err = suite.redisClient.Get(ctx, "key").Result()
	suite.Error(err)

	setRedis2natsResult, err = suite.redis2natsClient.Get(ctx, "key").Result()
	suite.Error(err)

	suite.Equal(setRedisResult, setRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestTTL() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test SET
	setRedisResult, err := suite.redisClient.Set(ctx, "key", "value", 3*time.Second).Result()
	suite.NoError(err)

	setRedis2natsResult, err := suite.redis2natsClient.Set(ctx, "key", "value", 3*time.Second).Result()
	suite.NoError(err)

	suite.Equal(setRedisResult, setRedis2natsResult)

	<-time.After(2 * time.Second)

	// Test TTL
	ttlRedisResult, err := suite.redisClient.TTL(ctx, "key").Result()
	suite.NoError(err)

	ttlRedis2natsResult, err := suite.redis2natsClient.TTL(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(ttlRedisResult, ttlRedis2natsResult)

	<-time.After(2 * time.Second)

	// Test TTL expired
	ttlRedisResult, err = suite.redisClient.TTL(ctx, "key").Result()
	suite.NoError(err)

	ttlRedis2natsResult, err = suite.redis2natsClient.TTL(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(ttlRedisResult, ttlRedis2natsResult)

	// Test TTL not exists
	ttlRedisResult, err = suite.redisClient.TTL(ctx, "key2").Result()
	suite.NoError(err)

	ttlRedis2natsResult, err = suite.redis2natsClient.TTL(ctx, "key2").Result()
	suite.NoError(err)

	suite.Equal(ttlRedisResult, ttlRedis2natsResult)

	// Test SET
	_, err = suite.redisClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	// Test TTL not expired
	ttlRedisResult, err = suite.redisClient.TTL(ctx, "key").Result()
	suite.NoError(err)

	ttlRedis2natsResult, err = suite.redis2natsClient.TTL(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(ttlRedisResult, ttlRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestExpire() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test SET
	_, err := suite.redisClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	// Text EXPIRE

	expireRedisResult, err := suite.redisClient.Expire(ctx, "key", 3*time.Second).Result()
	suite.NoError(err)

	expireRedis2natsResult, err := suite.redis2natsClient.Expire(ctx, "key", 3*time.Second).Result()
	suite.NoError(err)

	suite.Equal(expireRedisResult, expireRedis2natsResult)

	// Test EXPIRE not exists
	expireRedisResult, err = suite.redisClient.Expire(ctx, "key2", 3*time.Second).Result()
	suite.NoError(err)

	expireRedis2natsResult, err = suite.redis2natsClient.Expire(ctx, "key2", 3*time.Second).Result()
	suite.NoError(err)

	suite.Equal(expireRedisResult, expireRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestGet() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test GET empty
	getRedisResult, err := suite.redisClient.Get(ctx, "key").Result()
	suite.Error(err)

	getRedis2natsResult, err := suite.redis2natsClient.Get(ctx, "key").Result()
	suite.Error(err)

	suite.Equal(getRedisResult, getRedis2natsResult)

	// insert data
	_, err = suite.redisClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	// Test GET
	getRedisResult, err = suite.redisClient.Get(ctx, "key").Result()
	suite.NoError(err)

	getRedis2natsResult, err = suite.redis2natsClient.Get(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(getRedisResult, getRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestKeys() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test KEYS empty
	keysRedisResult, err := suite.redisClient.Keys(ctx, "*").Result()
	suite.NoError(err)

	keysRedis2natsResult, err := suite.redis2natsClient.Keys(ctx, "*").Result()
	suite.NoError(err)

	suite.Equal(keysRedisResult, keysRedis2natsResult)

	// insert data
	_, err = suite.redisClient.Set(ctx, "hello", "value", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "hello", "value", 0).Result()
	suite.NoError(err)

	keysPatterns := []string{"*", "h?llo", "h*llo", "h[ae]llo", "h[^e]llo", "h[a-b]llo"}

	for _, pattern := range keysPatterns {
		keysRedisResult, err = suite.redisClient.Keys(ctx, pattern).Result()
		suite.NoError(err)

		keysRedis2natsResult, err = suite.redis2natsClient.Keys(ctx, pattern).Result()
		suite.NoError(err)

		suite.Equal(keysRedisResult, keysRedis2natsResult)
	}
}

func (suite *IntegrationTestSuite) TestMSet() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test MSET
	msetRedisResult, err := suite.redisClient.MSet(ctx, "key1", "value1", "key2", "value2").Result()
	suite.NoError(err)

	msetRedis2natsResult, err := suite.redisClient.MSet(ctx, "key1", "value1", "key2", "value2").Result()
	suite.NoError(err)

	suite.Equal(msetRedisResult, msetRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestMGet() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test MGET empty
	mgetRedisResult, err := suite.redisClient.MGet(ctx, "key1", "key2").Result()
	suite.NoError(err)

	mgetRedis2natsResult, err := suite.redis2natsClient.MGet(ctx, "key1", "key2").Result()
	suite.NoError(err)

	suite.Equal(mgetRedisResult, mgetRedis2natsResult)

	// setting value
	_, err = suite.redisClient.MSet(ctx, "key1", "value1", "key2", "value2").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.MSet(ctx, "key1", "value1", "key2", "value2").Result()
	suite.NoError(err)

	// Test MGET
	mgetRedisResult, err = suite.redisClient.MGet(ctx, "key1", "key2").Result()
	suite.NoError(err)

	mgetRedis2natsResult, err = suite.redis2natsClient.MGet(ctx, "key1", "key2").Result()
	suite.NoError(err)

	suite.Equal(mgetRedisResult, mgetRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestIncr() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// insert data
	_, err := suite.redisClient.Set(ctx, "key", "0", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key", "0", 0).Result()
	suite.NoError(err)

	var wgRedis sync.WaitGroup
	var wgRedis2nats sync.WaitGroup
	iterations := 1000

	for i := 0; i < iterations; i++ {
		wgRedis.Add(1)

		go func() {
			defer wgRedis.Done()
			// Test INCR
			_, errIncr := suite.redisClient.Incr(ctx, "key").Result()
			suite.NoError(errIncr)
		}()
	}

	for i := 0; i < iterations; i++ {
		wgRedis2nats.Add(1)

		go func() {
			defer wgRedis2nats.Done()
			// Test INCR
			_, errIncr := suite.redis2natsClient.Incr(ctx, "key").Result()
			suite.NoError(errIncr)
		}()
	}

	wgRedis.Wait()
	wgRedis2nats.Wait()

	// get value
	getRedisResult, err := suite.redisClient.Get(ctx, "key").Result()
	suite.NoError(err)

	getRedis2natsResult, err := suite.redis2natsClient.Get(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(getRedisResult, getRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestDecr() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// insert data
	_, err := suite.redisClient.Set(ctx, "key", "5000", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key", "5000", 0).Result()
	suite.NoError(err)

	var wgRedis sync.WaitGroup
	var wgRedis2nats sync.WaitGroup
	iterations := 1000

	for i := 0; i < iterations; i++ {
		wgRedis.Add(1)

		go func() {
			defer wgRedis.Done()
			// Test INCR
			_, errDecr := suite.redisClient.Decr(ctx, "key").Result()
			suite.NoError(errDecr)
		}()
	}

	for i := 0; i < iterations; i++ {
		wgRedis2nats.Add(1)

		go func() {
			defer wgRedis2nats.Done()
			// Test INCR
			_, errDecr := suite.redis2natsClient.Decr(ctx, "key").Result()
			suite.NoError(errDecr)
		}()
	}

	wgRedis.Wait()
	wgRedis2nats.Wait()

	// get value
	getRedisResult, err := suite.redisClient.Get(ctx, "key").Result()
	suite.NoError(err)

	getRedis2natsResult, err := suite.redis2natsClient.Get(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(getRedisResult, getRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestExists() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test NOT EXISTS
	existsRedisResult, err := suite.redisClient.Exists(ctx, "key").Result()
	suite.NoError(err)

	existsRedis2natsResult, err := suite.redis2natsClient.Exists(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(existsRedisResult, existsRedis2natsResult)

	// insert data
	_, err = suite.redisClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	// Test EXISTS
	existsRedisResult, err = suite.redisClient.Exists(ctx, "key").Result()
	suite.NoError(err)

	existsRedis2natsResult, err = suite.redis2natsClient.Exists(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(existsRedisResult, existsRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestDel() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test DEL
	delRedisResult, err := suite.redisClient.Del(ctx, "key").Result()
	suite.NoError(err)

	delRedis2natsResult, err := suite.redis2natsClient.Del(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(delRedisResult, delRedis2natsResult)

	// insert data
	_, err = suite.redisClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.Set(ctx, "key", "value", 0).Result()
	suite.NoError(err)

	// Test DEL
	delRedisResult, err = suite.redisClient.Del(ctx, "key").Result()
	suite.NoError(err)

	delRedis2natsResult, err = suite.redis2natsClient.Del(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(delRedisResult, delRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestHSet() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test HSET
	hsetRedisResult, err := suite.redisClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	hsetRedis2natsResult, err := suite.redis2natsClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	suite.Equal(hsetRedisResult, hsetRedis2natsResult)

	// Test HSET with different value
	hsetRedisResult, err = suite.redisClient.HSet(ctx, "key", "field2", "value2").Result()
	suite.NoError(err)

	hsetRedis2natsResult, err = suite.redis2natsClient.HSet(ctx, "key", "field2", "value2").Result()
	suite.NoError(err)

	suite.Equal(hsetRedisResult, hsetRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestHGet() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test HGET empty
	hgetRedisResult, err := suite.redisClient.HGet(ctx, "key", "field1").Result()
	suite.Error(err)

	hgetRedis2natsResult, err := suite.redis2natsClient.HGet(ctx, "key", "field1").Result()
	suite.Error(err)

	suite.Equal(hgetRedisResult, hgetRedis2natsResult)

	// insert data
	_, err = suite.redisClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	// Test HGET
	hgetRedisResult, err = suite.redisClient.HGet(ctx, "key", "field1").Result()
	suite.NoError(err)

	hgetRedis2natsResult, err = suite.redis2natsClient.HGet(ctx, "key", "field1").Result()
	suite.NoError(err)

	suite.Equal(hgetRedisResult, hgetRedis2natsResult)

	// Test HGET on non-existing field
	hgetRedisResult, err = suite.redisClient.HGet(ctx, "key", "field2").Result()
	suite.Error(err)

	hgetRedis2natsResult, err = suite.redis2natsClient.HGet(ctx, "key", "field2").Result()
	suite.Error(err)

	suite.Equal(hgetRedisResult, hgetRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestHGetAll() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test HGETALL empty
	hgetallRedisResult, err := suite.redisClient.HGetAll(ctx, "key").Result()
	suite.NoError(err)

	hgetallRedis2natsResult, err := suite.redis2natsClient.HGetAll(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(hgetallRedisResult, hgetallRedis2natsResult)

	// insert data
	for i := 0; i < 10; i++ {
		_, err = suite.redisClient.HSet(ctx, "key", fmt.Sprintf("field%d", i), fmt.Sprintf("value%d", i)).Result()
		suite.NoError(err)

		_, err = suite.redis2natsClient.HSet(ctx, "key", fmt.Sprintf("field%d", i), fmt.Sprintf("value%d", i)).Result()
		suite.NoError(err)
	}

	// Test HGETALL
	hgetallRedisResult, err = suite.redisClient.HGetAll(ctx, "key").Result()
	suite.NoError(err)

	hgetallRedis2natsResult, err = suite.redis2natsClient.HGetAll(ctx, "key").Result()
	suite.NoError(err)

	// Test HGETALL

	suite.Equal(hgetallRedisResult, hgetallRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestHKeys() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test HKEYS empty
	hkeysRedisResult, err := suite.redisClient.HKeys(ctx, "key").Result()
	suite.NoError(err)

	hkeysRedis2natsResult, err := suite.redis2natsClient.HKeys(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(hkeysRedisResult, hkeysRedis2natsResult)

	// insert data
	_, err = suite.redisClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	// Test HKEYS
	hkeysRedisResult, err = suite.redisClient.HKeys(ctx, "key").Result()
	suite.NoError(err)

	hkeysRedis2natsResult, err = suite.redis2natsClient.HKeys(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(hkeysRedisResult, hkeysRedis2natsResult)

	// TODO Add more patterns
}

func (suite *IntegrationTestSuite) TestHLen() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test HLEN empty
	hlenRedisResult, err := suite.redisClient.HLen(ctx, "key").Result()
	suite.NoError(err)

	hlenRedis2natsResult, err := suite.redis2natsClient.HLen(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(hlenRedisResult, hlenRedis2natsResult)

	// insert data
	_, err = suite.redisClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	// Test HLEN
	hlenRedisResult, err = suite.redisClient.HLen(ctx, "key").Result()
	suite.NoError(err)

	hlenRedis2natsResult, err = suite.redis2natsClient.HLen(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(hlenRedisResult, hlenRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestHExists() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test HExists empty
	hexistsRedisResult, err := suite.redisClient.HExists(ctx, "key", "field1").Result()
	suite.NoError(err)

	hexistsRedis2natsResult, err := suite.redis2natsClient.HExists(ctx, "key", "field1").Result()
	suite.NoError(err)

	suite.Equal(hexistsRedisResult, hexistsRedis2natsResult)

	// insert data
	_, err = suite.redisClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	// Test HExists
	hexistsRedisResult, err = suite.redisClient.HExists(ctx, "key", "field1").Result()
	suite.NoError(err)

	hexistsRedis2natsResult, err = suite.redis2natsClient.HExists(ctx, "key", "field1").Result()
	suite.NoError(err)

	suite.Equal(hexistsRedisResult, hexistsRedis2natsResult)

	// Test HExists on non-existing field
	hexistsRedisResult, err = suite.redisClient.HExists(ctx, "key", "field2").Result()
	suite.NoError(err)

	hexistsRedis2natsResult, err = suite.redis2natsClient.HExists(ctx, "key", "field2").Result()
	suite.NoError(err)

	suite.Equal(hexistsRedisResult, hexistsRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestHDel() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test HDEL
	hdelRedisResult, err := suite.redisClient.HDel(ctx, "key", "field1").Result()
	suite.NoError(err)

	hdelRedis2natsResult, err := suite.redis2natsClient.HDel(ctx, "key", "field1").Result()
	suite.NoError(err)

	suite.Equal(hdelRedisResult, hdelRedis2natsResult)

	// insert data
	_, err = suite.redisClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.HSet(ctx, "key", "field1", "value1").Result()
	suite.NoError(err)

	// Test HDEL
	hdelRedisResult, err = suite.redisClient.HDel(ctx, "key", "field1").Result()
	suite.NoError(err)

	hdelRedis2natsResult, err = suite.redis2natsClient.HDel(ctx, "key", "field1").Result()
	suite.NoError(err)

	suite.Equal(hdelRedisResult, hdelRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestLPush() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test LPush
	lpushRedisResult, err := suite.redisClient.LPush(ctx, "key", "value1").Result()
	suite.NoError(err)

	lpushRedis2natsResult, err := suite.redis2natsClient.LPush(ctx, "key", "value1").Result()
	suite.NoError(err)

	suite.Equal(lpushRedisResult, lpushRedis2natsResult)

	// Test LPush with different value
	lpushRedisResult, err = suite.redisClient.LPush(ctx, "key", "value2").Result()
	suite.NoError(err)

	lpushRedis2natsResult, err = suite.redis2natsClient.LPush(ctx, "key", "value2").Result()
	suite.NoError(err)

	suite.Equal(lpushRedisResult, lpushRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestLPop() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test LPop empty
	lpopRedisResult, err := suite.redisClient.LPop(ctx, "key").Result()
	suite.Error(err)

	lpopRedis2natsResult, err := suite.redis2natsClient.LPop(ctx, "key").Result()
	suite.Error(err)

	suite.Equal(lpopRedisResult, lpopRedis2natsResult)

	// insert data
	_, err = suite.redisClient.LPush(ctx, "key", "value1").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.LPush(ctx, "key", "value1").Result()
	suite.NoError(err)

	// Test LPop
	lpopRedisResult, err = suite.redisClient.LPop(ctx, "key").Result()
	suite.NoError(err)

	lpopRedis2natsResult, err = suite.redis2natsClient.LPop(ctx, "key").Result()
	suite.NoError(err)

	suite.Equal(lpopRedisResult, lpopRedis2natsResult)

	for i := 0; i < 10; i++ {
		_, err = suite.redisClient.LPush(ctx, "key", fmt.Sprintf("value%d", i)).Result()
		suite.NoError(err)

		_, err = suite.redis2natsClient.LPush(ctx, "key", fmt.Sprintf("value%d", i)).Result()
		suite.NoError(err)
	}

	// Test LPop
	lpopCountRedisResult, err := suite.redisClient.LPopCount(ctx, "key", 5).Result()
	suite.NoError(err)

	lpopCountRedis2natsResult, err := suite.redis2natsClient.LPopCount(ctx, "key", 5).Result()
	suite.NoError(err)

	suite.Equal(lpopCountRedisResult, lpopCountRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestLRange() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test LRange empty
	lrangeRedisResult, err := suite.redisClient.LRange(ctx, "key", 0, -1).Result()
	suite.NoError(err)

	lrangeRedis2natsResult, err := suite.redis2natsClient.LRange(ctx, "key", 0, -1).Result()
	suite.NoError(err)

	suite.Equal(lrangeRedisResult, lrangeRedis2natsResult)

	// insert data
	_, err = suite.redisClient.LPush(ctx, "key", "value1").Result()
	suite.NoError(err)

	_, err = suite.redis2natsClient.LPush(ctx, "key", "value1").Result()
	suite.NoError(err)

	// Test LRange
	lrangeRedisResult, err = suite.redisClient.LRange(ctx, "key", 0, -1).Result()
	suite.NoError(err)

	lrangeRedis2natsResult, err = suite.redis2natsClient.LRange(ctx, "key", 0, -1).Result()
	suite.NoError(err)

	suite.Equal(lrangeRedisResult, lrangeRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestSelect() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test SELECT
	selectRedisResult, err := suite.redisClient.Do(ctx, "SELECT", 1).Result()
	suite.NoError(err)

	selectRedis2natsResult, err := suite.redis2natsClient.Do(ctx, "SELECT", 1).Result()
	suite.NoError(err)

	suite.Equal(selectRedisResult, selectRedis2natsResult)

	// Test SELECT
	selectRedisResult, err = suite.redisClient.Do(ctx, "SELECT", 17).Result()
	suite.Error(err)

	selectRedis2natsResult, err = suite.redis2natsClient.Do(ctx, "SELECT", 17).Result()
	suite.Error(err)

	suite.Equal(selectRedisResult, selectRedis2natsResult)
}

func (suite *IntegrationTestSuite) TestNotSupported() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test unsupported command
	_, err := suite.redisClient.Do(ctx, "NOTSUPPORTED").Result()
	suite.Error(err)

	_, err = suite.redis2natsClient.Do(ctx, "NOTSUPPORTED").Result()
	suite.Error(err)
}

func (suite *IntegrationTestSuite) TestInvalidCommand() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.T().Cleanup(cancel)

	// Test unsupported command
	_, err := suite.redisClient.Do(ctx, "GET", "KEY", "OTHER", "COMMAND").Result()
	suite.Error(err)

	_, err = suite.redis2natsClient.Do(ctx, "GET", "KEY", "OTHER", "COMMAND").Result()
	suite.Error(err)
}

func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
