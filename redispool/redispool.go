package redispool

import (
	"fmt"
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
)

// default redis TCP port
const defaultRedisPort = 6379

// RedisServer ... redis server to connect to
var RedisServer = func() string {
	// $REDIS_SERVER takes precedence
	server := os.Getenv("REDIS_SERVER")
	if server == "" {
		// otherwise use the localhost:6379
		server = fmt.Sprintf("localhost:%d", defaultRedisPort)
	}
	return server
}()

// RedisPool ... redis connection pool
var RedisPool = redis.Pool{
	// check the health of an idle connection before the connection is used again
	TestOnBorrow: testIdleConn,
	MaxIdle:      2,               // Maximum number of idle connections in the pool
	IdleTimeout:  2 * time.Minute, // Close connections after remaining idle for this duration.
	Dial:         dialRedis,
}

func testIdleConn(c redis.Conn, _ time.Time) error {
	fmt.Println("Ping connection")
	reply, err := c.Do("Ping")
	fmt.Printf("TestOnBorrow :: Redis reply %v\n", reply)
	return err
}

func dialRedis() (redis.Conn, error) {
	c, err := redis.Dial("tcp", RedisServer)
	if err != nil {
		fmt.Printf("Dial (%s) failed: %v\n", RedisServer, err)
		return nil, err
	}

	fmt.Printf("opened redis connection to %s\n", RedisServer)
	return c, nil
}
