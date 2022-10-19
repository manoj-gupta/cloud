package redispool

import (
	"fmt"
	"os"

	"github.com/gomodule/redigo/redis"
)

const (
	stringKey = "redis-string-key"
)

// Run .. to test redispool
func Run(name string) {
	fmt.Printf("Running %s\n", name)

	conn := RedisPool.Get()
	defer func() {
		fmt.Printf("closing redis connection to %s\n", RedisServer)
		conn.Close()
	}()

	if _, err := conn.Do("SET", stringKey, "Hello Redis"); err != nil {
		fmt.Printf("Can not update %s err:%v\n", stringKey, err)
		os.Exit(1)
	}

	if val, err := redis.String(conn.Do("GET", stringKey)); err != nil {
		fmt.Printf("Can not read %s err:%v\n", stringKey, err)
		os.Exit(1)
	} else {
		fmt.Printf("READ Success --> key:%s val:%s\n", stringKey, val)
	}
}
