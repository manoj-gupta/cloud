package main

import (
	"github.com/manoj-gupta/cloud/redispool"
)

type appInfo struct {
	name string
	run  func(name string)
}

var apps = []appInfo{
	{"redis", redispool.Run},
}

func main() {
	for _, app := range apps {
		app.run(app.name)
	}
}
