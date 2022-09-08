package main

import (
	"context"

	"github.com/manoj-gupta/cloud/kafkapc"
)

func main() {
	ctx := context.Background()
	// produce messages in a new go routine as
	// both producer and consumer functions are blocking
	go kafkapc.Producer(ctx)
	kafkapc.Consumer(ctx)
}
