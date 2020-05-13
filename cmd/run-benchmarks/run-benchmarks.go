package main

import (
	"context"
	"fmt"

	"github.com/mongodb/amboy/benchmarks"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := benchmarks.RunQueue(ctx)
	if err != nil {
		fmt.Println(err)
	}
}
