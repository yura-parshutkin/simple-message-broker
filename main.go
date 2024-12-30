package main

import (
	"brocker/pkg/mule"
	"brocker/pkg/mule/server"
	"context"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := server.NewHttpServer("localhost:8090", mule.Config{{
		QueueName: "test",
		Size:      10,
		SubsSize:  10,
	}})
	go func() {
		defer stop()
		err := srv.Start()
		if err != nil {
			log.Println("server start err:", err)
			return
		}
	}()
	defer func() { _ = srv.Stop(ctx) }()
	<-ctx.Done()
}
