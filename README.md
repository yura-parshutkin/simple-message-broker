# Simplest message broker. 

Implement subscription and message sending to a queue via HTTP protocol. 
Ensure delivery guarantee: a message will be removed from the queue only upon receipt by all subscribers.

How to run:
```go
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
		err := srv.Run()
		if err != nil {
			log.Println("server run err:", err)
			return
		}
	}()
	defer func() { _ = srv.Stop(ctx) }()
	<-ctx.Done()
}
```

How to add new message:
```
curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"your": "body","can": "be","here": "as JSON"}' \
http://localhost:8090/v1/queues/test/messages
```

How to add subscription:
```
curl -X POST http://localhost:8090/v1/queues/test/subscriptions
```

Run project:
```
make
./application
```