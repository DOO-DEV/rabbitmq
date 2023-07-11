package main

import (
	"context"
	"golang.org/x/sync/errgroup"
	"log"
	"rabbitmq/internal"
	"time"
)

func main() {

	conn, err := internal.ConnectRabbitMQ("hossein", "xzwn", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}

	mqClient, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}

	messageBus, err := mqClient.Consume("customers_created", "email-service", false)
	if err != nil {
		panic(err)
	}

	// blocking is used to block forever
	var blocking chan struct{}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// Create an Errgroup to manage concurrecy
	g, ctx := errgroup.WithContext(ctx)
	// Set amount of concurrent tasks
	g.SetLimit(10)
	go func() {
		for message := range messageBus {
			// Spawn a worker
			msg := message
			g.Go(func() error {
				log.Printf("New Message: %v", msg)

				time.Sleep(10 * time.Second)
				// Multiple means that we acknowledge a batch of messages, leave false for now
				if err := msg.Ack(false); err != nil {
					log.Printf("Acknowledged message failed: Retry ? Handle manually %s\n", msg.MessageId)
					return err
				}
				log.Printf("Acknowledged message %s\n", msg.MessageId)
				return nil
			})
		}
	}()

	log.Println("Consuming, to close the program press CTRL+C")
	// This will block forever
	<-blocking
}
