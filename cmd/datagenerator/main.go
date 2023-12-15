package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SensorData struct {
	Voltage     float64 `json:"voltage"`
	Current     float64 `json:"current"`
	Temperature float64 `json:"temperature"`
	ID          string  `json:"id"`
}

type RMQStore struct {
	Conn  *amqp.Connection
	ChMap map[string]*amqp.Channel
}

func (store *RMQStore) publishData(wg *sync.WaitGroup, cycles int, cycleInterval int) {
	defer wg.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	slog.Info(fmt.Sprintf("Publishing %d cycles at an interval of %d milliseconds", cycles, cycleInterval))
	for i := 0; i < cycles; i++ {
		// Generate a new UUID for each sensor data
		sensorID, err := uuid.NewUUID()
		if err != nil {
			failOnError(err, "Failed to generate UUID")
			continue
		}

		// Generate random data
		data := SensorData{
			Voltage:     rand.Float64() * 100, // Random voltage
			Current:     rand.Float64() * 10,  // Random current
			Temperature: rand.Float64() * 40,  // Random temperature
			ID:          sensorID.String(),    // Use UUID as string
		}

		// Marshal the data to a JSON string
		jsonData, err := json.Marshal(data)
		if err != nil {
			failOnError(err, "Failed to marshal JSON")
			continue
		}

		// Publish the JSON data
		for _, ch := range store.ChMap {
			err := ch.PublishWithContext(ctx,
				"testDataExchange", // exchange
				"",                 // routing key
				false,              // mandatory
				false,              // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        jsonData,
				})
			failOnError(err, "Failed to publish a message")
		}

		// slog.Info("Published message", "messageID", i)
		time.Sleep(time.Duration(cycleInterval) * time.Millisecond)
	}
}

// func (store *RMQStore) publishData() {
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()
// 	for i := 0; i < 100; i++ {
// 		slog.Info("Publishing message", "messageID", i)
// 		for _, ch := range store.ChMap {
// 			err := ch.PublishWithContext(ctx,
// 				"testDataExchange", // exchange
// 				"",                 // routing key
// 				false,              // mandatory
// 				false,              // immediate
// 				amqp.Publishing{
// 					ContentType: "text/plain",
// 					Body:        []byte(fmt.Sprintf("Hello World, msg %d", i)),
// 				})
// 			failOnError(err, "Failed to publish a message")
// 		}
// 		time.Sleep(1 * time.Second)
// 	}
// }

func failOnError(err error, msg string) {
	if err != nil {
		errorMsg := fmt.Sprintf("%s: %s", msg, err)
		slog.Error(errorMsg)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"testDataExchange", // exchange name
		"direct",           // exchange type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	_, err = ch.QueueDeclare(
		"testQueue1", // queue name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		"testQueue1",       // queue name
		"",                 // routing key
		"testDataExchange", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	rmqStore := RMQStore{
		Conn:  conn,
		ChMap: make(map[string]*amqp.Channel),
	}
	rmqStore.ChMap["testQueue1"] = ch

	slog.Info("RabbitMQ Setup Compelted")

	var wg sync.WaitGroup
	threads := 10
	wg.Add(threads)
	// for i := 0; i < threads; i++ {
	// 	go rmqStore.publishData(&wg, 10000, 20)
	// }

	go rmqStore.publishData(&wg, 10000, 1000)
	go rmqStore.publishData(&wg, 10000, 1000)
	go rmqStore.publishData(&wg, 10000, 1000)
	go rmqStore.publishData(&wg, 10000, 1000)
	go rmqStore.publishData(&wg, 10000, 1000)
	go rmqStore.publishData(&wg, 10000, 1000)
	go rmqStore.publishData(&wg, 10000, 750)
	go rmqStore.publishData(&wg, 10000, 100)
	go rmqStore.publishData(&wg, 10000, 75)
	go rmqStore.publishData(&wg, 10000, 20)

	wg.Wait()
	slog.Info("Finished publishing")
}
