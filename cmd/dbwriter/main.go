package main

import (
	"context"
	"log"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	rabbitMQURL    = "amqp://guest:guest@localhost:5672/"
	queueName      = "testQueue1"
	influxDBURL    = "http://localhost:8086"
	influxDBToken  = "your-influxdb-token"
	influxDBOrg    = "your-org"
	influxDBBucket = "your-bucket"
)

func connectToRabbitMQ() *amqp.Channel {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}

	return ch
}

func connectToInfluxDB() influxdb2.Client {
	client := influxdb2.NewClient(influxDBURL, influxDBToken)
	return client
}

func consumeAndWrite(ch *amqp.Channel, influxClient influxdb2.Client) {
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
	}

	for msg := range msgs {
		log.Printf("Received a message: %s", msg.Body)
		writeDataToInfluxDB(influxClient, string(msg.Body))
	}
}

func writeDataToInfluxDB(client influxdb2.Client, data string) {
	writeAPI := client.WriteAPIBlocking(influxDBOrg, influxDBBucket)

	// Create a point and add to batch
	p := influxdb2.NewPointWithMeasurement("measurementName").
		AddTag("tag", "tagValue").
		AddField("field", data).
		SetTime(time.Now())

	// Write the point
	err := writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to write to InfluxDB: %s", err)
	}
}

func main() {
	rabbitCh := connectToRabbitMQ()
	defer rabbitCh.Close()

	influxClient := connectToInfluxDB()
	defer influxClient.Close()

	consumeAndWrite(rabbitCh, influxClient)
}
