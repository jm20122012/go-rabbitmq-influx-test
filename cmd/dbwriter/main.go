package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SensorData struct {
	Voltage     float64 `json:"voltage"`
	Current     float64 `json:"current"`
	Temperature float64 `json:"temperature"`
	ID          string  `json:"id"`
}

var (
	influxDBToken  string
	influxDBOrg    string
	influxDBBucket string
)

const (
	rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	influxDBURL = "http://localhost:8086"
	queueName   = "testQueue1"
)

type RMQConn struct {
	Conn       *amqp.Connection
	ChannelMap map[string]*amqp.Channel
}

func (rmqConn *RMQConn) connectToRabbitMQ() {
	c, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		slog.Error("Failed to connect to RabbitMQ", "error", err)
	}

	// ch, err := c.Channel()
	// if err != nil {
	// 	slog.Error("Failed to open a channel", "error", err)
	// }

	rmqConn.Conn = c
}

// func connectToRabbitMQ() *amqp.Channel {
// 	conn, err := amqp.Dial(rabbitMQURL)
// 	if err != nil {
// 		slog.Error("Failed to connect to RabbitMQ", "error", err)
// 	}

// 	ch, err := conn.Channel()
// 	if err != nil {
// 		slog.Error("Failed to open a channel", "error", err)
// 	}

// 	return ch
// }

func connectToInfluxDB() influxdb2.Client {
	slog.Info("Connecting to Influx with", "token", influxDBToken, "org", influxDBOrg, "bucket", influxDBBucket)
	client := influxdb2.NewClient(influxDBURL, influxDBToken)
	return client
}

func consumeAndWrite(wg *sync.WaitGroup, queueName string, ch *amqp.Channel, influxClient influxdb2.Client) {
	defer wg.Done()
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
		slog.Error("Failed to register a consumer", "error", err)
	}

	for msg := range msgs {
		// slog.Info("Received a message", "message", msg.Body)
		var data SensorData
		err := json.Unmarshal(msg.Body, &data)
		if err != nil {
			slog.Error("Could not unmarshal data", "error", err)
		}
		writeDataToInfluxDB(influxClient, data)
	}
}

func writeDataToInfluxDB(client influxdb2.Client, data SensorData) {
	writeAPI := client.WriteAPIBlocking(influxDBOrg, influxDBBucket)

	// Create a point and add to batch
	p := influxdb2.NewPointWithMeasurement("testSensorData").
		AddTag("tag", "testSensor1").
		AddField("id", data.ID).
		AddField("voltage", data.Voltage).
		AddField("current", data.Current).
		AddField("temperature", data.Temperature).
		SetTime(time.Now())

	// Write the point
	err := writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		slog.Error("Failed to write to InfluxDB", "error", err)
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Error("Could nod load env file", "error", err)
	}
	influxDBToken = os.Getenv("DB_WRITER_TOKEN")
	influxDBOrg = os.Getenv("INFLUX_ORG")
	influxDBBucket = os.Getenv("INFLUX_BUCKET")

	var RMQ RMQConn
	RMQ.connectToRabbitMQ()
	RMQ.ChannelMap = make(map[string]*amqp.Channel)

	// 	ch, err := conn.Channel()
	// 	if err != nil {
	// 		slog.Error("Failed to open a channel", "error", err)
	// 	}

	ch1, err := RMQ.Conn.Channel()
	if err != nil {
		slog.Error("Could not create channel 1", "error", err)
	}
	slog.Info("Created channel 1")
	defer ch1.Close()

	ch2, err := RMQ.Conn.Channel()
	if err != nil {
		slog.Error("Could not create channel 2", "error", err)
	}
	slog.Info("Created channel 2")
	defer ch2.Close()

	slog.Info("Creating queue for channel 1")
	_, err = ch1.QueueDeclare(
		"testQueue1", // queue name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		slog.Error("Failed to create Queue for channel 1")
	}

	slog.Info("Binding channel 1 queue to exchange...")
	err = ch1.QueueBind(
		"testQueue1",       // queue name
		"",                 // routing key
		"testDataExchange", // exchange
		false,
		nil,
	)
	if err != nil {
		slog.Error("Could not bind channel 1 queue to exchange", "error", err)
	}

	slog.Info("Creating queue for channel 2")
	_, err = ch2.QueueDeclare(
		"testQueue2", // queue name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		slog.Error("Failed to create Queue for channel 2")
	}

	slog.Info("Binding channel 2 queue to exchange...")
	err = ch2.QueueBind(
		"testQueue2",       // queue name
		"",                 // routing key
		"testDataExchange", // exchange
		false,
		nil,
	)
	if err != nil {
		slog.Error("Could not bind channel 2 queue to exchange", "error", err)
	}

	slog.Info("Adding queues to queue map...")
	RMQ.ChannelMap["testQueue1"] = ch1
	RMQ.ChannelMap["testQueue2"] = ch2

	influxClient := connectToInfluxDB()
	defer influxClient.Close()

	slog.Info("Starting consume and write...")

	var wg sync.WaitGroup
	wg.Add(2)

	for key, val := range RMQ.ChannelMap {
		slog.Info("Starting channel consume routine", "channel", key)
		go consumeAndWrite(&wg, key, val, influxClient)
	}

	wg.Wait()
}

// func writeDataToInfluxDB(client influxdb2.Client, data SensorData) {
//     // Create a new point using the measurement name (e.g., "sensor_data")
//     p := influxdb2.NewPoint("sensor_data",
//         map[string]string{
//             "id": data.ID, // Using ID as a tag for querying
//         },
//         map[string]interface{}{
//             "voltage":     data.Voltage,
//             "current":     data.Current,
//             "temperature": data.Temperature,
//         },
//         time.Now(), // or you might want to use a timestamp from the data if available
//     )

//     // Get non-blocking write client
//     writeAPI := client.WriteAPI("your-organization", "your-bucket")

//     // Add data point to the batch
//     writeAPI.WritePoint(p)

//     // Always check for errors
//     // Ensure that any buffered data is sent
//     writeAPI.Flush()

//     // Check if there were any errors during the write
//     if writeErr := writeAPI.Errors(); writeErr != nil {
//         log.Fatalf("Write error: %s", writeErr)
//     }
// }
