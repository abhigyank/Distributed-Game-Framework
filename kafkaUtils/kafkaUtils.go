package kafkaUtils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var timeTopicCreate time.Duration = 1
var timeTopicDelete time.Duration = 1

// GetKafkaWriter configures and returns a Kafka Writer
func GetKafkaWriter(kafkaBrokerUrls []string, clientID string, topic string) *kafka.Writer {
	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: clientID,
	}

	config := kafka.WriterConfig{
		Brokers:      kafkaBrokerUrls,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Dialer:       dialer,
		WriteTimeout: 1 * time.Second,
		ReadTimeout:  1 * time.Second,
		BatchSize:	  20,
		BatchTimeout: 50 * time.Millisecond,
		Async:        true,
	}
	return kafka.NewWriter(config)
}

// GetKafkaWriterBall configures and returns a Kafka Writer for ball
func GetKafkaWriterBall(kafkaBrokerUrls []string, clientID string, topic string) *kafka.Writer {
	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: clientID,
	}

	config := kafka.WriterConfig{
		Brokers:      kafkaBrokerUrls,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Dialer:       dialer,
		WriteTimeout: 1 * time.Second,
		ReadTimeout:  1 * time.Second,
		BatchTimeout: 10 * time.Millisecond,
	}
	return kafka.NewWriter(config)
}


// GetKafkaReader configures and returns a Kafka Reader
func GetKafkaReader(kafkaBrokerUrls []string, clientID string, topic string) *kafka.Reader {
	config := kafka.ReaderConfig{
		Brokers:         kafkaBrokerUrls,
		GroupID:         clientID,
		Topic:           topic,
		MinBytes:        2,                     // 1B
		MaxBytes:        1000,                   // 100B
		MaxWait:         10 * time.Millisecond, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
		ReadLagInterval: -1,
	}
	return kafka.NewReader(config)
}

// PushKafkaMessage pushes the message on writer
func PushKafkaMessage(parent context.Context, writer *kafka.Writer, key, value []byte) (err error) {
	message := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
	return writer.WriteMessages(parent, message)
}

// DeleteTopics deletes all the topics from the Kafka Server
func DeleteTopics(kafkaAddress string) {
	conn, _ := kafka.Dial("tcp", kafkaAddress)
	partitions, _ := conn.ReadPartitions()
	for i := 0; i < len(partitions); i++ {
		if strings.Contains(partitions[i].Topic, "_0") {
			fmt.Println("Deleting topic:", partitions[i].Topic)
			DeleteTopic(kafkaAddress, partitions[i].Topic)
		}
	}

}

// DeleteTopic deletes a given topic from the Kafka Server
func DeleteTopic(kafkaAddress string, topicName string) {
	conn, _ := kafka.Dial("tcp", kafkaAddress)

	err := conn.DeleteTopics(topicName)
	if err != nil {
		// Can occur when topic doesn't exists
		fmt.Println("Deletion error for topic", topicName, err)
	} else {
		//Wait for deletion to complete.
		time.Sleep(timeTopicDelete * time.Second)
	}

	conn.Close()
}

// CreateTopic creates a given topic on the Kafka Server
func CreateTopic(kafkaAddress string, topicName string) {
	conn, _ := kafka.Dial("tcp", kafkaAddress)
	topic := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	err := conn.CreateTopics(topic)
	if err != nil {
		fmt.Println("Creation error for topic", topicName, err)
	} else {
		//Wait for creation to complete.
		time.Sleep(timeTopicCreate * time.Second)
	}

	conn.Close()
}
