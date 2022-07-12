package main

import (
	"go-kafka-example/consumer"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

func main() {
	// Setup Logging
	customFormatter := new(logrus.JSONFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.PrettyPrint = true

	logrus.SetFormatter(customFormatter)

	kafkaConfig := getKafkaConfig("", "")
	consumers, err := sarama.NewConsumer([]string{"localhost:9092"}, kafkaConfig)
	if err != nil {
		logrus.Errorf("Unable to create kafka producer got error %v", err)
		return
	}
	defer func() {
		if err := consumers.Close(); err != nil {
			logrus.Errorf("Unable to stop kafka producer: %v", err)
			return
		}
	}()

	kafka := &consumer.KafkaConsumer{
		Consumer: consumers,
	}

	topics, _ := kafka.Consumer.Topics()

	signals := make(chan os.Signal, 10)

	signal.Notify(signals, os.Interrupt)

	 kafka.Consume(topics,signals)


}


func getKafkaConfig(username, password string) *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Net.WriteTimeout = 5 * time.Second
	kafkaConfig.Producer.Retry.Max = 0

	if username != "" {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = username
		kafkaConfig.Net.SASL.Password = password
	}
	return kafkaConfig
}
