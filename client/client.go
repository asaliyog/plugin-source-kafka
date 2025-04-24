package client

import (
	"github.com/Shopify/sarama"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/rs/zerolog"
)

type Client struct {
	Kafka  sarama.Consumer
	Logger zerolog.Logger
}

func New(logger zerolog.Logger) (schema.ClientMeta, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"10.10.7.156:9092"}, config)
	if err != nil {
		return nil, err
	}

	return &Client{
		Kafka:  consumer,
		Logger: logger,
	}, nil
}

func (c *Client) ID() string {
	return "kafka"
}

func (c *Client) Close() error {
	return c.Kafka.Close()
}
