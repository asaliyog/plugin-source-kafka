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
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from the beginning
	config.Version = sarama.V2_8_0_0 // Use a stable version

	logger.Info().Msg("Connecting to Kafka broker at 10.10.7.156:9092")
	consumer, err := sarama.NewConsumer([]string{"10.10.7.156:9092"}, config)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create Kafka consumer")
		return nil, err
	}

	logger.Info().Msg("Successfully connected to Kafka broker")
	return &Client{
		Kafka:  consumer,
		Logger: logger,
	}, nil
}

func (c *Client) ID() string {
	return "kafka"
}

func (c *Client) Close() error {
	c.Logger.Info().Msg("Closing Kafka consumer")
	return c.Kafka.Close()
}
