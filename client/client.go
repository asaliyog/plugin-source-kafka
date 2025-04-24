package client

import (
	"github.com/Shopify/sarama"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/rs/zerolog"
	"time"
)

type Client struct {
	Kafka  sarama.Consumer
	Logger zerolog.Logger
	Spec   Spec
}

func New(logger zerolog.Logger) (schema.ClientMeta, error) {
	logger.Info().Msg("Starting to create new Kafka client")
	
	config := sarama.NewConfig()
	logger.Debug().Msg("Created new Sarama config")
	
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	
	logger.Debug().Msg("Configured Sarama settings")

	brokerAddr := "10.10.7.156:9092"
	logger.Info().Str("broker", brokerAddr).Msg("Attempting to connect to Kafka broker")
	
	consumer, err := sarama.NewConsumer([]string{brokerAddr}, config)
	if err != nil {
		logger.Error().Err(err).Str("broker", brokerAddr).Msg("Failed to create Kafka consumer")
		return nil, err
	}

	logger.Info().Str("broker", brokerAddr).Msg("Successfully connected to Kafka broker")
	
	// List available topics
	topics, err := consumer.Topics()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to list topics")
	} else {
		logger.Info().Strs("available_topics", topics).Msg("Successfully listed available topics")
	}

	return &Client{
		Kafka:  consumer,
		Logger: logger,
	}, nil
}

func (c *Client) ID() string {
	c.Logger.Debug().Msg("Getting client ID")
	return "kafka"
}

func (c *Client) Close() error {
	c.Logger.Info().Msg("Starting to close Kafka consumer")
	err := c.Kafka.Close()
	if err != nil {
		c.Logger.Error().Err(err).Msg("Error while closing Kafka consumer")
	} else {
		c.Logger.Info().Msg("Successfully closed Kafka consumer")
	}
	return err
}
