package client

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/rs/zerolog"
)

type Client struct {
	Kafka  sarama.Consumer
	Logger zerolog.Logger
	Spec   Spec
}

func New(logger zerolog.Logger) (schema.ClientMeta, error) {
	// Create a more verbose logger that writes to stdout
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger().Level(zerolog.DebugLevel)
	
	logger.Info().Msg("=== Starting to create new Kafka client ===")
	
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
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	logger.Info().Str("broker", brokerAddr).Msg("Successfully connected to Kafka broker")
	
	// List available topics
	topics, err := consumer.Topics()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to list topics")
		consumer.Close() // Close consumer if we can't list topics
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}
	
	if len(topics) == 0 {
		logger.Warn().Msg("No topics found in Kafka broker")
	} else {
		logger.Info().Strs("available_topics", topics).Msg("Successfully listed available topics")
	}

	client := &Client{
		Kafka:  consumer,
		Logger: logger,
	}

	// Verify the client is working by trying to list topics again
	_, err = client.Kafka.Topics()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to verify Kafka connection")
		consumer.Close()
		return nil, fmt.Errorf("failed to verify Kafka connection: %w", err)
	}

	logger.Info().Msg("=== Kafka client created successfully ===")
	return client, nil
}

func (c *Client) ID() string {
	if c == nil {
		return "nil-client"
	}
	c.Logger.Debug().Msg("Getting client ID")
	return "kafka"
}

func (c *Client) Close() error {
	if c == nil {
		return fmt.Errorf("client is nil")
	}
	if c.Kafka == nil {
		return fmt.Errorf("Kafka consumer is nil")
	}
	
	c.Logger.Info().Msg("=== Starting to close Kafka consumer ===")
	err := c.Kafka.Close()
	if err != nil {
		c.Logger.Error().Err(err).Msg("Error while closing Kafka consumer")
	} else {
		c.Logger.Info().Msg("Successfully closed Kafka consumer")
	}
	return err
}
