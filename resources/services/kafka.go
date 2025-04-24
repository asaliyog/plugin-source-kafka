package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/transformers"
	"github.com/hermanschaaf/cq-source-xkcd/client"
	"github.com/rs/zerolog"
)

type KernelMessage struct {
	Arguments   string `json:"arguments"`
	Version     string `json:"version"`
	InstanceID  string `json:"instance_id"`
}

type PackagesMessage struct {
	Arch        string `json:"arch"`
	Name        string `json:"name"`
	Release     string `json:"release"`
	Version     string `json:"version"`
	InstanceID  string `json:"instance_id"`
}

type OSMessage struct {
	Build       string `json:"build"`
	Codename    string `json:"codename"`
	Platform    string `json:"platform"`
	PlatformLike string `json:"platform_like"`
	Version     string `json:"version"`
	InstanceID  string `json:"instance_id"`
}

func KernelTable() *schema.Table {
	return &schema.Table{
		Name:      "clean_osquery_kernel",
		Resolver:  FetchKernelMessages,
		Transform: transformers.TransformWithStruct(&KernelMessage{}),
		Description: "Kernel information from osquery",
	}
}

func PackagesTable() *schema.Table {
	return &schema.Table{
		Name:      "clean_osquery_packages",
		Resolver:  FetchPackagesMessages,
		Transform: transformers.TransformWithStruct(&PackagesMessage{}),
		Description: "Package information from osquery",
	}
}

func OSTable() *schema.Table {
	return &schema.Table{
		Name:      "clean_osquery_os",
		Resolver:  FetchOSMessages,
		Transform: transformers.TransformWithStruct(&OSMessage{}),
		Description: "OS information from osquery",
	}
}

const (
	messageTimeout = 30 * time.Second  // Increased timeout to 30 seconds
	maxRetries    = 3                 // Number of times to retry if no messages
)

func consumeMessages(ctx context.Context, consumer sarama.PartitionConsumer, logger zerolog.Logger, topic string) (<-chan *sarama.ConsumerMessage, <-chan error) {
	logger.Debug().Str("topic", topic).Msg("Starting to set up message consumption channels")
	
	messages := make(chan *sarama.ConsumerMessage)
	errors := make(chan error)

	go func() {
		defer func() {
			logger.Debug().Str("topic", topic).Msg("Closing message channels")
			close(messages)
			close(errors)
		}()

		logger.Debug().Str("topic", topic).Msg("Starting message consumption loop")
		for {
			select {
			case msg := <-consumer.Messages():
				logger.Debug().Str("topic", topic).
					Int64("offset", msg.Offset).
					Int("partition", int(msg.Partition)).
					Msg("Received message from Kafka")
				messages <- msg
			case err := <-consumer.Errors():
				logger.Error().Err(err).Str("topic", topic).Msg("Error consuming message")
				errors <- err
			case <-ctx.Done():
				logger.Debug().Str("topic", topic).Msg("Context cancelled, stopping message consumption")
				return
			}
		}
	}()

	logger.Debug().Str("topic", topic).Msg("Message consumption channels set up successfully")
	return messages, errors
}

func FetchKernelMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	c.Logger.Info().Msg("Starting to consume from clean_osquery_kernel topic")
	
	// List available topics
	topics, err := c.Kafka.Topics()
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to list topics")
	} else {
		c.Logger.Info().Strs("available_topics", topics).Msg("Available Kafka topics")
	}
	
	c.Logger.Debug().Msg("Attempting to create consumer for clean_osquery_kernel topic")
	consumer, err := c.Kafka.ConsumePartition("clean_osquery_kernel", 0, sarama.OffsetOldest)
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to create consumer for clean_osquery_kernel")
		return fmt.Errorf("failed to create consumer for clean_osquery_kernel: %w", err)
	}
	defer func() {
		c.Logger.Debug().Msg("Closing consumer for clean_osquery_kernel")
		consumer.Close()
	}()

	c.Logger.Info().Msg("Successfully created consumer for clean_osquery_kernel")
	messageCount := 0
	retryCount := 0
	lastMessageTime := time.Now()

	c.Logger.Debug().Msg("Setting up message consumption channels")
	messages, errors := consumeMessages(ctx, consumer, c.Logger, "clean_osquery_kernel")

	c.Logger.Info().Msg("Entering main message processing loop")
	for {
		select {
		case msg := <-messages:
			if msg == nil {
				if retryCount >= maxRetries {
					c.Logger.Info().Int("total_messages", messageCount).Msg("No more messages available")
					return nil
				}
				retryCount++
				c.Logger.Debug().Int("retry_count", retryCount).Msg("No message received, retrying")
				continue
			}
			retryCount = 0
			lastMessageTime = time.Now()
			messageCount++

			c.Logger.Debug().Str("topic", "clean_osquery_kernel").
				Int64("offset", msg.Offset).
				Int("partition", int(msg.Partition)).
				Str("key", string(msg.Key)).
				Msg("Processing raw message")

			var kernelMsg KernelMessage
			if err := json.Unmarshal(msg.Value, &kernelMsg); err != nil {
				c.Logger.Error().Err(err).
					Str("raw_value", string(msg.Value)).
					Msg("Failed to unmarshal kernel message")
				continue
			}
			c.Logger.Debug().Interface("message", kernelMsg).Msg("Successfully processed kernel message")
			res <- kernelMsg

			if messageCount%100 == 0 {
				c.Logger.Info().Int("processed_messages", messageCount).Msg("Kernel message processing progress")
			}

		case err := <-errors:
			c.Logger.Error().Err(err).Msg("Error consuming from clean_osquery_kernel")

		case <-time.After(messageTimeout):
			if time.Since(lastMessageTime) > messageTimeout {
				c.Logger.Info().Int("total_messages", messageCount).Msg("No new messages received, completing sync")
				return nil
			}

		case <-ctx.Done():
			c.Logger.Info().Int("total_messages", messageCount).Msg("Context cancelled")
			return nil
		}
	}
}

func FetchPackagesMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	c.Logger.Info().Msg("Starting to consume from clean_osquery_packages topic")
	
	consumer, err := c.Kafka.ConsumePartition("clean_osquery_packages", 0, sarama.OffsetOldest)
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to create consumer for clean_osquery_packages")
		return fmt.Errorf("failed to create consumer for clean_osquery_packages: %w", err)
	}
	defer consumer.Close()

	c.Logger.Info().Msg("Successfully created consumer for clean_osquery_packages")
	messageCount := 0
	retryCount := 0
	lastMessageTime := time.Now()

	messages, errors := consumeMessages(ctx, consumer, c.Logger, "clean_osquery_packages")

	for {
		select {
		case msg := <-messages:
			if msg == nil {
				if retryCount >= maxRetries {
					c.Logger.Info().Int("total_messages", messageCount).Msg("No more messages available")
					return nil
				}
				retryCount++
				continue
			}
			retryCount = 0
			lastMessageTime = time.Now()
			messageCount++

			var packagesMsg PackagesMessage
			if err := json.Unmarshal(msg.Value, &packagesMsg); err != nil {
				c.Logger.Error().Err(err).Msg("Failed to unmarshal packages message")
				continue
			}
			res <- packagesMsg

			if messageCount%100 == 0 {
				c.Logger.Info().Int("processed_messages", messageCount).Msg("Packages message processing progress")
			}

		case err := <-errors:
			c.Logger.Error().Err(err).Msg("Error consuming from clean_osquery_packages")

		case <-time.After(messageTimeout):
			if time.Since(lastMessageTime) > messageTimeout {
				c.Logger.Info().Int("total_messages", messageCount).Msg("No new messages received, completing sync")
				return nil
			}

		case <-ctx.Done():
			c.Logger.Info().Int("total_messages", messageCount).Msg("Context cancelled")
			return nil
		}
	}
}

func FetchOSMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	c.Logger.Info().Msg("Starting to consume from clean_osquery_os topic")
	
	consumer, err := c.Kafka.ConsumePartition("clean_osquery_os", 0, sarama.OffsetOldest)
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to create consumer for clean_osquery_os")
		return fmt.Errorf("failed to create consumer for clean_osquery_os: %w", err)
	}
	defer consumer.Close()

	c.Logger.Info().Msg("Successfully created consumer for clean_osquery_os")
	messageCount := 0
	retryCount := 0
	lastMessageTime := time.Now()

	messages, errors := consumeMessages(ctx, consumer, c.Logger, "clean_osquery_os")

	for {
		select {
		case msg := <-messages:
			if msg == nil {
				if retryCount >= maxRetries {
					c.Logger.Info().Int("total_messages", messageCount).Msg("No more messages available")
					return nil
				}
				retryCount++
				continue
			}
			retryCount = 0
			lastMessageTime = time.Now()
			messageCount++

			var osMsg OSMessage
			if err := json.Unmarshal(msg.Value, &osMsg); err != nil {
				c.Logger.Error().Err(err).Msg("Failed to unmarshal OS message")
				continue
			}
			res <- osMsg

			if messageCount%100 == 0 {
				c.Logger.Info().Int("processed_messages", messageCount).Msg("OS message processing progress")
			}

		case err := <-errors:
			c.Logger.Error().Err(err).Msg("Error consuming from clean_osquery_os")

		case <-time.After(messageTimeout):
			if time.Since(lastMessageTime) > messageTimeout {
				c.Logger.Info().Int("total_messages", messageCount).Msg("No new messages received, completing sync")
				return nil
			}

		case <-ctx.Done():
			c.Logger.Info().Int("total_messages", messageCount).Msg("Context cancelled")
			return nil
		}
	}
} 