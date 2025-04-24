package services

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/transformers"
	"github.com/hermanschaaf/cq-source-xkcd/client"
	"github.com/rs/zerolog"
)

var (
	// Global counter for auto-incrementing IDs
	kernelIDCounter    int64
	packagesIDCounter  int64
	osIDCounter       int64
)

// AutoIncrementResolver returns a resolver that auto-increments a counter
func AutoIncrementResolver(counter *int64) schema.ColumnResolver {
	return func(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource, c schema.Column) error {
		val := atomic.AddInt64(counter, 1)
		return resource.Set(c.Name, val)
	}
}

type KernelMessage struct {
	Arguments   string    `json:"arguments"`
	Version     string    `json:"version"`
	InstanceID  string    `json:"instance_id"`
	ProcessedAt time.Time `json:"processed_at"`
}

type PackagesMessage struct {
	Arch        string    `json:"arch"`
	Name        string    `json:"name"`
	Release     string    `json:"release"`
	Version     string    `json:"version"`
	InstanceID  string    `json:"instance_id"`
	ProcessedAt time.Time `json:"processed_at"`
}

type OSMessage struct {
	Build       string    `json:"build"`
	Codename    string    `json:"codename"`
	Platform    string    `json:"platform"`
	PlatformLike string   `json:"platform_like"`
	Version     string    `json:"version"`
	InstanceID  string    `json:"instance_id"`
	ProcessedAt time.Time `json:"processed_at"`
}

func KernelTable() *schema.Table {
	return &schema.Table{
		Name:      "cleanosquerykernel",
		Resolver:  FetchKernelMessages,
		Transform: transformers.TransformWithStruct(&KernelMessage{}),
		Description: "Kernel information from osquery",
		Columns: []schema.Column{
			{
				Name:        "id",
				Type:        arrow.PrimitiveTypes.Int64,
				Resolver:    AutoIncrementResolver(&kernelIDCounter),
				PrimaryKey:  true,
				Description: "Auto-incrementing primary key",
			},
			{
				Name:        "processed_at",
				Type:        arrow.FixedWidthTypes.Timestamp_us,
				Resolver:    schema.PathResolver("ProcessedAt"),
				Description: "When the message was processed",
			},
			{
				Name:     "instance_id",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("InstanceID"),
			},
			{
				Name:     "arguments",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Arguments"),
			},
			{
				Name:     "version",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Version"),
			},
		},
	}
}

func PackagesTable() *schema.Table {
	return &schema.Table{
		Name:      "cleanosquerypackages",
		Resolver:  FetchPackagesMessages,
		Transform: transformers.TransformWithStruct(&PackagesMessage{}),
		Description: "Package information from osquery",
		Columns: []schema.Column{
			{
				Name:        "id",
				Type:        arrow.PrimitiveTypes.Int64,
				Resolver:    AutoIncrementResolver(&packagesIDCounter),
				PrimaryKey:  true,
				Description: "Auto-incrementing primary key",
			},
			{
				Name:        "processed_at",
				Type:        arrow.FixedWidthTypes.Timestamp_us,
				Resolver:    schema.PathResolver("ProcessedAt"),
				Description: "When the message was processed",
			},
			{
				Name:     "instance_id",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("InstanceID"),
			},
			{
				Name:     "name",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Name"),
			},
			{
				Name:     "version",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Version"),
			},
			{
				Name:     "release",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Release"),
			},
			{
				Name:     "arch",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Arch"),
			},
		},
	}
}

func OSTable() *schema.Table {
	return &schema.Table{
		Name:      "cleanosqueryos",
		Resolver:  FetchOSMessages,
		Transform: transformers.TransformWithStruct(&OSMessage{}),
		Description: "OS information from osquery",
		Columns: []schema.Column{
			{
				Name:        "id",
				Type:        arrow.PrimitiveTypes.Int64,
				Resolver:    AutoIncrementResolver(&osIDCounter),
				PrimaryKey:  true,
				Description: "Auto-incrementing primary key",
			},
			{
				Name:        "processed_at",
				Type:        arrow.FixedWidthTypes.Timestamp_us,
				Resolver:    schema.PathResolver("ProcessedAt"),
				Description: "When the message was processed",
			},
			{
				Name:     "instance_id",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("InstanceID"),
			},
			{
				Name:     "build",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Build"),
			},
			{
				Name:     "codename",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Codename"),
			},
			{
				Name:     "platform",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Platform"),
			},
			{
				Name:     "platform_like",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("PlatformLike"),
			},
			{
				Name:     "version",
				Type:     arrow.BinaryTypes.String,
				Resolver: schema.PathResolver("Version"),
			},
		},
	}
}

const (
	messageTimeout = 30 * time.Second  // Increased timeout to 30 seconds
	maxRetries    = 3                 // Number of times to retry if no messages
)

func consumeMessages(ctx context.Context, consumer sarama.PartitionConsumer, logger zerolog.Logger, topic string) (<-chan *sarama.ConsumerMessage, <-chan error) {
	logger.Debug().Str("topic", topic).Msg("Starting to set up message consumption channels")
	
	if consumer == nil {
		logger.Error().Msg("Consumer is nil")
		return nil, nil
	}

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
			case msg, ok := <-consumer.Messages():
				if !ok {
					logger.Debug().Str("topic", topic).Msg("Message channel closed")
					return
				}
				if msg == nil {
					logger.Debug().Str("topic", topic).Msg("Received nil message")
					continue
				}
				logger.Debug().Str("topic", topic).
					Int64("offset", msg.Offset).
					Int("partition", int(msg.Partition)).
					Msg("Received message from Kafka")
				messages <- msg
			case err, ok := <-consumer.Errors():
				if !ok {
					logger.Debug().Str("topic", topic).Msg("Error channel closed")
					return
				}
				if err != nil {
					logger.Error().Err(err).Str("topic", topic).Msg("Error consuming message")
					errors <- err
				}
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
	topic := "cleanosquerykernel"
	c.Logger.Info().Msg("=== Starting to consume from " + topic + " topic ===")
	
	if c.Kafka == nil {
		c.Logger.Error().Msg("Kafka client is nil")
		return fmt.Errorf("kafka client is nil")
	}

	// List available topics
	topics, err := c.Kafka.Topics()
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to list topics")
	} else {
		c.Logger.Info().Strs("available_topics", topics).Msg("Available Kafka topics")
	}
	
	// Get all partitions for the topic
	partitions, err := c.Kafka.Partitions(topic)
	if err != nil {
		c.Logger.Error().Err(err).Str("topic", topic).Msg("Failed to get partitions")
		return fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
	}
	
	c.Logger.Info().Str("topic", topic).Int("partition_count", len(partitions)).Msg("Found partitions for topic")

	// Create a channel to collect errors from all partition consumers
	errChan := make(chan error, len(partitions))
	
	// Start a consumer for each partition
	for _, partition := range partitions {
		c.Logger.Info().Str("topic", topic).Int32("partition", partition).Msg("Starting consumer for partition")
		
		consumer, err := c.Kafka.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			c.Logger.Error().Err(err).Str("topic", topic).Int32("partition", partition).Msg("Failed to create consumer for partition")
			errChan <- fmt.Errorf("failed to create consumer for partition %d: %w", partition, err)
			continue
		}

		// Start a goroutine to consume messages from this partition
		go func(p int32, cons sarama.PartitionConsumer) {
			defer func() {
				c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Closing partition consumer")
				cons.Close()
			}()

			messageCount := 0
			retryCount := 0
			lastMessageTime := time.Now()

			c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Setting up message consumption channels")
			messages, errors := consumeMessages(ctx, cons, c.Logger, fmt.Sprintf("%s-partition-%d", topic, p))
			if messages == nil || errors == nil {
				errChan <- fmt.Errorf("failed to set up message consumption channels for partition %d", p)
				return
			}

			c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Entering message processing loop")
			for {
				select {
				case msg, ok := <-messages:
					if !ok {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Message channel closed")
						return
					}
					if msg == nil {
						if retryCount >= maxRetries {
							c.Logger.Info().Str("topic", topic).Int32("partition", p).Int("total_messages", messageCount).Msg("No more messages available")
							return
						}
						retryCount++
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Int("retry_count", retryCount).Msg("No message received, retrying")
						continue
					}
					retryCount = 0
					lastMessageTime = time.Now()
					messageCount++

					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Int64("offset", msg.Offset).
						Str("key", string(msg.Key)).
						Msg("Processing raw message")

					var kernelMsg KernelMessage
					if err := json.Unmarshal(msg.Value, &kernelMsg); err != nil {
						c.Logger.Error().Err(err).
							Str("topic", topic).Int32("partition", p).
							Str("raw_value", string(msg.Value)).
							Msg("Failed to unmarshal kernel message")
						continue
					}

					// Set the processed timestamp
					kernelMsg.ProcessedAt = time.Now()

					// Log the parsed message
					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Str("instance_id", kernelMsg.InstanceID).
						Str("version", kernelMsg.Version).
						Str("arguments", kernelMsg.Arguments).
						Time("processed_at", kernelMsg.ProcessedAt).
						Msg("Successfully parsed kernel message")

					// Send to result channel
					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Interface("message", kernelMsg).
						Msg("Sending message to result channel")
					res <- kernelMsg

					if messageCount%100 == 0 {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).
							Int("processed_messages", messageCount).
							Msg("Kernel message processing progress")
					}

				case err, ok := <-errors:
					if !ok {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Error channel closed")
						return
					}
					if err != nil {
						c.Logger.Error().Err(err).Str("topic", topic).Int32("partition", p).Msg("Error consuming message")
						errChan <- err
					}

				case <-time.After(messageTimeout):
					if time.Since(lastMessageTime) > messageTimeout {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).
							Int("total_messages", messageCount).
							Msg("No new messages received, completing sync")
						return
					}

				case <-ctx.Done():
					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Int("total_messages", messageCount).
						Msg("Context cancelled")
					return
				}
			}
		}(partition, consumer)
	}

	// Wait for all partition consumers to complete or for an error
	for i := 0; i < len(partitions); i++ {
		select {
		case err := <-errChan:
			if err != nil {
				c.Logger.Error().Err(err).Msg("Error from partition consumer")
				return err
			}
		case <-ctx.Done():
			c.Logger.Info().Msg("Context cancelled")
			return nil
		}
	}

	c.Logger.Info().Msg("=== Completed consuming from " + topic + " topic ===")
	return nil
}

func FetchPackagesMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	topic := "cleanosquerypackages"
	c.Logger.Info().Msg("=== Starting to consume from " + topic + " topic ===")
	
	if c.Kafka == nil {
		c.Logger.Error().Msg("Kafka client is nil")
		return fmt.Errorf("kafka client is nil")
	}

	// List available topics
	topics, err := c.Kafka.Topics()
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to list topics")
	} else {
		c.Logger.Info().Strs("available_topics", topics).Msg("Available Kafka topics")
	}
	
	// Get all partitions for the topic
	partitions, err := c.Kafka.Partitions(topic)
	if err != nil {
		c.Logger.Error().Err(err).Str("topic", topic).Msg("Failed to get partitions")
		return fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
	}
	
	c.Logger.Info().Str("topic", topic).Int("partition_count", len(partitions)).Msg("Found partitions for topic")

	// Create a channel to collect errors from all partition consumers
	errChan := make(chan error, len(partitions))
	
	// Start a consumer for each partition
	for _, partition := range partitions {
		c.Logger.Info().Str("topic", topic).Int32("partition", partition).Msg("Starting consumer for partition")
		
		consumer, err := c.Kafka.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			c.Logger.Error().Err(err).Str("topic", topic).Int32("partition", partition).Msg("Failed to create consumer for partition")
			errChan <- fmt.Errorf("failed to create consumer for partition %d: %w", partition, err)
			continue
		}

		// Start a goroutine to consume messages from this partition
		go func(p int32, cons sarama.PartitionConsumer) {
			defer func() {
				c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Closing partition consumer")
				cons.Close()
			}()

			messageCount := 0
			retryCount := 0
			lastMessageTime := time.Now()

			c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Setting up message consumption channels")
			messages, errors := consumeMessages(ctx, cons, c.Logger, fmt.Sprintf("%s-partition-%d", topic, p))
			if messages == nil || errors == nil {
				errChan <- fmt.Errorf("failed to set up message consumption channels for partition %d", p)
				return
			}

			c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Entering message processing loop")
			for {
				select {
				case msg, ok := <-messages:
					if !ok {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Message channel closed")
						return
					}
					if msg == nil {
						if retryCount >= maxRetries {
							c.Logger.Info().Str("topic", topic).Int32("partition", p).Int("total_messages", messageCount).Msg("No more messages available")
							return
						}
						retryCount++
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Int("retry_count", retryCount).Msg("No message received, retrying")
						continue
					}
					retryCount = 0
					lastMessageTime = time.Now()
					messageCount++

					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Int64("offset", msg.Offset).
						Str("key", string(msg.Key)).
						Msg("Processing raw message")

					var packagesMsg PackagesMessage
					if err := json.Unmarshal(msg.Value, &packagesMsg); err != nil {
						c.Logger.Error().Err(err).
							Str("topic", topic).Int32("partition", p).
							Str("raw_value", string(msg.Value)).
							Msg("Failed to unmarshal packages message")
						continue
					}
					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Interface("message", packagesMsg).
						Msg("Successfully processed packages message")
					res <- packagesMsg

					if messageCount%100 == 0 {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).
							Int("processed_messages", messageCount).
							Msg("Packages message processing progress")
					}

				case err, ok := <-errors:
					if !ok {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Error channel closed")
						return
					}
					if err != nil {
						c.Logger.Error().Err(err).Str("topic", topic).Int32("partition", p).Msg("Error consuming message")
						errChan <- err
					}

				case <-time.After(messageTimeout):
					if time.Since(lastMessageTime) > messageTimeout {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).
							Int("total_messages", messageCount).
							Msg("No new messages received, completing sync")
						return
					}

				case <-ctx.Done():
					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Int("total_messages", messageCount).
						Msg("Context cancelled")
					return
				}
			}
		}(partition, consumer)
	}

	// Wait for all partition consumers to complete or for an error
	for i := 0; i < len(partitions); i++ {
		select {
		case err := <-errChan:
			if err != nil {
				c.Logger.Error().Err(err).Msg("Error from partition consumer")
				return err
			}
		case <-ctx.Done():
			c.Logger.Info().Msg("Context cancelled")
			return nil
		}
	}

	c.Logger.Info().Msg("=== Completed consuming from " + topic + " topic ===")
	return nil
}

func FetchOSMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	topic := "cleanosqueryos"
	c.Logger.Info().Msg("=== Starting to consume from " + topic + " topic ===")
	
	if c.Kafka == nil {
		c.Logger.Error().Msg("Kafka client is nil")
		return fmt.Errorf("kafka client is nil")
	}

	// List available topics
	topics, err := c.Kafka.Topics()
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to list topics")
	} else {
		c.Logger.Info().Strs("available_topics", topics).Msg("Available Kafka topics")
	}
	
	// Get all partitions for the topic
	partitions, err := c.Kafka.Partitions(topic)
	if err != nil {
		c.Logger.Error().Err(err).Str("topic", topic).Msg("Failed to get partitions")
		return fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
	}
	
	c.Logger.Info().Str("topic", topic).Int("partition_count", len(partitions)).Msg("Found partitions for topic")

	// Create a channel to collect errors from all partition consumers
	errChan := make(chan error, len(partitions))
	
	// Start a consumer for each partition
	for _, partition := range partitions {
		c.Logger.Info().Str("topic", topic).Int32("partition", partition).Msg("Starting consumer for partition")
		
		consumer, err := c.Kafka.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			c.Logger.Error().Err(err).Str("topic", topic).Int32("partition", partition).Msg("Failed to create consumer for partition")
			errChan <- fmt.Errorf("failed to create consumer for partition %d: %w", partition, err)
			continue
		}

		// Start a goroutine to consume messages from this partition
		go func(p int32, cons sarama.PartitionConsumer) {
			defer func() {
				c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Closing partition consumer")
				cons.Close()
			}()

			messageCount := 0
			retryCount := 0
			lastMessageTime := time.Now()

			c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Setting up message consumption channels")
			messages, errors := consumeMessages(ctx, cons, c.Logger, fmt.Sprintf("%s-partition-%d", topic, p))
			if messages == nil || errors == nil {
				errChan <- fmt.Errorf("failed to set up message consumption channels for partition %d", p)
				return
			}

			c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Entering message processing loop")
			for {
				select {
				case msg, ok := <-messages:
					if !ok {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Message channel closed")
						return
					}
					if msg == nil {
						if retryCount >= maxRetries {
							c.Logger.Info().Str("topic", topic).Int32("partition", p).Int("total_messages", messageCount).Msg("No more messages available")
							return
						}
						retryCount++
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Int("retry_count", retryCount).Msg("No message received, retrying")
						continue
					}
					retryCount = 0
					lastMessageTime = time.Now()
					messageCount++

					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Int64("offset", msg.Offset).
						Str("key", string(msg.Key)).
						Msg("Processing raw message")

					var osMsg OSMessage
					if err := json.Unmarshal(msg.Value, &osMsg); err != nil {
						c.Logger.Error().Err(err).
							Str("topic", topic).Int32("partition", p).
							Str("raw_value", string(msg.Value)).
							Msg("Failed to unmarshal OS message")
						continue
					}
					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Interface("message", osMsg).
						Msg("Successfully processed OS message")
					res <- osMsg

					if messageCount%100 == 0 {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).
							Int("processed_messages", messageCount).
							Msg("OS message processing progress")
					}

				case err, ok := <-errors:
					if !ok {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).Msg("Error channel closed")
						return
					}
					if err != nil {
						c.Logger.Error().Err(err).Str("topic", topic).Int32("partition", p).Msg("Error consuming message")
						errChan <- err
					}

				case <-time.After(messageTimeout):
					if time.Since(lastMessageTime) > messageTimeout {
						c.Logger.Info().Str("topic", topic).Int32("partition", p).
							Int("total_messages", messageCount).
							Msg("No new messages received, completing sync")
						return
					}

				case <-ctx.Done():
					c.Logger.Info().Str("topic", topic).Int32("partition", p).
						Int("total_messages", messageCount).
						Msg("Context cancelled")
					return
				}
			}
		}(partition, consumer)
	}

	// Wait for all partition consumers to complete or for an error
	for i := 0; i < len(partitions); i++ {
		select {
		case err := <-errChan:
			if err != nil {
				c.Logger.Error().Err(err).Msg("Error from partition consumer")
				return err
			}
		case <-ctx.Done():
			c.Logger.Info().Msg("Context cancelled")
			return nil
		}
	}

	c.Logger.Info().Msg("=== Completed consuming from " + topic + " topic ===")
	return nil
} 