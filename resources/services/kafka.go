package services

import (
	"context"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/transformers"
	"github.com/hermanschaaf/cq-source-xkcd/client"
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
	}
}

func PackagesTable() *schema.Table {
	return &schema.Table{
		Name:      "clean_osquery_packages",
		Resolver:  FetchPackagesMessages,
		Transform: transformers.TransformWithStruct(&PackagesMessage{}),
	}
}

func OSTable() *schema.Table {
	return &schema.Table{
		Name:      "clean_osquery_os",
		Resolver:  FetchOSMessages,
		Transform: transformers.TransformWithStruct(&OSMessage{}),
	}
}

func FetchKernelMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	c.Logger.Info().Msg("Starting to consume from clean_osquery_kernel topic")
	
	consumer, err := c.Kafka.ConsumePartition("clean_osquery_kernel", 0, sarama.OffsetOldest)
	if err != nil {
		c.Logger.Error().Err(err).Msg("Failed to create consumer for clean_osquery_kernel")
		return err
	}
	defer consumer.Close()

	c.Logger.Info().Msg("Successfully created consumer for clean_osquery_kernel")
	for {
		select {
		case msg := <-consumer.Messages():
			c.Logger.Debug().Str("topic", msg.Topic).Int64("offset", msg.Offset).Msg("Received message")
			var kernelMsg KernelMessage
			if err := json.Unmarshal(msg.Value, &kernelMsg); err != nil {
				c.Logger.Error().Err(err).Msg("Failed to unmarshal kernel message")
				continue
			}
			c.Logger.Info().Interface("message", kernelMsg).Msg("Successfully parsed kernel message")
			res <- kernelMsg
		case err := <-consumer.Errors():
			c.Logger.Error().Err(err).Msg("Error consuming from clean_osquery_kernel")
		case <-ctx.Done():
			c.Logger.Info().Msg("Context cancelled, stopping kernel message consumption")
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
		return err
	}
	defer consumer.Close()

	c.Logger.Info().Msg("Successfully created consumer for clean_osquery_packages")
	for {
		select {
		case msg := <-consumer.Messages():
			c.Logger.Debug().Str("topic", msg.Topic).Int64("offset", msg.Offset).Msg("Received message")
			var packagesMsg PackagesMessage
			if err := json.Unmarshal(msg.Value, &packagesMsg); err != nil {
				c.Logger.Error().Err(err).Msg("Failed to unmarshal packages message")
				continue
			}
			c.Logger.Info().Interface("message", packagesMsg).Msg("Successfully parsed packages message")
			res <- packagesMsg
		case err := <-consumer.Errors():
			c.Logger.Error().Err(err).Msg("Error consuming from clean_osquery_packages")
		case <-ctx.Done():
			c.Logger.Info().Msg("Context cancelled, stopping packages message consumption")
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
		return err
	}
	defer consumer.Close()

	c.Logger.Info().Msg("Successfully created consumer for clean_osquery_os")
	for {
		select {
		case msg := <-consumer.Messages():
			c.Logger.Debug().Str("topic", msg.Topic).Int64("offset", msg.Offset).Msg("Received message")
			var osMsg OSMessage
			if err := json.Unmarshal(msg.Value, &osMsg); err != nil {
				c.Logger.Error().Err(err).Msg("Failed to unmarshal OS message")
				continue
			}
			c.Logger.Info().Interface("message", osMsg).Msg("Successfully parsed OS message")
			res <- osMsg
		case err := <-consumer.Errors():
			c.Logger.Error().Err(err).Msg("Error consuming from clean_osquery_os")
		case <-ctx.Done():
			c.Logger.Info().Msg("Context cancelled, stopping OS message consumption")
			return nil
		}
	}
} 