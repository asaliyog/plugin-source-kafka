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
		Resolver:  fetchKernelMessages,
		Transform: transformers.TransformWithStruct(&KernelMessage{}),
	}
}

func PackagesTable() *schema.Table {
	return &schema.Table{
		Name:      "clean_osquery_packages",
		Resolver:  fetchPackagesMessages,
		Transform: transformers.TransformWithStruct(&PackagesMessage{}),
	}
}

func OSTable() *schema.Table {
	return &schema.Table{
		Name:      "clean_osquery_os",
		Resolver:  fetchOSMessages,
		Transform: transformers.TransformWithStruct(&OSMessage{}),
	}
}

func fetchKernelMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	consumer, err := c.Kafka.ConsumePartition("clean_osquery_kernel", 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	defer consumer.Close()

	for {
		select {
		case msg := <-consumer.Messages():
			var kernelMsg KernelMessage
			if err := json.Unmarshal(msg.Value, &kernelMsg); err != nil {
				c.Logger.Error().Err(err).Msg("failed to unmarshal kernel message")
				continue
			}
			res <- kernelMsg
		case <-ctx.Done():
			return nil
		}
	}
}

func fetchPackagesMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	consumer, err := c.Kafka.ConsumePartition("clean_osquery_packages", 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	defer consumer.Close()

	for {
		select {
		case msg := <-consumer.Messages():
			var packagesMsg PackagesMessage
			if err := json.Unmarshal(msg.Value, &packagesMsg); err != nil {
				c.Logger.Error().Err(err).Msg("failed to unmarshal packages message")
				continue
			}
			res <- packagesMsg
		case <-ctx.Done():
			return nil
		}
	}
}

func fetchOSMessages(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	c := meta.(*client.Client)
	consumer, err := c.Kafka.ConsumePartition("clean_osquery_os", 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	defer consumer.Close()

	for {
		select {
		case msg := <-consumer.Messages():
			var osMsg OSMessage
			if err := json.Unmarshal(msg.Value, &osMsg); err != nil {
				c.Logger.Error().Err(err).Msg("failed to unmarshal os message")
				continue
			}
			res <- osMsg
		case <-ctx.Done():
			return nil
		}
	}
} 