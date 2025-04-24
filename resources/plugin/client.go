package plugin

import (
	"context"
	"fmt"

	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/scheduler"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/rs/zerolog"

	"github.com/hermanschaaf/cq-source-xkcd/client"
	"github.com/hermanschaaf/cq-source-xkcd/resources/services"
)

const (
	maxMsgSize = 100 * 1024 * 1024 // 100 MiB
)

type Client struct {
	plugin.UnimplementedDestination
	scheduler *scheduler.Scheduler
	client    schema.ClientMeta
	logger    zerolog.Logger
}

func (c *Client) Close(ctx context.Context) error {
	if c.client != nil {
		return c.client.(*client.Client).Close()
	}
	return nil
}

func Configure(ctx context.Context, logger zerolog.Logger, spec []byte, opts plugin.NewClientOptions) (plugin.Client, error) {
	c := &Client{
		logger:    logger,
		scheduler: scheduler.NewScheduler(scheduler.WithLogger(logger)),
	}

	var err error
	c.client, err = client.New(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return c, nil
}

func (c *Client) Tables(_ context.Context, options plugin.TableOptions) (schema.Tables, error) {
	return schema.Tables{
		services.KernelTable(),
		services.PackagesTable(),
		services.OSTable(),
	}, nil
}

func (c *Client) Sync(ctx context.Context, options plugin.SyncOptions, res chan<- message.SyncMessage) error {
	if c.client == nil {
		return fmt.Errorf("client not configured")
	}

	tables, err := c.Tables(ctx, plugin.TableOptions{})
	if err != nil {
		return err
	}

	return c.scheduler.Sync(ctx, c.client, tables, res, scheduler.WithSyncDeterministicCQID(options.DeterministicCQID))
}
