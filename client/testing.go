package client

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/scheduler"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/transformers"
	"github.com/rs/zerolog"
)

func TestHelper(t *testing.T, table *schema.Table) {
	table.IgnoreInTests = false
	t.Helper()

	l := zerolog.New(zerolog.NewTestWriter(t)).Output(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.StampMicro},
	).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	sched := scheduler.NewScheduler(scheduler.WithLogger(l))

	c, err := New(l)
	if err != nil {
		t.Fatal(err)
	}

	tables := schema.Tables{table}
	if err := transformers.TransformTables(tables); err != nil {
		t.Fatal(err)
	}
	messages, err := sched.SyncAll(context.Background(), c, tables)
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}
	plugin.ValidateNoEmptyColumns(t, tables, messages)
}
