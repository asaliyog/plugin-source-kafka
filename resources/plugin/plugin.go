package plugin

import (
	"github.com/cloudquery/plugin-sdk/v4/plugin"
)

func Plugin() *plugin.Plugin {
	return plugin.NewPlugin(
		"kafka",
		"v1.0.0",
		Configure,
	)
}
