# Building Custom Source Plugins with CloudQuery SDK

This repository demonstrates how to build a custom source plugin for CloudQuery that consumes data from Kafka topics. It serves as a practical example of extending CloudQuery's capabilities to work with any data source.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Key Components](#key-components)
- [Implementation Guide](#implementation-guide)
- [Best Practices](#best-practices)
- [Testing](#testing)
- [Deployment](#deployment)

## Overview

CloudQuery is a powerful data integration platform that allows you to sync data from various sources to any supported destination. The SDK provides a framework for building custom source plugins that can:

- Connect to any data source
- Transform data into a structured format
- Handle incremental syncs
- Support multiple tables
- Provide detailed logging and error handling

## Architecture

A CloudQuery source plugin consists of several key components:

1. **Plugin Definition**: The entry point that registers your plugin with CloudQuery
2. **Client**: Handles connection to your data source
3. **Tables**: Define the schema and data transformation
4. **Resolvers**: Implement the logic to fetch and process data

### Directory Structure
```
.
├── client/             # Data source client implementation
├── resources/          # Table definitions and resolvers
│   ├── plugin/        # Plugin configuration
│   └── services/      # Business logic and data processing
├── cmd/               # Command-line tools and testing
└── main.go           # Plugin entry point
```

## Key Components

### 1. Plugin Definition
```go
func Plugin() *plugin.Plugin {
    return plugin.NewPlugin(
        "kafka",           // Plugin name
        "v1.0.0",         // Version
        Configure,        // Configuration function
    )
}
```

### 2. Client Implementation
```go
type Client struct {
    Kafka  sarama.Consumer
    Logger zerolog.Logger
}

func New(logger zerolog.Logger) (schema.ClientMeta, error) {
    // Initialize connection to data source
    // Return client instance
}
```

### 3. Table Definition
```go
func MyTable() *schema.Table {
    return &schema.Table{
        Name:      "mytable",
        Resolver:  FetchData,
        Transform: transformers.TransformWithStruct(&MyStruct{}),
        Columns: []schema.Column{
            {
                Name:        "id",
                Type:        arrow.PrimitiveTypes.Int64,
                Resolver:    AutoIncrementResolver(&counter),
                PrimaryKey:  true,
            },
            // Additional columns...
        },
    }
}
```

### 4. Data Resolver
```go
func FetchData(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
    // Fetch and process data
    // Send results to the channel
    return nil
}
```

## Implementation Guide

### 1. Setting Up Your Project

1. Initialize your Go module:
```bash
go mod init github.com/yourusername/cq-source-yourplugin
```

2. Add required dependencies:
```bash
go get github.com/cloudquery/plugin-sdk/v4
```

### 2. Implementing the Plugin

1. Create the plugin entry point:
```go
// main.go
package main

import (
    "context"
    "log"
    "github.com/cloudquery/plugin-sdk/v4/serve"
    "github.com/yourusername/cq-source-yourplugin/resources/plugin"
)

func main() {
    p := serve.Plugin(plugin.Plugin())
    if err := p.Serve(context.Background()); err != nil {
        log.Fatalf("failed to serve plugin: %v", err)
    }
}
```

2. Define your data structures:
```go
type MyMessage struct {
    ID        string    `json:"id"`
    Timestamp time.Time `json:"timestamp"`
    Data      string    `json:"data"`
}
```

3. Implement data fetching:
```go
func FetchData(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
    client := meta.(*client.Client)
    
    // Fetch data from your source
    // Process and transform data
    // Send to result channel
    
    return nil
}
```

### 3. Building and Testing

1. Build your plugin:
```bash
go build -o cq-source-yourplugin
```

2. Test your plugin:
```bash
cloudquery sync config.yaml
```

## Best Practices

1. **Error Handling**
   - Use proper error wrapping
   - Implement graceful degradation
   - Provide detailed error messages

2. **Logging**
   - Use structured logging
   - Include relevant context
   - Set appropriate log levels

3. **Performance**
   - Implement pagination where possible
   - Use goroutines for concurrent processing
   - Handle rate limiting

4. **Data Quality**
   - Validate data before processing
   - Handle missing or malformed data
   - Implement data transformation

## Testing

1. **Unit Tests**
```go
func TestMyResolver(t *testing.T) {
    // Test your resolver implementation
}
```

2. **Integration Tests**
```go
func TestPlugin(t *testing.T) {
    // Test the entire plugin
}
```

## Deployment

1. Build for different platforms:
```bash
GOOS=linux GOARCH=amd64 go build -o cq-source-yourplugin-linux
GOOS=darwin GOARCH=amd64 go build -o cq-source-yourplugin-mac
```

2. Create a configuration file:
```yaml
kind: source
spec:
  name: yourplugin
  path: ./cq-source-yourplugin
  version: v1.0.0
  destinations: ["postgresql"]
  tables: ["*"]
  spec:
    # Your plugin-specific configuration
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please:
1. Check the [CloudQuery documentation](https://www.cloudquery.io/docs)
2. Open an issue in this repository
3. Join the [CloudQuery community](https://www.cloudquery.io/community)
