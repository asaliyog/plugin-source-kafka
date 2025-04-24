package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hermanschaaf/cq-source-xkcd/client"
	"github.com/hermanschaaf/cq-source-xkcd/resources/services"
	"github.com/rs/zerolog"
)

func main() {
	// Create logger with debug level
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger().Level(zerolog.DebugLevel)

	// Create Kafka client
	logger.Info().Msg("Creating Kafka client...")
	kafkaClient, err := client.New(logger)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer kafkaClient.(*client.Client).Close()
	logger.Info().Msg("Kafka client created successfully")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info().Msg("Received shutdown signal")
		cancel()
	}()

	// Start consuming messages
	logger.Info().Msg("Starting to consume messages...")

	// Create channels for each topic
	kernelChan := make(chan interface{}, 100)  // Buffered channels to prevent blocking
	packagesChan := make(chan interface{}, 100)
	osChan := make(chan interface{}, 100)

	// Start goroutines for each topic
	go func() {
		logger.Info().Msg("Starting kernel messages consumer...")
		if err := services.FetchKernelMessages(ctx, kafkaClient, nil, kernelChan); err != nil {
			logger.Error().Err(err).Msg("Error fetching kernel messages")
		}
	}()

	go func() {
		logger.Info().Msg("Starting packages messages consumer...")
		if err := services.FetchPackagesMessages(ctx, kafkaClient, nil, packagesChan); err != nil {
			logger.Error().Err(err).Msg("Error fetching packages messages")
		}
	}()

	go func() {
		logger.Info().Msg("Starting OS messages consumer...")
		if err := services.FetchOSMessages(ctx, kafkaClient, nil, osChan); err != nil {
			logger.Error().Err(err).Msg("Error fetching OS messages")
		}
	}()

	// Process messages
	logger.Info().Msg("Entering message processing loop...")
	messageCount := 0
	for {
		select {
		case msg := <-kernelChan:
			messageCount++
			logger.Info().Interface("message", msg).Int("total_messages", messageCount).Msg("Received kernel message")
		case msg := <-packagesChan:
			messageCount++
			logger.Info().Interface("message", msg).Int("total_messages", messageCount).Msg("Received packages message")
		case msg := <-osChan:
			messageCount++
			logger.Info().Interface("message", msg).Int("total_messages", messageCount).Msg("Received OS message")
		case <-ctx.Done():
			logger.Info().Int("total_messages", messageCount).Msg("Context cancelled or timeout reached")
			return
		}
	}
} 