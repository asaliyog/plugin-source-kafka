#!/bin/bash

# Exit on error
set -e

echo "Building binaries..."

# Build for Linux
echo "Building for Linux..."
GOOS=linux GOARCH=amd64 go build -o kafka-plugin-linux

# Build for macOS
echo "Building for macOS..."
go build -o kafka-plugin-mac

echo "Built binaries:"
echo "- kafka-plugin-linux (Linux)"
echo "- kafka-plugin-mac (macOS)"

# Make binaries executable
chmod +x kafka-plugin-linux
chmod +x kafka-plugin-mac 