#!/bin/bash

# Build for Linux
GOOS=linux GOARCH=amd64 go build -o kafka-plugin-linux

# Build for macOS (current system)
go build -o kafka-plugin-mac

echo "Built binaries:"
echo "- kafka-plugin-linux (Linux)"
echo "- kafka-plugin-mac (macOS)" 