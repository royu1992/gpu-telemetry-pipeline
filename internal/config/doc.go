// Package config provides shared environment variable loading helpers.
//
// Each service defines its own config struct (e.g. QueueConfig, StreamerConfig)
// and uses the helpers here to parse, validate, and apply defaults to
// environment variable values.
package config
