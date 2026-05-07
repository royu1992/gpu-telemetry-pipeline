// Package server provides shared Gin HTTP server setup used across services.
//
// It includes router initialisation, common middleware (body-size limiting,
// request logging, panic recovery), and helpers for writing consistent
// JSON error responses.
package server
