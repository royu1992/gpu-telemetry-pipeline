// Package gateway implements the REST API handler logic for the API Gateway
// service.
//
// It provides handlers for:
//   - GET /api/v1/gpus                       — list all GPUs with telemetry
//   - GET /api/v1/gpus/:id/telemetry         — query telemetry for a GPU
//     with optional start_time / end_time filters
package gateway
