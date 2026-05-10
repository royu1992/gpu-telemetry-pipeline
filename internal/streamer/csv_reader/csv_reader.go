package csv_reader

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// csvColumns lists the exact header names the streamer expects to find in the
// CSV file. The column index for each name is resolved once at Open time and
// stored in colIndex, so row parsing never relies on positional assumptions.
var csvColumns = []string{
	"timestamp",
	"metric_name",
	"gpu_id",
	"device",
	"uuid",
	"modelName",
	"Hostname",
	"value",
	"labels_raw",
}

// CSVRow represents a single, fully-parsed and validated CSV telemetry record.
// Field names mirror the JSON keys expected by the message-queue PublishRequest.
// All string values are trimmed of leading/trailing whitespace.
// Hostname has already been resolved according to the three-step policy
// described in STREAMER_ARCHITECTURE.md §4.
type CSVRow struct {
	Timestamp  string
	MetricName string
	GpuID      string
	Device     string
	UUID       string
	ModelName  string
	Hostname   string
	Value      string
	LabelsRaw  string
}

// Validate returns an error if any field that the message-queue marks as
// "binding:required" is empty. This mirrors the server-side validation so
// bad rows are caught before a network round-trip is attempted.
func (r CSVRow) Validate() error {
	if r.Timestamp == "" {
		return errors.New("missing required field: timestamp")
	}
	if r.MetricName == "" {
		return errors.New("missing required field: metric_name")
	}
	if r.UUID == "" {
		return errors.New("missing required field: uuid")
	}
	if r.Value == "" {
		return errors.New("missing required field: value")
	}
	return nil
}

// CSVReader wraps an open CSV file and provides sequential row access.
// It is not safe for concurrent use; only the single telemetry loop goroutine
// should call its methods.
type CSVReader struct {
	// file is the underlying OS file handle, kept open for the lifetime of the
	// Reader so Rewind() can seek back to the beginning without reopening.
	file *os.File

	// csv is the standard-library CSV parser layered on top of file.
	// It is replaced on every Rewind() call.
	csv *csv.Reader

	// colIndex maps each expected column name to its 0-based index in the
	// header row. Built once in Open() and never mutated afterwards.
	colIndex map[string]int

	// path is kept for error messages so callers know which file caused a problem.
	path string
}

// Open opens the CSV file at path, reads and validates the header row, and
// returns a ready-to-use Reader positioned at the first data row.
// The caller must call Close() when done to release the file handle.
func Open(path string) (*CSVReader, error) {
	// Open the file in read-only mode. The file must already exist at the path
	// provided by the STREAMER_CSV_PATH environment variable.
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening CSV file %q: %w", path, err)
	}

	r := &CSVReader{
		file:     f,
		csv:      csv.NewReader(f),
		colIndex: make(map[string]int),
		path:     path,
	}

	// Read the header row and build the column index. This must be done before
	// any data rows are read so colIndex is populated for parseRow().
	if err := r.readHeader(); err != nil {
		f.Close()
		return nil, err
	}

	return r, nil
}

// readHeader reads the first row from the CSV, treats it as a header, and
// populates r.colIndex. Returns an error if the row cannot be read or if any
// of the required columns is absent.
func (r *CSVReader) readHeader() error {
	// Read the first row; any CSV parse error here means the file is unreadable.
	headers, err := r.csv.Read()
	if err != nil {
		return fmt.Errorf("reading CSV header from %q: %w", r.path, err)
	}

	// Map each header name (trimmed) to its column position.
	for i, h := range headers {
		r.colIndex[strings.TrimSpace(h)] = i
	}

	// Verify that every column the streamer depends on is present. This
	// produces a clear error message at startup rather than a panic at runtime.
	for _, required := range csvColumns {
		if _, ok := r.colIndex[required]; !ok {
			return fmt.Errorf("CSV file %q is missing required column %q", r.path, required)
		}
	}

	return nil
}

// ReadRow reads and returns the next data row from the CSV file.
// It returns (Row, nil) on success, (Row{}, io.EOF) when the file is
// exhausted, and (Row{}, err) for any parse error.
// The hostname field in the returned Row has already been normalised.
func (r *CSVReader) ReadRow() (CSVRow, error) {
	// Read the next raw record from the CSV parser.
	record, err := r.csv.Read()
	if err != nil {
		// io.EOF is a normal sentinel, not a failure; the caller (loop) handles
		// it by rewinding. All other errors are unexpected parse failures.
		return CSVRow{}, err
	}

	// Convert the raw string slice into a structured Row, applying hostname
	// normalisation as part of the mapping.
	return r.parseRow(record), nil
}

// parseRow converts a raw CSV record (string slice) into a Row using the
// column index built during Open. Fields are trimmed of whitespace.
// The Hostname field is normalised via resolveHostname before being stored.
func (r *CSVReader) parseRow(record []string) CSVRow {
	// col is a helper closure that retrieves a field by column name, falling
	// back to an empty string if the record is shorter than expected.
	col := func(name string) string {
		idx, ok := r.colIndex[name]
		if !ok || idx >= len(record) {
			return ""
		}

		return strings.TrimSpace(record[idx])
	}

	return CSVRow{
		Timestamp:  col("timestamp"),
		MetricName: col("metric_name"),
		GpuID:      col("gpu_id"),
		Device:     col("device"),
		UUID:       col("uuid"),
		ModelName:  col("modelName"),
		// Hostname is resolved through the three-step resolution policy.
		Hostname:  resolveHostname(col("Hostname")),
		Value:     col("value"),
		LabelsRaw: col("labels_raw"),
	}
}

// Rewind seeks the underlying file back to byte 0 and re-initialises the CSV
// parser, then skips the header row so ReadRow() will resume from the first
// data row. It is called by the loop each time EOF is reached.
func (r *CSVReader) Rewind() error {
	// Seek the file descriptor back to the very beginning of the file.
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding CSV file %q: %w", r.path, err)
	}

	// Create a fresh csv.Reader on the same file descriptor. The old reader
	// must be discarded because it maintains internal state (buffer, line count)
	// that would be inconsistent after the seek.
	r.csv = csv.NewReader(r.file)

	// Re-skip the header row so the next ReadRow() call returns row 2 (the
	// first data row), not the column names.
	if _, err := r.csv.Read(); err != nil {
		return fmt.Errorf("skipping header on rewind of %q: %w", r.path, err)
	}

	return nil
}

// Close releases the underlying OS file handle. It should be deferred by the
// caller immediately after a successful Open().
func (r *CSVReader) Close() error {
	return r.file.Close()
}

// resolveHostname applies the three-step hostname resolution policy:
//  1. Trim all leading and trailing whitespace.
//  2. If the result is empty, fall back to the OS hostname (pod name in k8s).
//  3. If the OS hostname is also empty or unavailable, use "unknown-host".
func resolveHostname(csvHostname string) string {
	// Step 1: Trim whitespace. An empty string after trimming is treated as
	// "not provided" and triggers the fallback chain.
	h := strings.TrimSpace(csvHostname)

	// Step 2: Fall back to os.Hostname() if the CSV field is blank.
	// In Kubernetes, os.Hostname() returns the pod name, which is a meaningful
	// and traceable identity for observability even if the CSV source is missing.
	if h == "" {
		if sysHost, err := os.Hostname(); err == nil {
			h = strings.TrimSpace(sysHost)
		}
	}

	// Step 3: Use the static sentinel if both the CSV field and the system
	// hostname are unavailable. This ensures the field is never empty in the
	// telemetry record so downstream queries always have a value to filter on.
	if h == "" {
		h = "unknown-host"
	}

	return h
}
