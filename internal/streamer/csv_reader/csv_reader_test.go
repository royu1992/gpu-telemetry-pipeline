package csv_reader

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestResolveHostname(t *testing.T) {
	// Get actual system hostname for testing fallback
	sysHost, _ := os.Hostname()
	trimmedSysHost := strings.TrimSpace(sysHost)

	tests := []struct {
		name        string
		csvHostname string
		expected    string
	}{
		{
			name:        "valid hostname preserved as-is",
			csvHostname: "GPU-NODE-01",
			expected:    "GPU-NODE-01",
		},
		{
			name:        "whitespaces are trimmed",
			csvHostname: "  Mixed-Case-Host  ",
			expected:    "Mixed-Case-Host",
		},
		{
			name:        "empty string falls back to system hostname",
			csvHostname: "",
			expected:    trimmedSysHost,
		},
		{
			name:        "blank string falls back to system hostname",
			csvHostname: "   ",
			expected:    trimmedSysHost,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// If system hostname is empty in this environment, fallback to "unknown-host"
			expected := tt.expected
			if expected == "" {
				expected = "unknown-host"
			}

			got := resolveHostname(tt.csvHostname)
			if got != expected {
				t.Errorf("resolveHostname() = %v, want %v", got, expected)
			}
		})
	}
}

func TestCSVRow_Validate(t *testing.T) {
	tests := []struct {
		name    string
		row     CSVRow
		wantErr bool
	}{
		{
			name: "valid row",
			row: CSVRow{
				Timestamp:  "2025-07-21T14:00:00Z",
				MetricName: "gpu_util",
				UUID:       "GPU-123",
				Value:      "45.5",
			},
			wantErr: false,
		},
		{
			name: "missing timestamp",
			row: CSVRow{
				MetricName: "gpu_util",
				UUID:       "GPU-123",
				Value:      "45.5",
			},
			wantErr: true,
		},
		{
			name: "missing metric name",
			row: CSVRow{
				Timestamp: "2025-07-21T14:00:00Z",
				UUID:      "GPU-123",
				Value:     "45.5",
			},
			wantErr: true,
		},
		{
			name: "missing uuid",
			row: CSVRow{
				Timestamp:  "2025-07-21T14:00:00Z",
				MetricName: "gpu_util",
				Value:      "45.5",
			},
			wantErr: true,
		},
		{
			name: "missing value",
			row: CSVRow{
				Timestamp:  "2025-07-21T14:00:00Z",
				MetricName: "gpu_util",
				UUID:       "GPU-123",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.row.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("CSVRow.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCSVReader(t *testing.T) {
	// Create a temporary CSV file for testing
	content := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,value,labels_raw\n" +
		"2025-07-21T14:00:00Z,gpu_temp,0,GeForce,UUID-AAA,RTX 3080,Node-A,72.0,label=test\n" +
		"2025-07-21T14:00:01Z,gpu_temp,0,GeForce,UUID-AAA,RTX 3080,Node-A,73.0,label=test\n"

	tmpfile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	t.Run("Open and ReadRow", func(t *testing.T) {
		r, err := Open(tmpfile.Name())
		if err != nil {
			t.Fatalf("Open() failed: %v", err)
		}
		defer r.Close()

		// Read first row
		row, err := r.ReadRow()
		if err != nil {
			t.Fatalf("ReadRow() 1 failed: %v", err)
		}
		if row.Hostname != "Node-A" {
			t.Errorf("expected Hostname Node-A, got %v", row.Hostname)
		}

		// Read second row
		row, err = r.ReadRow()
		if err != nil {
			t.Fatalf("ReadRow() 2 failed: %v", err)
		}
		if row.Value != "73.0" {
			t.Errorf("expected Value 73.0, got %v", row.Value)
		}

		// Read third row (EOF)
		_, err = r.ReadRow()
		if err != io.EOF {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})

	t.Run("Rewind", func(t *testing.T) {
		r, err := Open(tmpfile.Name())
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		// Read all rows to reach EOF
		for {
			_, err := r.ReadRow()
			if err == io.EOF {
				break
			}
		}

		// Rewind
		if err := r.Rewind(); err != nil {
			t.Fatalf("Rewind() failed: %v", err)
		}

		// Should be able to read first row again
		row, err := r.ReadRow()
		if err != nil {
			t.Fatalf("ReadRow() after rewind failed: %v", err)
		}
		if row.Hostname != "Node-A" {
			t.Errorf("expected Hostname Node-A after rewind, got %v", row.Hostname)
		}
	})

	t.Run("Open missing file", func(t *testing.T) {
		_, err := Open("non-existent.csv")
		if err == nil {
			t.Error("expected error opening missing file, got nil")
		}
	})

	t.Run("Invalid Header", func(t *testing.T) {
		badContent := "bad,header\n1,2\n"
		badFile, _ := os.CreateTemp("", "bad*.csv")
		defer os.Remove(badFile.Name())
		badFile.Write([]byte(badContent))
		badFile.Close()

		_, err := Open(badFile.Name())
		if err == nil {
			t.Error("expected error for missing required columns, got nil")
		}
	})

	t.Run("Empty file returns readHeader error", func(t *testing.T) {
		// Step: Create a completely empty file — csv.Read() will return io.EOF
		// on the first call, which readHeader() propagates as an error.
		emptyFile, err := os.CreateTemp("", "empty*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(emptyFile.Name())
		emptyFile.Close()

		// Step: Open should fail because the header row cannot be read.
		_, err = Open(emptyFile.Name())
		if err == nil {
			t.Error("expected error opening empty CSV file, got nil")
		}
	})

	t.Run("ReadRow returns parse error for malformed data row", func(t *testing.T) {
		// Step: Create a CSV file with a valid 9-column header but a data row
		// that has only 2 fields. The csv.Reader will return csv.ErrFieldCount
		// on the second Read(), exercising the non-EOF error path in ReadRow().
		malformedContent := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,value,labels_raw\n" +
			"only,two_fields\n"

		malformedFile, err := os.CreateTemp("", "malformed*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(malformedFile.Name())
		malformedFile.WriteString(malformedContent)
		malformedFile.Close()

		// Step: Open should succeed because the header is valid.
		r, err := Open(malformedFile.Name())
		if err != nil {
			t.Fatalf("Open() failed: %v", err)
		}
		defer r.Close()

		// Step: ReadRow() on the bad data row should return a non-nil, non-EOF error.
		_, err = r.ReadRow()
		if err == nil {
			t.Error("expected parse error for malformed row, got nil")
		}
		if err == io.EOF {
			t.Error("expected parse error for malformed row, got io.EOF")
		}
	})
}
