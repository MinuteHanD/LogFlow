package main

import (
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestLogProcessor_normalizeTimestamp(t *testing.T) {
	processor := NewLogProcessor(newDiscardLogger(), "test-version")

	testCases := []struct {
		name         string
		input        string
		expectError  bool
		expectedTime time.Time
	}{
		{
			name:        "RFC3339 Format",
			input:       "2025-07-01T10:30:00Z",
			expectError: false,

			expectedTime: time.Date(2025, 7, 1, 10, 30, 0, 0, time.UTC),
		},
		{
			name:         "Format with Milliseconds",
			input:        "2025-07-01T10:30:00.123Z",
			expectError:  false,
			expectedTime: time.Date(2025, 7, 1, 10, 30, 0, 123000000, time.UTC),
		},
		{
			name:         "Format without Timezone (should be interpreted as UTC)",
			input:        "2025-07-01 10:30:00",
			expectError:  false,
			expectedTime: time.Date(2025, 7, 1, 10, 30, 0, 0, time.UTC),
		},
		{
			name:        "Invalid Format",
			input:       "July 1st, 2025",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsedTime, _, err := processor.normalizeTimestamp(tc.input)

			if tc.expectError {
				if err == nil {
					t.Error("Expected an error for invalid timestamp, but got none.")
				}
			} else {
				if err != nil {
					t.Errorf("Did not expect an error, but got: %v", err)
				}

				if !parsedTime.Equal(tc.expectedTime) {
					t.Errorf("Expected time %v, but got %v", tc.expectedTime, parsedTime)
				}
			}
		})
	}
}

func TestLogProcessor_normalizeLogLevel(t *testing.T) {
	processor := NewLogProcessor(newDiscardLogger(), "test-version")

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "Standard INFO", input: "INFO", expected: "INFO"},
		{name: "Lowercase info", input: "info", expected: "INFO"},
		{name: "WARNING to WARN", input: "WARNING", expected: "WARN"},
		{name: "PANIC to FATAL", input: "PANIC", expected: "FATAL"},
		{name: "Unknown level defaults to INFO", input: "VERBOSE", expected: "INFO"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized := processor.normalizeLogLevel(tc.input)
			if normalized != tc.expected {
				t.Errorf("Expected level '%s', but got '%s'", tc.expected, normalized)
			}
		})
	}
}

func TestLogProcessor_ProcessLog(t *testing.T) {
	processor := NewLogProcessor(newDiscardLogger(), "test-version")

	rawLog := IncomingLogEntry{
		Timestamp: "2025-07-01T12:00:00Z",
		Level:     "WARNING",
		Message:   "The system is getting warm.",
		Service:   "heater-control",
		Metadata:  map[string]string{"temp": "85C"},
	}

	parsedLog, err := processor.ProcessLog(rawLog)

	if err != nil {
		t.Fatalf("ProcessLog failed for a valid log: %v", err)
	}

	if parsedLog.Level != "WARN" {
		t.Errorf("Expected Level to be 'WARN', got '%s'", parsedLog.Level)
	}
	if parsedLog.Service != "heater-control" {
		t.Errorf("Expected Service to be 'heater-control', got '%s'", parsedLog.Service)
	}
	if parsedLog.ID == "" {
		t.Error("Expected a non-empty ID to be generated")
	}
	if parsedLog.MessageHash == "" {
		t.Error("Expected a non-empty MessageHash to be generated")
	}

	expectedMetadata := map[string]string{
		"temp":           "85C",
		"original_level": "WARNING",
	}
	if !reflect.DeepEqual(parsedLog.Metadata, expectedMetadata) {
		t.Errorf("Expected metadata %v, got %v", expectedMetadata, parsedLog.Metadata)
	}

	t.Run("Missing Service", func(t *testing.T) {
		rawLogNoService := IncomingLogEntry{
			Timestamp: "2025-07-01T12:00:00Z",
			Level:     "INFO",
			Message:   "Log with no service.",
		}
		parsed, _ := processor.ProcessLog(rawLogNoService)
		if parsed.Service != "unknown" {
			t.Errorf("Expected service to default to 'unknown', but got '%s'", parsed.Service)
		}
		if parsed.Metadata["service_defaulted"] != "true" {
			t.Error("Expected 'service_defaulted' metadata to be true when service is missing")
		}
	})
}
