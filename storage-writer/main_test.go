package main

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

// Helper function to create a discard logger for tests
func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestIndexManager_GetIndexname tests that the index name is generated correctly
// based on the timestamp of the log.
func TestIndexManager_GetIndexname(t *testing.T) {
	// We don't need a real Elasticsearch client for this test, so we pass nil.
	// We pass a discard logger as the second argument.
	manager := NewIndexManager(nil, newDiscardLogger())

	testCases := []struct {
		name     string
		input    time.Time
		expected string
	}{
		{
			name:     "Standard Date",
			input:    time.Date(2025, 7, 15, 10, 0, 0, 0, time.UTC),
			expected: "logs-2025.07.15",
		},
		{
			name:     "Beginning of the Year",
			input:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "logs-2024.01.01",
		},
		{
			name:     "End of the Year",
			input:    time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC),
			expected: "logs-2026.12.31",
		},
		{
			// This tests the safety check in the function.
			name:     "Zero Value Timestamp",
			input:    time.Time{}, // The zero value for a time.
			// It should default to the current day's index.
			expected: "logs-" + time.Now().Format("2006.01.02"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			indexName := manager.GetIndexname(tc.input)
			if indexName != tc.expected {
				t.Errorf("Expected index name '%s', but got '%s'", tc.expected, indexName)
			}
		})
	}
}
