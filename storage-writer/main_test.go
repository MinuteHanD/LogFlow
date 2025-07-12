package main

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestIndexManager_GetIndexname(t *testing.T) {

	manager := NewIndexManager(nil, newDiscardLogger(), "logs")

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

			name:  "Zero Value Timestamp",
			input: time.Time{},

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
