package main

import (
	"testing"
)

func TestLogValidator_ValidateSize(t *testing.T) {

	validator := NewLogValidator()

	testCases := []struct {
		name        string
		input       []byte
		expectError bool
	}{
		{
			name:        "Empty Body",
			input:       []byte{},
			expectError: true,
		},
		{
			name:        "Body Too Large",
			input:       make([]byte, MaxMessageSize+1),
			expectError: true,
		},
		{
			name:        "Valid Body Size",
			input:       []byte(`{"message": "hello"}`),
			expectError: false,
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			err := validator.ValidateSize(tc.input)

			if tc.expectError && err == nil {

				t.Errorf("Expected an error, but got none.")
			}
			if !tc.expectError && err != nil {

				t.Errorf("Did not expect an error, but got: %v", err)
			}
		})
	}
}

func TestLogValidator_ValidateLogEntry(t *testing.T) {
	validator := NewLogValidator()

	baseValidLog := func() *LogEntry {
		return &LogEntry{
			Timestamp: "2025-07-01T12:00:00Z",
			Level:     "INFO",
			Message:   "This is a valid message.",
			Service:   "test-service",
		}
	}

	testCases := []struct {
		name          string
		modifier      func(log *LogEntry)
		expectedField string
	}{
		{
			name: "Missing Timestamp",
			modifier: func(log *LogEntry) {
				log.Timestamp = ""
			},
			expectedField: "timestamp",
		},
		{
			name: "Invalid Timestamp Format",
			modifier: func(log *LogEntry) {
				log.Timestamp = "not-a-real-timestamp"
			},
			expectedField: "timestamp",
		},
		{
			name: "Missing Level",
			modifier: func(log *LogEntry) {
				log.Level = ""
			},
			expectedField: "level",
		},
		{
			name: "Invalid Level",
			modifier: func(log *LogEntry) {
				log.Level = "NOT_A_REAL_LEVEL"
			},
			expectedField: "level",
		},
		{
			name: "Missing Message",
			modifier: func(log *LogEntry) {
				log.Message = ""
			},
			expectedField: "message",
		},
		{
			name: "Invalid Service Name",
			modifier: func(log *LogEntry) {
				log.Service = "invalid service name with spaces"
			},
			expectedField: "service",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logEntry := baseValidLog()
			tc.modifier(logEntry)

			errors := validator.ValidateLogEntry(logEntry)

			if len(errors) == 0 {
				t.Fatalf("Expected a validation error for field '%s', but got none.", tc.expectedField)
			}

			found := false
			for _, err := range errors {
				if err.Field == tc.expectedField {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected an error for field '%s', but it was not found in the errors: %v", tc.expectedField, errors)
			}
		})
	}

	t.Run("Completely Valid Log", func(t *testing.T) {
		logEntry := baseValidLog()
		errors := validator.ValidateLogEntry(logEntry)
		if len(errors) != 0 {
			t.Errorf("Expected no errors for a valid log, but got: %v", errors)
		}
	})
}

func TestLogValidator_ValidateComplete(t *testing.T) {
	validator := NewLogValidator()

	testCases := []struct {
		name    string
		input   []byte
		isValid bool
	}{
		{
			name:    "Valid Log",
			input:   []byte(`{"timestamp":"2025-07-01T12:00:00Z","level":"INFO","message":"hello"}`),
			isValid: true,
		},
		{
			name:    "Invalid JSON",
			input:   []byte(`this is not json`),
			isValid: false,
		},
		{
			name:    "Valid JSON, but invalid content (missing level)",
			input:   []byte(`{"timestamp":"2025-07-01T12:00:00Z","message":"hello"}`),
			isValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := validator.ValidateComplete(tc.input)
			if result.IsValid != tc.isValid {
				t.Errorf("Expected IsValid to be %v, but got %v. Errors: %v", tc.isValid, result.IsValid, result.Errors)
			}
		})
	}
}
