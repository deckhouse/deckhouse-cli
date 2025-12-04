/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package loki

import (
	"errors"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestGetEndTimestamp(t *testing.T) {
	// Save original values to restore after tests
	originalEndTimestamp := endTimestamp
	originalGetLogWithRetryFunc := getLogWithRetryFunc

	// Restore original values after all tests
	defer func() {
		endTimestamp = originalEndTimestamp
		getLogWithRetryFunc = originalGetLogWithRetryFunc
	}()

	tests := []struct {
		name                string
		endTimestampFlag    string
		mockGetLogWithRetry func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error)
		wantTimestamp       int64
		wantErr             bool
		errContains         string
	}{
		{
			name:             "success_with_empty_endTimestamp_flag_fetches_from_api",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{
									{"1704067200000000000", "test log message"},
								},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: 1704067200000000000,
			wantErr:       false,
		},
		{
			name:             "error_when_api_call_fails",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return nil, nil, errors.New("connection refused")
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error get latest timestamp JSON from loki",
		},
		{
			name:             "error_when_no_logs_found_empty_result",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{},
					},
				}, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "no logs found in Loki, cannot determine end timestamp",
		},
		{
			name:             "error_when_no_logs_found_empty_values",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "no logs found in Loki, cannot determine end timestamp",
		},
		{
			name:             "error_when_timestamp_is_not_a_valid_number",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{
									{"not_a_number", "test log message"},
								},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error converting timestamp",
		},
		{
			name:             "error_when_timestamp_overflows_int64",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{
									{"99999999999999999999999999999", "test log message"},
								},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error converting timestamp",
		},
		{
			name:             "success_with_valid_date_string_in_endTimestamp_flag",
			endTimestampFlag: "2024-01-01 12:00:00",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				// This should not be called when endTimestamp flag is set
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:             "success_with_different_valid_date",
			endTimestampFlag: "2023-06-15 08:30:45",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: time.Date(2023, 6, 15, 8, 30, 45, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:             "error_with_invalid_date_format_wrong_separator",
			endTimestampFlag: "2024/01/01 12:00:00",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error parsing date",
		},
		{
			name:             "error_with_invalid_date_format_missing_time",
			endTimestampFlag: "2024-01-01",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error parsing date",
		},
		{
			name:             "error_with_invalid_date_format_garbage_string",
			endTimestampFlag: "not-a-date-at-all",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error parsing date",
		},
		{
			name:             "error_with_invalid_date_format_iso8601",
			endTimestampFlag: "2024-01-01T12:00:00Z",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error parsing date",
		},
		{
			name:             "error_with_invalid_date_format_unix_timestamp",
			endTimestampFlag: "1704067200",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error parsing date",
		},
		{
			name:             "error_with_invalid_date_values",
			endTimestampFlag: "2024-13-45 25:70:90",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error parsing date",
		},
		{
			name:             "success_with_boundary_date_epoch",
			endTimestampFlag: "1970-01-01 00:00:00",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:             "success_with_boundary_date_end_of_day",
			endTimestampFlag: "2024-12-31 23:59:59",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				t.Error("getLogWithRetry should not be called when endTimestamp flag is set")
				return nil, nil, nil
			},
			wantTimestamp: time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:             "success_with_api_returning_large_nanosecond_timestamp",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{
									{"9223372036854775807", "test log message"}, // max int64
								},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: 9223372036854775807,
			wantErr:       false,
		},
		{
			name:             "success_with_api_returning_multiple_results_uses_first",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{
									{"1000000000000000000", "first log"},
									{"2000000000000000000", "second log"},
								},
							},
							{
								Values: [][]string{
									{"3000000000000000000", "third log"},
								},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: 1000000000000000000,
			wantErr:       false,
		},
		{
			name:             "success_with_api_returning_zero_timestamp",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{
									{"0", "test log message"},
								},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: 0,
			wantErr:       false,
		},
		{
			name:             "success_with_api_returning_negative_timestamp",
			endTimestampFlag: "",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return &QueryRange{
					Data: struct {
						Result []struct {
							Values [][]string `json:"values"`
						} `json:"result"`
					}{
						Result: []struct {
							Values [][]string `json:"values"`
						}{
							{
								Values: [][]string{
									{"-1000000000", "test log message"},
								},
							},
						},
					},
				}, nil, nil
			},
			wantTimestamp: -1000000000,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test state
			endTimestamp = tt.endTimestampFlag
			getLogWithRetryFunc = tt.mockGetLogWithRetry

			// Create fake kubernetes client and config
			kubeCl := fake.NewSimpleClientset()
			config := &rest.Config{}

			// Call the function under test
			got, err := getEndTimestamp(config, kubeCl, "test-token")

			// Check error expectations
			if (err != nil) != tt.wantErr {
				t.Errorf("getEndTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && tt.errContains != "" {
				if err == nil || !containsString(err.Error(), tt.errContains) {
					t.Errorf("getEndTimestamp() error = %v, should contain %q", err, tt.errContains)
					return
				}
			}

			// Check timestamp value
			if !tt.wantErr && got != tt.wantTimestamp {
				t.Errorf("getEndTimestamp() = %v, want %v", got, tt.wantTimestamp)
			}
		})
	}
}

func TestGetEndTimestamp_VerifiesCurlCommand(t *testing.T) {
	// Save original values
	originalEndTimestamp := endTimestamp
	originalGetLogWithRetryFunc := getLogWithRetryFunc

	defer func() {
		endTimestamp = originalEndTimestamp
		getLogWithRetryFunc = originalGetLogWithRetryFunc
	}()

	endTimestamp = ""
	var capturedCommand []string

	getLogWithRetryFunc = func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
		capturedCommand = fullCommand
		return &QueryRange{
			Data: struct {
				Result []struct {
					Values [][]string `json:"values"`
				} `json:"result"`
			}{
				Result: []struct {
					Values [][]string `json:"values"`
				}{
					{
						Values: [][]string{
							{"1704067200000000000", "test log"},
						},
					},
				},
			},
		}, nil, nil
	}

	kubeCl := fake.NewSimpleClientset()
	config := &rest.Config{}

	_, err := getEndTimestamp(config, kubeCl, "my-test-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the curl command was generated correctly
	if len(capturedCommand) == 0 {
		t.Fatal("expected curl command to be captured")
	}

	// Check that curl is the first command
	if capturedCommand[0] != "curl" {
		t.Errorf("expected first element to be 'curl', got %q", capturedCommand[0])
	}

	// Check that URL contains query_range
	foundURL := false
	for _, part := range capturedCommand {
		if containsString(part, "query_range") {
			foundURL = true
			break
		}
	}
	if !foundURL {
		t.Error("expected curl command to contain query_range URL")
	}

	// Check that auth token is included
	foundAuth := false
	for i, part := range capturedCommand {
		if part == "-H" && i+1 < len(capturedCommand) && containsString(capturedCommand[i+1], "Bearer my-test-token") {
			foundAuth = true
			break
		}
	}
	if !foundAuth {
		t.Error("expected curl command to contain Authorization header with token")
	}
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
