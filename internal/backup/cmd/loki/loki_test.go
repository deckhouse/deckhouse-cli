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
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// TestParseEndTimestampFromFlag tests date parsing from flag string (no mocks needed)
func TestParseEndTimestampFromFlag(t *testing.T) {
	tests := []struct {
		name          string
		dateStr       string
		wantTimestamp int64
		wantErr       bool
		errContains   string
	}{
		{
			name:          "valid_date",
			dateStr:       "2024-01-01 12:00:00",
			wantTimestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:          "valid_date_different_time",
			dateStr:       "2023-06-15 08:30:45",
			wantTimestamp: time.Date(2023, 6, 15, 8, 30, 45, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:          "boundary_date_epoch",
			dateStr:       "1970-01-01 00:00:00",
			wantTimestamp: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:          "boundary_date_end_of_day",
			dateStr:       "2024-12-31 23:59:59",
			wantTimestamp: time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC).UnixNano(),
			wantErr:       false,
		},
		{
			name:        "invalid_format_wrong_separator",
			dateStr:     "2024/01/01 12:00:00",
			wantErr:     true,
			errContains: "error parsing date",
		},
		{
			name:        "invalid_format_missing_time",
			dateStr:     "2024-01-01",
			wantErr:     true,
			errContains: "error parsing date",
		},
		{
			name:        "invalid_format_garbage_string",
			dateStr:     "not-a-date-at-all",
			wantErr:     true,
			errContains: "error parsing date",
		},
		{
			name:        "invalid_format_iso8601",
			dateStr:     "2024-01-01T12:00:00Z",
			wantErr:     true,
			errContains: "error parsing date",
		},
		{
			name:        "invalid_format_unix_timestamp",
			dateStr:     "1704067200",
			wantErr:     true,
			errContains: "error parsing date",
		},
		{
			name:        "invalid_date_values",
			dateStr:     "2024-13-45 25:70:90",
			wantErr:     true,
			errContains: "error parsing date",
		},
		{
			name:        "empty_string",
			dateStr:     "",
			wantErr:     true,
			errContains: "error parsing date",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEndTimestampFromFlag(tt.dateStr)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseEndTimestampFromFlag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && tt.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("parseEndTimestampFromFlag() error = %v, should contain %q", err, tt.errContains)
					return
				}
			}

			if !tt.wantErr && got != tt.wantTimestamp {
				t.Errorf("parseEndTimestampFromFlag() = %v, want %v", got, tt.wantTimestamp)
			}
		})
	}
}

// TestFetchEndTimestampFromLoki tests fetching timestamp from Loki API
func TestFetchEndTimestampFromLoki(t *testing.T) {
	originalGetLogWithRetryFunc := getLogWithRetryFunc
	defer func() {
		getLogWithRetryFunc = originalGetLogWithRetryFunc
	}()

	tests := []struct {
		name                string
		mockGetLogWithRetry func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error)
		wantTimestamp       int64
		wantErr             bool
		errContains         string
	}{
		{
			name: "success_fetches_timestamp",
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
			name: "error_when_api_call_fails",
			mockGetLogWithRetry: func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
				return nil, nil, errors.New("connection refused")
			},
			wantTimestamp: 0,
			wantErr:       true,
			errContains:   "error get latest timestamp JSON from loki",
		},
		{
			name: "error_when_empty_result",
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
			name: "error_when_empty_values",
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
			name: "error_when_invalid_timestamp",
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
			name: "error_when_timestamp_overflows",
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
			name: "success_with_max_int64_timestamp",
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
									{"9223372036854775807", "test log message"},
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
			name: "success_uses_first_value_from_multiple_results",
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
			name: "success_with_zero_timestamp",
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
			name: "success_with_negative_timestamp",
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
			getLogWithRetryFunc = tt.mockGetLogWithRetry

			kubeCl := fake.NewSimpleClientset()
			config := &rest.Config{}

			got, err := fetchEndTimestampFromLoki(config, kubeCl, "test-token")

			if (err != nil) != tt.wantErr {
				t.Errorf("fetchEndTimestampFromLoki() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && tt.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("fetchEndTimestampFromLoki() error = %v, should contain %q", err, tt.errContains)
					return
				}
			}

			if !tt.wantErr && got != tt.wantTimestamp {
				t.Errorf("fetchEndTimestampFromLoki() = %v, want %v", got, tt.wantTimestamp)
			}
		})
	}
}

// TestGetEndTimestamp tests the orchestrating function
func TestGetEndTimestamp(t *testing.T) {
	originalEndTimestamp := endTimestamp
	originalGetLogWithRetryFunc := getLogWithRetryFunc

	defer func() {
		endTimestamp = originalEndTimestamp
		getLogWithRetryFunc = originalGetLogWithRetryFunc
	}()

	t.Run("uses_flag_value_when_set", func(t *testing.T) {
		endTimestamp = "2024-01-01 12:00:00"
		apiCalled := false
		getLogWithRetryFunc = func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
			apiCalled = true
			return nil, nil, errors.New("should not be called")
		}

		kubeCl := fake.NewSimpleClientset()
		config := &rest.Config{}

		got, err := getEndTimestamp(config, kubeCl, "test-token")
		if err != nil {
			t.Errorf("getEndTimestamp() unexpected error = %v", err)
			return
		}

		if apiCalled {
			t.Error("getEndTimestamp() should not call API when flag is set")
		}

		want := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano()
		if got != want {
			t.Errorf("getEndTimestamp() = %v, want %v", got, want)
		}
	})

	t.Run("fetches_from_api_when_flag_is_empty", func(t *testing.T) {
		endTimestamp = ""
		apiCalled := false
		getLogWithRetryFunc = func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
			apiCalled = true
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

		got, err := getEndTimestamp(config, kubeCl, "test-token")
		if err != nil {
			t.Errorf("getEndTimestamp() unexpected error = %v", err)
			return
		}

		if !apiCalled {
			t.Error("getEndTimestamp() should call API when flag is empty")
		}

		want := int64(1704067200000000000)
		if got != want {
			t.Errorf("getEndTimestamp() = %v, want %v", got, want)
		}
	})

	t.Run("returns_parse_error_for_invalid_flag", func(t *testing.T) {
		endTimestamp = "invalid-date"
		getLogWithRetryFunc = func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
			t.Error("API should not be called for invalid flag")
			return nil, nil, nil
		}

		kubeCl := fake.NewSimpleClientset()
		config := &rest.Config{}

		_, err := getEndTimestamp(config, kubeCl, "test-token")
		if err == nil {
			t.Error("getEndTimestamp() expected error for invalid date flag")
			return
		}

		if !strings.Contains(err.Error(), "error parsing date") {
			t.Errorf("getEndTimestamp() error = %v, should contain 'error parsing date'", err)
		}
	})

	t.Run("returns_api_error_when_api_fails", func(t *testing.T) {
		endTimestamp = ""
		getLogWithRetryFunc = func(config *rest.Config, kubeCl kubernetes.Interface, fullCommand []string) (*QueryRange, *SeriesAPI, error) {
			return nil, nil, errors.New("network error")
		}

		kubeCl := fake.NewSimpleClientset()
		config := &rest.Config{}

		_, err := getEndTimestamp(config, kubeCl, "test-token")
		if err == nil {
			t.Error("getEndTimestamp() expected error when API fails")
			return
		}

		if !strings.Contains(err.Error(), "error get latest timestamp JSON from loki") {
			t.Errorf("getEndTimestamp() error = %v, should contain 'error get latest timestamp JSON from loki'", err)
		}
	})
}

// TestFetchEndTimestampFromLoki_VerifiesCurlCommand verifies correct curl command generation
func TestFetchEndTimestampFromLoki_VerifiesCurlCommand(t *testing.T) {
	originalGetLogWithRetryFunc := getLogWithRetryFunc
	defer func() {
		getLogWithRetryFunc = originalGetLogWithRetryFunc
	}()

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

	_, err := fetchEndTimestampFromLoki(config, kubeCl, "my-test-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
		if strings.Contains(part, "query_range") {
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
		if part == "-H" && i+1 < len(capturedCommand) && strings.Contains(capturedCommand[i+1], "Bearer my-test-token") {
			foundAuth = true
			break
		}
	}
	if !foundAuth {
		t.Error("expected curl command to contain Authorization header with token")
	}
}
