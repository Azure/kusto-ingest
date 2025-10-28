package kusto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExponentialBackoffWithJitter(t *testing.T) {
	baseDelay := 1 * time.Second

	tests := []struct {
		name     string
		attempt  int
		expected struct {
			min time.Duration
			max time.Duration
		}
	}{
		{
			name:    "attempt 0 should return base delay with jitter",
			attempt: 0,
			expected: struct {
				min time.Duration
				max time.Duration
			}{
				min: baseDelay,                                 // 1s * 1.0 = 1s
				max: time.Duration(float64(baseDelay)*1.1) + 1, // 1s * 1.1 = 1.1s (plus small buffer)
			},
		},
		{
			name:    "attempt 1 should be roughly double base delay",
			attempt: 1,
			expected: struct {
				min time.Duration
				max time.Duration
			}{
				min: 2 * time.Second,                               // 2s * 1.0 = 2s
				max: time.Duration(float64(2*time.Second)*1.1) + 1, // 2s * 1.1 = 2.2s (plus small buffer)
			},
		},
		{
			name:    "attempt 2 should be roughly 4x base delay",
			attempt: 2,
			expected: struct {
				min time.Duration
				max time.Duration
			}{
				min: 4 * time.Second,                               // 4s * 1.0 = 4s
				max: time.Duration(float64(4*time.Second)*1.1) + 1, // 4s * 1.1 = 4.4s (plus small buffer)
			},
		},
		{
			name:    "attempt 3 should be roughly 8x base delay",
			attempt: 3,
			expected: struct {
				min time.Duration
				max time.Duration
			}{
				min: 8 * time.Second,                               // 8s * 1.0 = 8s
				max: time.Duration(float64(8*time.Second)*1.1) + 1, // 8s * 1.1 = 8.8s (plus small buffer)
			},
		},
		{
			name:    "attempt 4 grows exponentially without cap",
			attempt: 4,
			expected: struct {
				min time.Duration
				max time.Duration
			}{
				min: 16 * time.Second,                               // 16s * 1.0 = 16s
				max: time.Duration(float64(16*time.Second)*1.1) + 1, // 16s * 1.1 = 17.6s
			},
		},
		{
			name:    "negative attempt should return base delay",
			attempt: -1,
			expected: struct {
				min time.Duration
				max time.Duration
			}{
				min: baseDelay,
				max: time.Duration(float64(baseDelay)*1.1) + 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run the test multiple times to account for randomness in jitter
			for i := 0; i < 10; i++ {
				result := exponentialBackoffWithJitter(tt.attempt, baseDelay)

				assert.GreaterOrEqual(t, result, tt.expected.min, "backoff should be at least minimum expected")
				assert.LessOrEqual(t, result, tt.expected.max, "backoff should not exceed maximum expected")

				// Ensure result is never zero or negative
				assert.Greater(t, result, time.Duration(0), "backoff should always be positive")
			}
		})
	}
}

func TestExponentialBackoffWithJitterDistribution(t *testing.T) {
	// Test that jitter is actually adding randomness
	baseDelay := 1 * time.Second
	attempt := 1

	results := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		results[i] = exponentialBackoffWithJitter(attempt, baseDelay)
	}

	// Check that we get different values (indicating jitter is working)
	uniqueValues := make(map[time.Duration]bool)
	for _, result := range results {
		uniqueValues[result] = true
	}

	// With 100 samples and 10% jitter, we should have multiple unique values
	assert.Greater(t, len(uniqueValues), 1, "jitter should produce varying backoff durations")

	// All values should be within expected range for attempt 1
	expectedMin := 2 * time.Second
	expectedMax := time.Duration(float64(2*time.Second) * 1.1)

	for _, result := range results {
		assert.GreaterOrEqual(t, result, expectedMin, "all results should be at least base exponential value")
		assert.LessOrEqual(t, result, expectedMax, "all results should be within jitter bounds")
	}
}
