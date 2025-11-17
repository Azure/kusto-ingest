package kusto

import (
	"testing"
	"time"

	kustoerrors "github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/kusto-ingest/internal/cli/testingcli"
	"github.com/stretchr/testify/assert"
)

func TestCalculateDelay(t *testing.T) {
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
			name:    "attempt 4 grows exponentially",
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
				result := calculateDelay(tt.attempt, baseDelay)

				assert.GreaterOrEqual(t, result, tt.expected.min, "backoff should be at least minimum expected")
				assert.LessOrEqual(t, result, tt.expected.max, "backoff should not exceed maximum expected")

				// Ensure result is never zero or negative
				assert.Greater(t, result, time.Duration(0), "backoff should always be positive")
			}
		})
	}
}

func TestCalculateDelayDistribution(t *testing.T) {
	// Test that jitter is actually adding randomness
	baseDelay := 1 * time.Second
	attempt := 1

	results := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		results[i] = calculateDelay(attempt, baseDelay)
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

func TestCalculateDelayOverflow(t *testing.T) {
	maxDelay := time.Duration(9223372036854775807) // math.MaxInt64

	tests := []struct {
		name      string
		attempt   int
		baseDelay time.Duration
		expected  time.Duration
	}{
		{
			name:      "attempt 63 should return max duration",
			attempt:   63,
			baseDelay: 1 * time.Second,
			expected:  maxDelay,
		},
		{
			name:      "attempt 64 should return max duration",
			attempt:   64,
			baseDelay: 1 * time.Second,
			expected:  maxDelay,
		},
		{
			name:      "attempt 100 should return max duration",
			attempt:   100,
			baseDelay: 1 * time.Second,
			expected:  maxDelay,
		},
		{
			name:      "large base delay with moderate attempt should overflow",
			attempt:   40,
			baseDelay: time.Duration(1 << 30), // 1073741824 nanoseconds
			expected:  maxDelay,
		},
		{
			name:      "attempt 62 with 1 second base should not overflow",
			attempt:   62,
			baseDelay: 1 * time.Second,
			expected:  maxDelay, // 2^62 * 1 second would be ~146 years, but should be capped
		},
		{
			name:      "very large base delay with attempt 1 should overflow",
			attempt:   1,
			baseDelay: time.Duration(1 << 62), // Half of MaxInt64
			expected:  maxDelay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateDelay(tt.attempt, tt.baseDelay)

			// Result should equal maxDelay
			assert.Equal(t, tt.expected, result, "overflow should return max duration")

			// Verify result is never negative (which would indicate overflow bug)
			assert.Greater(t, result, time.Duration(0), "result should never be negative")
		})
	}
}

func TestCalculateDelayNoOverflow(t *testing.T) {
	// Test cases that should NOT overflow
	tests := []struct {
		name      string
		attempt   int
		baseDelay time.Duration
	}{
		{
			name:      "attempt 10 with 1 second base",
			attempt:   10,
			baseDelay: 1 * time.Second,
		},
		{
			name:      "attempt 20 with 1 millisecond base",
			attempt:   20,
			baseDelay: 1 * time.Millisecond,
		},
		{
			name:      "attempt 30 with 1 microsecond base",
			attempt:   30,
			baseDelay: 1 * time.Microsecond,
		},
		{
			name:      "attempt 50 with 1 nanosecond base",
			attempt:   50,
			baseDelay: 1 * time.Nanosecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateDelay(tt.attempt, tt.baseDelay)

			// Result should be positive
			assert.Greater(t, result, time.Duration(0), "result should be positive")

			// Result should be reasonable (not negative overflow)
			assert.Less(t, result, time.Duration(9223372036854775807), "result should not exceed MaxInt64")

			// Result should be at least baseDelay * 2^attempt (before jitter, accounting for jitter range)
			expectedMin := tt.baseDelay * time.Duration(int64(1)<<tt.attempt)
			// With jitter up to 10%, the result can be up to 1.1x the expected value
			assert.GreaterOrEqual(t, result, expectedMin, "result should be at least the exponential base")
		})
	}
}

func TestInvokeWithRetries(t *testing.T) {
	t.Run("success on first attempt", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invoke := func() error {
			callCount++
			return nil
		}

		err := invokeWithRetries(invoke, 3, 10, cli.Logger())
		assert.NoError(t, err)
		assert.Equal(t, 1, callCount, "should succeed on first attempt")
	})

	t.Run("retries on transient error then succeeds", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invoke := func() error {
			callCount++
			if callCount < 3 {
				// Return a retryable error
				return kustoerrors.ES(kustoerrors.OpQuery, kustoerrors.KTimeout, "request timed out")
			}
			return nil
		}

		err := invokeWithRetries(invoke, 5, 10, cli.Logger())
		assert.NoError(t, err)
		assert.Equal(t, 3, callCount, "should retry until success")
	})

	t.Run("fails immediately on non-retryable error", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invoke := func() error {
			callCount++
			// Return a non-retryable client args error
			return kustoerrors.ES(kustoerrors.OpQuery, kustoerrors.KClientArgs, "invalid arguments")
		}

		err := invokeWithRetries(invoke, 3, 10, cli.Logger())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-retryable kusto error")
		assert.Equal(t, 1, callCount, "should not retry on non-retryable errors")
	})

	t.Run("fails after exhausting max retries", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invoke := func() error {
			callCount++
			// Always return a retryable error
			return kustoerrors.ES(kustoerrors.OpQuery, kustoerrors.KHTTPError, "internal server error")
		}

		err := invokeWithRetries(invoke, 2, 30, cli.Logger())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exhausted max retries (2)")
		assert.Equal(t, 3, callCount, "should attempt MaxRetries+1 times (initial attempt + 2 retries; attempts 0, 1, 2)")
	})

	t.Run("respects max timeout", func(t *testing.T) {
		cli := testingcli.New()

		callCount := 0
		invoke := func() error {
			callCount++
			// Always return a retryable error
			return kustoerrors.ES(kustoerrors.OpQuery, kustoerrors.KTimeout, "request timed out")
		}

		err := invokeWithRetries(invoke, 10, 1, cli.Logger())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max timeout reached")
		// With 1 second timeout and exponential backoff (1s, 2s, 4s...),
		// we should only get a few attempts before timeout
		assert.Less(t, callCount, 5, "should stop early due to timeout")
	})
}
