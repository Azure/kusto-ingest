package kusto

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/charmbracelet/log"
)

const (
	retryBaseDelay = 1 * time.Second
)

/*
invokeWithRetries executes the provided function with retry logic for transient Kusto errors.
It uses exponential backoff with jitter and respects both maxRetries and maxTimeout constraints.

Parameters:
  - invoke: The function to execute with retries
  - maxRetries: Maximum number of retry attempts (e.g., 3 means 4 total attempts: initial + 3 retries)
  - maxTimeout: Maximum total duration in seconds before timing out
  - logger: Logger for logging retry attempts (optional, can be nil)

Returns an error if:
  - A non-retryable error is encountered
  - Maximum timeout is reached
  - Maximum retries are exhausted
*/
func invokeWithRetries(
	invoke func() error,
	maxRetries int,
	maxTimeout int,
	logger *log.Logger,
) error {
	var err error
	maxTimeoutDuration := time.Duration(maxTimeout) * time.Second
	deadline := time.Now().Add(maxTimeoutDuration)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Check if we've exceeded the deadline before attempting
		if attempt > 0 && time.Now().After(deadline) {
			return fmt.Errorf("max timeout reached after %d retries: %w", attempt-1, err)
		}

		err = invoke()
		if err == nil {
			return nil
		}

		// Check error type for retry logic using azure-kusto-go SDK's Retry function
		if !errors.Retry(err) {
			return fmt.Errorf("non-retryable kusto error: %w", err)
		}

		// Calculate next backoff duration
		backoffDelay := calculateDelay(attempt, retryBaseDelay)

		if time.Now().Add(backoffDelay).After(deadline) {
			return fmt.Errorf("max timeout reached after %d retries: %w", attempt, err)
		}

		if logger != nil {
			logger.Warn("transient kusto error, will retry", "error", err, "attempt", attempt+1, "backoff", backoffDelay)
		}

		time.Sleep(backoffDelay)
	}

	return fmt.Errorf("exhausted max retries (%d): %w", maxRetries, err)
}

/*
calculateDelay computes the next retry delay duration.
Uses exponential backoff with jitter to prevent thundering herd issues.
Formula: baseDelay * (2^attempt) * (1 + jitter)
Where jitter is a random value between 0 and 0.1 (10% jitter)
*/
func calculateDelay(attempt int, baseDelay time.Duration) time.Duration {
	if attempt < 0 {
		return baseDelay
	}

	// Check for overflow before multiplying by checking if we would exceed math.MaxInt64
	maxDelay := time.Duration(math.MaxInt64)

	// Calculate 2^attempt using bit shifting, checking for overflow
	// If attempt >= 63, shifting would overflow (2^63 > MaxInt64)
	if attempt >= 63 {
		return maxDelay
	}

	multiplier := int64(1) << attempt // 2^attempt

	// Check if baseDelay * multiplier would overflow
	if baseDelay > maxDelay/time.Duration(multiplier) {
		return maxDelay
	}

	delay := baseDelay * time.Duration(multiplier)

	// Add jitter: random value between 0% and 10% of the delay
	jitter := rand.Float64() * 0.1 // 0% to 10% jitter
	jitteredDelay := time.Duration(float64(delay) * (1.0 + jitter))

	// Check for overflow after jitter (float64 conversion could overflow)
	if jitteredDelay < 0 || jitteredDelay > maxDelay {
		return maxDelay
	}

	// Apply max cap again after jitter
	if jitteredDelay > maxDelay {
		return maxDelay
	}

	return jitteredDelay
}
