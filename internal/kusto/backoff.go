package kusto

import (
	"math/rand"
	"time"
)

// exponentialBackoffWithJitter calculates the next backoff duration using exponential backoff with jitter.
// This helps prevent thundering herd issues when many clients retry simultaneously.
//
// Formula: min(maxDelay, baseDelay * (2^attempt)) * (1 + jitter)
// Where jitter is a random value between 0 and 0.1 (10% jitter)
func exponentialBackoffWithJitter(attempt int, baseDelay, maxDelay time.Duration) time.Duration {
	if attempt < 0 {
		return baseDelay
	}

	// Calculate exponential backoff: baseDelay * 2^attempt
	delay := baseDelay
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
			break
		}
	}

	// Add jitter: random value between 0% and 10% of the delay
	jitter := rand.Float64() * 0.1 // 0% to 10% jitter
	jitteredDelay := time.Duration(float64(delay) * (1.0 + jitter))

	// Ensure we don't exceed the maximum delay
	if jitteredDelay > maxDelay {
		jitteredDelay = maxDelay
	}

	return jitteredDelay
}