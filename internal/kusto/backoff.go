package kusto

import (
	"math"
	"math/rand/v2"
	"time"
)

// exponentialBackoffWithJitter calculates the next backoff duration using exponential backoff with jitter.
// This helps prevent thundering herd issues when many clients retry simultaneously.
// Formula: baseDelay * (2^attempt) * (1 + jitter)
// Where jitter is a random value between 0 and 0.1 (10% jitter)
func exponentialBackoffWithJitter(attempt int, baseDelay time.Duration) time.Duration {
	if attempt < 0 {
		return baseDelay
	}

	// Calculate exponential backoff: baseDelay * 2^attempt
	delay := baseDelay * time.Duration(math.Pow(2, float64(attempt)))

	// Add jitter: random value between 0% and 10% of the delay
	jitter := rand.Float64() * 0.1 // 0% to 10% jitter
	jitteredDelay := time.Duration(float64(delay) * (1.0 + jitter))

	return jitteredDelay
}
