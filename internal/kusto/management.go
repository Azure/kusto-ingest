package kusto

import (
	"fmt"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/kql"
	"github.com/Azure/kusto-ingest/internal/cli"
)

func (m ManagementOptions) Run(cli cli.Provider) error {
	cli.Logger().Debug(
		"management command settings",
		"target.endpoint", m.KustoTarget.Endpoint,
		"target.database", m.KustoTarget.Database,
		"auth.tenant", m.Auth.TenantID,
		"auth.clientID", m.Auth.ClientID,
		"maxRetries", m.MaxRetries,
		"maxTimeout", m.MaxTimeout,
	)

	queryer, err := m.createQueryClient(m.KustoTarget, m.Auth)
	if err != nil {
		return fmt.Errorf("create Kusto query client: %w", err)
	}
	defer func() { _ = queryer.Close() }()

	ctx, cancel := cli.Context()
	defer cancel()

	cli.Logger().Info("executing management commands")
	start := time.Now()
	stmt := kql.New("").AddUnsafe(string(m.Source))

	var lastErr error
	var attempt int
	baseDelay := 1 * time.Second
	maxDelay := 10 * time.Second
	maxTimeout := time.Duration(m.MaxTimeout) * time.Second
	deadline := time.Now().Add(maxTimeout)

	for attempt = 0; attempt <= m.MaxRetries; attempt++ {
		_, err = queryer.Mgmt(ctx, m.KustoTarget.Database, stmt)
		if err == nil {
			cli.Logger().Info("management commands executed", "duration", time.Since(start), "attempt", attempt+1)
			return nil
		}

		// Check error type for retry logic using azure-kusto-go SDK's Retry function
		if !errors.Retry(err) {
			cli.Logger().Error("non-retryable error, aborting", "error", err)
			return fmt.Errorf("execute management commands: %w", err)
		}

		lastErr = err
		
		// Calculate next backoff duration with exponential backoff and jitter
		backoffDelay := exponentialBackoffWithJitter(attempt, baseDelay, maxDelay)
		
		if time.Now().Add(backoffDelay).After(deadline) {
			cli.Logger().Error("max timeout reached, aborting retries", "error", err)
			break
		}
		
		cli.Logger().Warn("transient error, will retry", "error", err, "attempt", attempt+1, "backoff", backoffDelay)
		time.Sleep(backoffDelay)
	}

	return fmt.Errorf("execute management commands after %d retries: %w", attempt, lastErr)
}


