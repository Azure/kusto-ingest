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

	stmt := kql.New("").AddUnsafe(string(m.Source))
	invokeQuery := func() error {
		_, err := queryer.Mgmt(ctx, m.KustoTarget.Database, stmt)
		return err
	}

	cli.Logger().Info("executing management command")
	err = m.invokeQueryWithRetries(cli, invokeQuery)
	if err != nil {
		cli.Logger().Error("failed to execute management command", "error", err)
	}
	return err
}

func (m ManagementOptions) invokeQueryWithRetries(cli cli.Provider, invokeQuery func() error) error {
	start := time.Now()
	var err error
	baseDelay := 1 * time.Second
	maxTimeout := time.Duration(m.MaxTimeout) * time.Second
	deadline := time.Now().Add(maxTimeout)

	for attempt := 0; attempt <= m.MaxRetries; attempt++ {
		err = invokeQuery()
		if err == nil {
			cli.Logger().Info("management command executed successfully", "duration", time.Since(start), "attempt", attempt+1)
			return nil
		}

		// Check error type for retry logic using azure-kusto-go SDK's Retry function
		if !errors.Retry(err) {
			return fmt.Errorf("non-retryable kusto error: %w", err)
		}

		// Calculate next backoff duration with exponential backoff and jitter
		backoffDelay := exponentialBackoffWithJitter(attempt, baseDelay)

		if time.Now().Add(backoffDelay).After(deadline) {
			return fmt.Errorf("max timeout reached after %d retries: %w", attempt, err)
		}

		cli.Logger().Warn("transient kusto error, will retry", "error", err, "attempt", attempt+1, "backoff", backoffDelay)
		time.Sleep(backoffDelay)
	}

	return fmt.Errorf("exhausted max retries (%d): %w", m.MaxRetries, err)
}
