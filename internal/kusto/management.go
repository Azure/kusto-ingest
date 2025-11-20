package kusto

import (
	"fmt"
	"time"

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
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := queryer.Mgmt(ctx, m.KustoTarget.Database, stmt)
		return err
	}

	cli.Logger().Info("executing management command")

	start := time.Now()
	err = invokeWithRetries(
		invokeQuery,
		m.MaxRetries,
		m.MaxTimeout,
		cli.Logger(),
	)
	if err != nil {
		cli.Logger().Error("failed to execute management command", "error", err)
		return err
	}

	cli.Logger().Info("management command executed successfully", "duration", time.Since(start))
	return nil
}
