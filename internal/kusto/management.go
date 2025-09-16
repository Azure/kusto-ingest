package kusto

import (
	"fmt"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/kql"
	"github.com/Azure/kusto-ingest/internal/cli"
)

func (m ManagementOptions) Run(cli cli.Provider) error {
	cli.Logger().Debug(
		"file ingestion settings",
		"target.endpoint", m.KustoTarget.Endpoint,
		"target.database", m.KustoTarget.Database,
		"auth.tenant", m.Auth.TenantID,
		"auth.clientID", m.Auth.ClientID,
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
	_, err = queryer.Mgmt(ctx, m.KustoTarget.Database, stmt)
	if err != nil {
		return fmt.Errorf("execute management commands: %w", err)
	}
	cli.Logger().Info("management commands executed", "duration", time.Since(start))

	return nil
}
