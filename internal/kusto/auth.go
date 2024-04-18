package kusto

import (
	"fmt"

	"github.com/Azure/azure-kusto-go/kusto"
)

func (a AuthOptions) UseClientSecret() bool {
	return a.TenantID != "" && a.ClientID != "" && a.ClientSecret != ""
}

func (a AuthOptions) UseAZCLI() bool {
	return a.AZCLI
}

// PrepareKustoConnectionStringBuilder setups the connection string for the Kusto client.
// The authentication method is determined by the provided AuthOptions with the following priority:
//
// - azcli
// - client id/secret
func (a AuthOptions) PrepareKustoConnectionStringBuilder(b *kusto.ConnectionStringBuilder) {
	switch {
	case a.UseAZCLI():
		b.WithAzCli()
	case a.UseClientSecret():
		b.WithAadAppKey(a.ClientID, a.ClientSecret, a.TenantID)
	}
}

func (a AuthOptions) Validate() error {
	if a.UseClientSecret() || a.UseAZCLI() {
		return nil
	}

	return fmt.Errorf("missing required authentication options")
}