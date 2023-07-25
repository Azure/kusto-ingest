package kusto

import (
	"fmt"

	"github.com/Azure/azure-kusto-go/kusto"
)

func (a AuthOptions) UseClientSecret() bool {
	return a.TenantID != "" && a.ClientID != "" && a.ClientSecret != ""
}

func (a AuthOptions) PrepareKustoConnectionStringBuilder(b *kusto.ConnectionStringBuilder) {
	if a.UseClientSecret() {
		b.WithAadAppKey(a.ClientID, a.ClientSecret, a.TenantID)
	}
}

func (a AuthOptions) Validate() error {
	if a.UseClientSecret() {
		return nil
	}

	return fmt.Errorf("missing required authentication options")
}