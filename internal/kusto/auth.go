package kusto

import (
	"fmt"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

func (a AuthOptions) UseClientSecret() bool {
	return a.TenantID != "" && a.ClientID != "" && a.ClientSecret != ""
}

func (a AuthOptions) UseAZCLI() bool {
	return a.AZCLI
}

func (a AuthOptions) UseManagedIdentity() bool {
	return a.ManagedIdentityResourceID != ""
}

// PrepareKustoConnectionStringBuilder setups the connection string for the Kusto client.
// The authentication method is determined by the provided AuthOptions with the following priority:
//
// - azcli
// - managed identity
// - client id/secret
func (a AuthOptions) PrepareKustoConnectionStringBuilder(b *kusto.ConnectionStringBuilder) error {
	switch {
	case a.UseAZCLI():
		b.WithAzCli()
	case a.UseManagedIdentity():
		// NOTE: kusto library doesn't support passing in a resource ID for managed identity.
		// So we create the credential ourselves and pass it in.
		cred, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
			ID: azidentity.ResourceID(a.ManagedIdentityResourceID),
		})
		if err != nil {
			return fmt.Errorf("creating managed identity credential: %w", err)
		}
		b.WithTokenCredential(cred)
	case a.UseClientSecret():
		b.WithAadAppKey(a.ClientID, a.ClientSecret, a.TenantID)
	}

	return nil
}

func (a AuthOptions) Validate() error {
	if a.UseClientSecret() || a.UseManagedIdentity() || a.UseAZCLI() {
		return nil
	}

	return fmt.Errorf("missing required authentication options")
}
