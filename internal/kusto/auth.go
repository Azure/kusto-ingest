package kusto

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/charmbracelet/log"
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
//
// When a logger is provided, token acquisition is wrapped with diagnostic logging
// that decodes and prints JWT claims (audience, tenant, object ID, expiry, etc.)
// and measures token acquisition latency.
func (a AuthOptions) PrepareKustoConnectionStringBuilder(b *kusto.ConnectionStringBuilder, logger *log.Logger) error {
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

		if logger != nil {
			logger.Info("using managed identity credential",
				"resourceID", a.ManagedIdentityResourceID,
			)
			b.WithTokenCredential(&loggingTokenCredential{inner: cred, logger: logger})
		} else {
			b.WithTokenCredential(cred)
		}
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

// loggingTokenCredential wraps an azcore.TokenCredential and logs token details
// for diagnostic purposes. It decodes JWT claims to help debug authentication
// issues such as wrong audience or tenant in cross-tenant MSI scenarios.
// It tracks attempt counts and latency to detect timing issues.
type loggingTokenCredential struct {
	inner        azcore.TokenCredential
	logger       *log.Logger
	attemptCount atomic.Int64
}

func (l *loggingTokenCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	attempt := l.attemptCount.Add(1)

	l.logger.Info("acquiring token",
		"attempt", attempt,
		"scopes", options.Scopes,
		"tenantID", options.TenantID,
	)

	start := time.Now()
	token, err := l.inner.GetToken(ctx, options)
	duration := time.Since(start)

	if err != nil {
		l.logger.Error("token acquisition FAILED",
			"attempt", attempt,
			"error", err,
			"duration", duration,
			"scopes", options.Scopes,
			"tenantID", options.TenantID,
		)
		return token, err
	}

	l.logger.Info("token acquired successfully",
		"attempt", attempt,
		"duration", duration,
		"expiresOn", token.ExpiresOn,
	)

	// Decode JWT claims for diagnostic logging
	if claims, decodeErr := decodeJWTClaims(token.Token); decodeErr == nil {
		l.logger.Info("token claims",
			"attempt", attempt,
			"aud", claims["aud"],
			"iss", claims["iss"],
			"tid", claims["tid"],
			"oid", claims["oid"],
			"sub", claims["sub"],
			"appid", claims["appid"],
			"appidacr", claims["appidacr"],
			"exp", formatUnixTime(claims["exp"]),
			"iat", formatUnixTime(claims["iat"]),
			"nbf", formatUnixTime(claims["nbf"]),
		)
	} else {
		l.logger.Warn("could not decode token claims for diagnostics", "error", decodeErr)
	}

	return token, nil
}

// decodeJWTClaims extracts claims from a JWT token without signature verification.
// This is for diagnostic logging only — the token is still validated by the server.
func decodeJWTClaims(token string) (map[string]interface{}, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT: expected 3 parts, got %d", len(parts))
	}

	// Decode the payload (second part) using raw URL encoding (no padding)
	decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("base64 decode payload: %w", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, fmt.Errorf("json unmarshal claims: %w", err)
	}

	return claims, nil
}

// formatUnixTime converts a JSON number (unix timestamp) to a human-readable time string.
func formatUnixTime(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	if f, ok := v.(float64); ok {
		return time.Unix(int64(f), 0).UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("%v", v)
}
