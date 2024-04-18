package kusto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthOptions_Validate(t *testing.T) {
	cases := []struct{
		name string
		opts AuthOptions
		expectedErr bool
	}{
		{
			name: "no auth",
			opts: AuthOptions{},
			expectedErr: true,
		},
		{
			name: "azcli",
			opts: AuthOptions{AZCLI: true},
			expectedErr: false,
		},
		{
			name: "client id/secret",
			opts: AuthOptions{
				TenantID: "tenant-id",
				ClientID: "client-id",
				ClientSecret: "client-secret",
			},
			expectedErr: false,
		},
		{
			name: "partial client id/secret",
			opts: AuthOptions{
				ClientID: "client-id",
				ClientSecret: "client-secret",
			},
			expectedErr: true,
		},
		{
			name: "azcli and client id/secret",
			opts: AuthOptions{
				AZCLI: true,
				TenantID: "tenant-id",
				ClientID: "client-id",
				ClientSecret: "client-secret",
			},
			expectedErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.opts.Validate()
			if c.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}