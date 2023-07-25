package kusto

func newTestAuth() AuthOptions {
	return AuthOptions{
		TenantID: "test-tenant-id",
		ClientID: "test-client-id",
		ClientSecret: "test-client-secret",
	}
}

func newTestKustoTarget() KustoTargetOptions {
	return KustoTargetOptions{
		Endpoint: "https://example.kusto.windows.net",
		Database: "TestDatabase",
		Table: "TestTable",
	}
}