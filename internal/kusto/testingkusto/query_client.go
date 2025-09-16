package testingkusto

import (
	"context"
	"net/http"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
)

// QueryClient is a fake implementation of ingest.QueryClient for tests.
type QueryClient struct {
	MgmtFn  func(ctx context.Context, db string, query kusto.Statement, options ...kusto.MgmtOption) (*kusto.RowIterator, error)
	QueryFn func(ctx context.Context, db string, query kusto.Statement, options ...kusto.QueryOption) (*kusto.RowIterator, error)
	CloseFn func() error

	// Captured calls for assertions.
	MgmtCalls  []MgmtCall
	QueryCalls []QueryCall
}

// MgmtCall captures a Mgmt invocation.
type MgmtCall struct {
	DB    string
	Query kusto.Statement
}

// QueryCall captures a Query invocation.
type QueryCall struct {
	DB    string
	Query kusto.Statement
}

var _ ingest.QueryClient = (*QueryClient)(nil)

func (qc *QueryClient) Close() error {
	if qc.CloseFn != nil {
		return qc.CloseFn()
	}
	return nil
}

func (qc *QueryClient) Auth() kusto.Authorization { return kusto.Authorization{} }
func (qc *QueryClient) Endpoint() string          { return "https://example.kusto.windows.net" }

func (qc *QueryClient) Query(ctx context.Context, db string, query kusto.Statement, options ...kusto.QueryOption) (*kusto.RowIterator, error) {
	qc.QueryCalls = append(qc.QueryCalls, QueryCall{DB: db, Query: query})
	if qc.QueryFn != nil {
		return qc.QueryFn(ctx, db, query, options...)
	}
	return nil, nil
}

func (qc *QueryClient) Mgmt(ctx context.Context, db string, query kusto.Statement, options ...kusto.MgmtOption) (*kusto.RowIterator, error) {
	qc.MgmtCalls = append(qc.MgmtCalls, MgmtCall{DB: db, Query: query})
	if qc.MgmtFn != nil {
		return qc.MgmtFn(ctx, db, query, options...)
	}
	return nil, nil
}

func (qc *QueryClient) HttpClient() *http.Client            { return nil }
func (qc *QueryClient) ClientDetails() *kusto.ClientDetails { return &kusto.ClientDetails{} }

// NewQueryClient creates a QueryClient with optional mutators.
func NewQueryClient(ms ...func(*QueryClient)) *QueryClient {
	qc := &QueryClient{}
	for _, m := range ms {
		m(qc)
	}
	return qc
}
