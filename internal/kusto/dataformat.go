package kusto

import (
	"fmt"
	"strings"

	"github.com/Azure/azure-kusto-go/kusto/ingest"
)

// TODO: discuss with upstream to let ingest.DataFormat implement UnmarshalJSON.
var supportedIngestDataFormatsByString = func() map[DataFormatString]ingest.DataFormat {
	ss := []ingest.DataFormat{
		ingest.JSON,
		ingest.CSV,
	}

	rv := map[DataFormatString]ingest.DataFormat{}
	for _, s := range ss {
		rv[DataFormatString(s.String())] = s
	}

	return rv
}()

var supportedIngestDataFormatsHint = func() string {
	var ss []string
	for n := range supportedIngestDataFormatsByString {
		ss = append(ss, string(n))
	}

	return strings.Join(ss, ", ")
}()

type DataFormatString string

func (d DataFormatString) Validate() error {
	if _, ok := supportedIngestDataFormatsByString[d]; !ok {
		return fmt.Errorf("unsupported data format: %q, supported: %s", d, supportedIngestDataFormatsHint)
	}

	return nil
}

func (d DataFormatString) ToIngestDataFormat() ingest.DataFormat {
	return supportedIngestDataFormatsByString[d]
}