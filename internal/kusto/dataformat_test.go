package kusto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_DataFormatString_Validate(t *testing.T) {
	for v := range supportedIngestDataFormatsByString {
		err := v.Validate()
		assert.NoError(t, err, "%q should be valid data format", v)
	}

	invalid := []DataFormatString{
		"foo", "", "bar", "arvo",
	}
	for _, v := range invalid {
		err := v.Validate()
		assert.Error(t, err, "%q should be invalid data format", v)
	}
}

func Test_DataFormatString_ToIngestDataFormat(t *testing.T) {
	for v := range supportedIngestDataFormatsByString {
		assert.Equal(t, supportedIngestDataFormatsByString[v], v.ToIngestDataFormat(), "%q should be valid data format", v)
	}
}