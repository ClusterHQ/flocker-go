package flocker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatasetIDFromName(t *testing.T) {
	cases := map[string]string{
		"a6c0426f-f9a3-4b59-a62f-4807c382b768": "hello",
		"90a388bf-dfca-490f-812a-67aec0cd9f09": "hello\1234",
	}

	for expected, input := range cases {
		datasetID := datasetIDFromName(input)
		failureMsg := fmt.Sprintf("Expecting '%s' to be hashed to '%s', got: '%s'", input, expected, datasetID)
		assert.Equal(t, expected, datasetID, failureMsg)
	}
}
