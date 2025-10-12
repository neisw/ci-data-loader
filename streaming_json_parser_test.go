package cidataloader

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

//go:embed e2e-events_sample.json
var test_intervals string

func Test_StreamingParser(t *testing.T) {
	reader := strings.NewReader(test_intervals)

	decoder := json.NewDecoder(reader)

	// we hit our first delim, then items and the start of the array
	// then we want each complete item including the opening delimiter
	// Delim: "{"
	//Content: "items"
	//Delim: "["

	value, err := parseJSONStreamObject(decoder)
	if err != nil && err != io.EOF {
		fmt.Printf("Error parsing JSON stream: %v\n", err)
	} else if value != nil {
		fmt.Printf("JSON stream: %v\n", value)
	}
}
