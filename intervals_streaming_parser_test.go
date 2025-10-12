package cidataloader

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

//go:embed e2e-events_sample.json
var testStreamingIntervals string

func Test_IntervalsStreamingParser(t *testing.T) {
	reader := strings.NewReader(testStreamingIntervals)

	decoder := json.NewDecoder(reader)

	// we hit our first delim, then items and the start of the array
	// then we want each complete item including the opening delimiter
	// Delim: "{"
	//Content: "items"
	//Delim: "["

	elements := make([]map[string]any, 0)
	err := parseJSONStreamFile(decoder, func(m map[string]any) {
		elements = append(elements, m)

		co, _ := parseComplexRow(m, "testJobRun", "testSource")
		if co == nil {
			fmt.Printf("Invalid complex object for JSON stream object: %v\n", m)
		}
	})
	if err != nil && err != io.EOF {
		fmt.Printf("Error parsing JSON stream: %v\n", err)
	}

	intervals := map[string]any{}
	intervals["items"] = elements

	processed, err := json.Marshal(intervals)
	if err != nil {
		fmt.Printf("Error encoding JSON stream: %v\n", err)
	}

	var result map[string]any
	err = json.Unmarshal([]byte(testStreamingIntervals), &result)
	if err != nil {
		fmt.Printf("Error encoding JSON string: %v\n", err)
	}

	stringBytes, err := json.Marshal(intervals)
	if err != nil {
		fmt.Printf("Error encoding JSON stream: %v\n", err)
	}

	assert.Equal(t, stringBytes, processed)

}
