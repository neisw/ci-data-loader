package cidataloader

import (
	"encoding/json"
	"fmt"
	"io"
)

func parseJSONStreamArray(decoder *json.Decoder) ([]any, error) {

	result := make([]any, 0)

	for {
		var value any
		token, err := decoder.Token()
		if err == io.EOF {
			return result, err
		}
		if err != nil {
			return nil, err
		}
		// Process token based on its type
		switch token.(type) {
		case json.Delim:
			// Handle start/end of object or array
			switch token {
			case json.Delim('{'):
				value, err = parseJSONStreamObject(decoder)
				if err != nil {
					if err == io.EOF {
						if value != nil {
							result = append(result, value)
						}

						return result, err
					}
					return nil, err
				}

				if value != nil {
					result = append(result, value)
				}

			case json.Delim(']'):
				return result, nil
			}
		}
	}
}

func parseJSONStreamObject(decoder *json.Decoder) (map[string]any, error) {
	// when we enter here it should be because a `{` delimiter was encountered
	// we want to process until we see another at which point we call this recursively or
	// we find the closing `}`
	result := make(map[string]any)
	var key string
	for {

		var value any

		token, err := decoder.Token()
		if err == io.EOF {
			return result, err
		}
		if err != nil {
			return nil, err
		}
		// Process token based on its type
		switch token.(type) {
		case json.Delim:
			// Handle start/end of object or array
			switch token {
			case json.Delim('{'):
				value, err = parseJSONStreamObject(decoder)
				if err != nil {
					if err == io.EOF {
						if value != nil {
							if key != "" {
								result[key] = value
							}
						}
						if len(result) > 0 {
							return result, err
						}
					}
					return nil, err
				}

				if value != nil && key != "" {
					result[key] = value
				}
				// reset the key
				key = ""

			case json.Delim('}'):
				return result, nil

			case json.Delim('['):
				value, err = parseJSONStreamArray(decoder)
				if err != nil {
					if err == io.EOF {
						if value != nil && key != "" {
							result[key] = value
						}
						return result, err
					}
					return nil, err
				}

				if value != nil && key != "" {
					result[key] = value
				}
				// reset the key
				key = ""
			}
		case string:
			if key == "" {
				key = token.(string)
			} else {
				result[key] = token
				key = ""
			}
		default:
			if key != "" {
				result[key] = token
				key = ""
			} else {
				fmt.Printf("Error: no Key for content: %v\n", token)
			}
		}
	}
}
