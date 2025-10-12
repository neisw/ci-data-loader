package cidataloader

import (
	"encoding/json"
	"io"
)

func parseJSONStreamFile(decoder *json.Decoder, processItem func(map[string]any)) error {

	// when we enter here it should be because a `{` delimiter was encountered
	// we want to process until we see another at which point we call this recursively or
	// we find the closing `}`

	// {
	//    "items": [

	key := ""
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return err
		}
		if err != nil {
			return err
		}
		// Process token based on its type
		switch token.(type) {
		case json.Delim:
			// Handle start/end of object or array
			switch token {
			case json.Delim('{'):

			case json.Delim('}'):
				key = ""

			case json.Delim('['):
				if key == "items" {
					// we want to start processing each element in the array
					// and receive a callback when it is complete
					// then we can batch the records up and upload to bq
					// we should not hit EOF before the array ends
					err := parseJSONStreamArrayCallback(decoder, processItem)
					if err != nil {
						if err == io.EOF {
							return err
						}
						return err
					}
					// reset the key
					key = ""
				}
			}
		case string:
			// first value we see should be the key
			// if our key isn't empty and we see another string, treat that as the value
			if len(key) == 0 {
				key = token.(string)
			} else {
				key = ""
			}

		}
	}
}

func parseJSONStreamArrayCallback(decoder *json.Decoder, processItem func(map[string]any)) error {

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return err
		}
		if err != nil {
			return err
		}
		// Process token based on its type
		switch token.(type) {
		case json.Delim:
			// Handle start/end of object or array
			switch token {
			case json.Delim('{'):
				value, err := parseJSONStreamObject(decoder)
				if err != nil {
					if err == io.EOF && value != nil {
						processItem(value)
					}
					return err
				}

				if value != nil {
					processItem(value)
				}

			case json.Delim(']'):
				return nil
			}
		}
	}
}
