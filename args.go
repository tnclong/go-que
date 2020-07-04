package que

import (
	"bytes"
	"encoding/json"
	"errors"
)

var emptyArgs = []byte{'[', ']'}

// Args encodes args to json array.
//    plan := Plan{Args: Args(1,2,3)}
func Args(args ...interface{}) []byte {
	if len(args) == 0 {
		return emptyArgs
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(args); err != nil {
		panic(err)
	}
	data := buf.Bytes()
	return data[:len(data)-1]
}

// ParseArgs parse jsonArr and assign array element value to each args
// in order and returns count of args assign a value.
// Ignore redundant jsonArr element.
func ParseArgs(jsonArr []byte, args ...interface{}) (count int, err error) {
	d := json.NewDecoder(bytes.NewReader(jsonArr))
	t, err := d.Token()
	if t != json.Delim('[') {
		return 0, errors.New("jsonArr must be a JSON array")
	}
	if err != nil {
		return 0, err
	}

	for _, arg := range args {
		if !d.More() {
			break
		}

		if err := d.Decode(arg); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
