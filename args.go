package que

import (
	"bytes"
	"encoding/json"
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
