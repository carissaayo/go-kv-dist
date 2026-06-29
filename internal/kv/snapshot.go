package kv

import (
	"bytes"
	"encoding/gob"
)

// State is the KV map at a given raft apply index.
type State struct {
	Data map[string][]byte
}

func EncodeState(data map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(State{Data: data}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func DecodeState(b []byte) (map[string][]byte, error) {
	var st State
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&st); err != nil {
		return nil, err
	}
	return st.Data, nil
}
