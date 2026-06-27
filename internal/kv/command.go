package kv

import (
	"encoding/binary"
	"fmt"
)

const (
	OpSet    byte = 1
	OpDelete byte = 2
)
const maxKeyLen = 1 << 16
const maxValLen = 1 << 20

type Command struct {
	Op    byte
	Key   string
	Value []byte
}

func EncodeSet(key string, value []byte) ([]byte, error) {
	return encode(OpSet, key, value)
}

func EncodeDelete(key string) ([]byte, error) {
	return encode(OpDelete, key, nil)
}

func encode(op byte, key string, value []byte) ([]byte, error) {
	kb := []byte(key)

	if len(kb) > maxKeyLen {
		return nil, fmt.Errorf("kv: key too large")
	}

	if op == OpSet && len(value) > maxValLen {
		return nil, fmt.Errorf("kv: value too large")
	}

	// |op(1)|keyLen u32|key|valLen u32|val|
	size := 1 + 4 + len(kb)
	if op == OpSet {
		size += 4 + len(value)
	}

	buf := make([]byte, size)
	buf[0] = op

	binary.BigEndian.PutUint32(buf[1:], uint32(len(kb)))
	copy(buf[5:], kb)

	if op == OpSet {
		off := 5 + len(kb)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(value)))
		copy(buf[off+4:], value)
	}

	return buf, nil
}

func Decode(data []byte) (Command, error) {
	if len(data) < 5 {
		return Command{}, fmt.Errorf("kv: command too short")
	}

	op := data[0]
	keyLen := binary.BigEndian.Uint32(data[1:5])

	if int(keyLen) > maxKeyLen {
		return Command{}, fmt.Errorf("kv: key too large")
	}

	if len(data) < 5+int(keyLen) {
		return Command{}, fmt.Errorf("kv: truncated key")
	}

	key := string(data[5 : 5+keyLen])
	cmd := Command{Op: op, Key: key}

	switch op {
	case OpDelete:
		if len(data) != 5+int(keyLen) {
			return Command{}, fmt.Errorf("kv: delete has trailing bytes")
		}
		return cmd, nil
	case OpSet:
		off := 5 + int(keyLen)

		if len(data) < off+4 {
			return Command{}, fmt.Errorf("kv: truncated value length")
		}

		valLen := binary.BigEndian.Uint32(data[off : off+4])

		if int(valLen) > maxValLen {
			return Command{}, fmt.Errorf("kv: value too large")
		}

		off += 4

		if len(data) != off+int(valLen) {
			return Command{}, fmt.Errorf("kv: truncated value")
		}

		cmd.Value = data[off : off+int(valLen)]

		return cmd, nil
	default:
		return Command{}, fmt.Errorf("kv: unknown op %d", op)
	}
}
