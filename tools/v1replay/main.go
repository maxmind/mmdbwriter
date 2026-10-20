// Command v1replay applies a logged sequence of tree operations with
// mmdbwriter v1 and writes the resulting database. The v2 model test uses it
// to compare v1 and v2 behavior. It reads a JSON log on stdin and prints one
// JSON line with the error, if any, of each operation.
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"os"
	"strconv"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)

type replayLog struct {
	IPVersion       int     `json:"ipVersion"`
	DisableAliasing bool    `json:"disableAliasing"`
	IncludeReserved bool    `json:"includeReserved"`
	Inserter        string  `json:"inserter"`
	Output          string  `json:"output"`
	Ops             []logOp `json:"ops"`
}

type logOp struct {
	Kind   string          `json:"kind"`
	Prefix string          `json:"prefix"`
	Start  string          `json:"start"`
	End    string          `json:"end"`
	Func   string          `json:"func"`
	Value  json.RawMessage `json:"value"`
}

type result struct {
	Errors []string `json:"errors"`
	Fatal  string   `json:"fatal,omitempty"`
}

func main() {
	var log replayLog
	if err := json.NewDecoder(os.Stdin).Decode(&log); err != nil {
		exit(fmt.Errorf("reading the log: %w", err))
	}
	res, err := replay(log)
	if err != nil {
		res.Fatal = err.Error()
	}
	if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
		exit(err)
	}
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func replay(log replayLog) (res result, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("v1 panicked: %v", p)
		}
	}()
	opts := mmdbwriter.Options{
		BuildEpoch:              1,
		DatabaseType:            "v1-replay",
		Description:             map[string]string{"en": "v1 replay"},
		IPVersion:               log.IPVersion,
		DisableIPv4Aliasing:     log.DisableAliasing,
		IncludeReservedNetworks: log.IncludeReserved,
		RecordSize:              32,
	}
	if log.Inserter != "" {
		gen, ok := generators[log.Inserter]
		if !ok {
			return res, fmt.Errorf("unknown inserter %q", log.Inserter)
		}
		opts.Inserter = gen
	}
	tree, err := mmdbwriter.New(opts)
	if err != nil {
		return res, err
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("v1 panicked: %v", p)
		}
	}()
	for _, op := range log.Ops {
		res.Errors = append(res.Errors, errString(apply(tree, op)))
	}
	file, err := os.Create(log.Output)
	if err != nil {
		return res, err
	}
	if _, err := tree.WriteTo(file); err != nil {
		file.Close()
		return res, err
	}
	return res, file.Close()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func apply(tree *mmdbwriter.Tree, op logOp) error {
	value, err := decodeValue(op.Value)
	if err != nil {
		return fmt.Errorf("decoding value: %w", err)
	}
	var gen inserter.FuncGenerator
	if op.Func != "" {
		var ok bool
		if gen, ok = generators[op.Func]; !ok {
			return fmt.Errorf("unknown function %q", op.Func)
		}
	}
	switch op.Kind {
	case "insert":
		_, network, err := net.ParseCIDR(op.Prefix)
		if err != nil {
			return err
		}
		if ip4 := network.IP.To4(); ip4 != nil && len(op.Prefix) > 0 && !isIPv6Text(op.Prefix) {
			network.IP = ip4
		}
		if gen == nil {
			return tree.Insert(network, value)
		}
		return tree.InsertFunc(network, gen(value))
	case "insertRange":
		start, end := net.ParseIP(op.Start), net.ParseIP(op.End)
		if gen == nil {
			return tree.InsertRange(start, end, value)
		}
		return tree.InsertRangeFunc(start, end, gen(value))
	}
	return fmt.Errorf("unknown operation %q", op.Kind)
}

func isIPv6Text(s string) bool {
	for _, c := range s {
		if c == ':' {
			return true
		}
	}
	return false
}

var errCallback = errors.New("v1 replay callback failure")

// generators are v1 versions of the v2 model test's inserter functions.
var generators = map[string]inserter.FuncGenerator{
	"Replace": inserter.ReplaceWith,
	"Remove": func(mmdbtype.DataType) inserter.Func {
		return inserter.Remove
	},
	"TopLevelMerge": inserter.TopLevelMergeWith,
	"DeepMerge":     inserter.DeepMergeWith,
	"keepOld": func(mmdbtype.DataType) inserter.Func {
		return func(old mmdbtype.DataType) (mmdbtype.DataType, error) { return old, nil }
	},
	"copyOld": func(mmdbtype.DataType) inserter.Func {
		return func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
			if old == nil {
				return nil, nil
			}
			return old.Copy(), nil
		}
	},
	"wrapOld": func(newValue mmdbtype.DataType) inserter.Func {
		return func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
			if newValue == nil {
				return nil, nil
			}
			if old == nil {
				old = mmdbtype.String("<nil>")
			}
			return mmdbtype.Map{"old": old, "new": newValue}, nil
		}
	},
	"failOnU32_1": func(newValue mmdbtype.DataType) inserter.Func {
		return func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
			if u, ok := old.(mmdbtype.Uint32); ok && u == 1 {
				return nil, errCallback
			}
			return newValue, nil
		}
	},
	"failOnEmpty": func(newValue mmdbtype.DataType) inserter.Func {
		return func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
			if old == nil {
				return nil, errCallback
			}
			return newValue, nil
		}
	},
	"nilOnEqual": func(newValue mmdbtype.DataType) inserter.Func {
		return func(old mmdbtype.DataType) (mmdbtype.DataType, error) {
			if old != nil && newValue != nil && old.Equal(newValue) {
				return nil, nil
			}
			return newValue, nil
		}
	},
}

// decodeValue reads the typed JSON encoding that the v2 model test writes.
func decodeValue(raw json.RawMessage) (mmdbtype.DataType, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var v struct {
		T string                     `json:"t"`
		B string                     `json:"b"`
		N string                     `json:"n"`
		M map[string]json.RawMessage `json:"m"`
		K []string                   `json:"k"`
		S []json.RawMessage          `json:"s"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	switch v.T {
	case "map":
		out := mmdbtype.Map{}
		for i, key := range v.K {
			k, err := base64.StdEncoding.DecodeString(key)
			if err != nil {
				return nil, err
			}
			child, err := decodeValue(v.S[i])
			if err != nil {
				return nil, err
			}
			out[mmdbtype.String(k)] = child
		}
		return out, nil
	case "slice":
		out := mmdbtype.Slice{}
		for _, c := range v.S {
			child, err := decodeValue(c)
			if err != nil {
				return nil, err
			}
			out = append(out, child)
		}
		return out, nil
	case "str", "bytes":
		b, err := base64.StdEncoding.DecodeString(v.B)
		if err != nil {
			return nil, err
		}
		if v.T == "str" {
			return mmdbtype.String(b), nil
		}
		return mmdbtype.Bytes(b), nil
	case "bool":
		return mmdbtype.Bool(v.N == "1"), nil
	case "f64", "f32":
		bits, err := strconv.ParseUint(v.N, 16, 64)
		if err != nil {
			return nil, err
		}
		if v.T == "f32" {
			return mmdbtype.Float32(math.Float32frombits(uint32(bits))), nil
		}
		return mmdbtype.Float64(math.Float64frombits(bits)), nil
	case "u128":
		n, ok := new(big.Int).SetString(v.N, 10)
		if !ok {
			return nil, fmt.Errorf("bad uint128 %q", v.N)
		}
		return (*mmdbtype.Uint128)(n), nil
	}
	n, err := strconv.ParseInt(v.N, 10, 64)
	if err != nil && v.T != "u64" {
		return nil, err
	}
	switch v.T {
	case "u16":
		return mmdbtype.Uint16(n), nil
	case "u32":
		return mmdbtype.Uint32(n), nil
	case "i32":
		return mmdbtype.Int32(n), nil
	case "u64":
		u, err := strconv.ParseUint(v.N, 10, 64)
		if err != nil {
			return nil, err
		}
		return mmdbtype.Uint64(u), nil
	}
	return nil, fmt.Errorf("unknown type %q", v.T)
}
