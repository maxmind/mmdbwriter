package mmdbwriter

import (
	"encoding/hex"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func FuzzTreeModel(f *testing.F) {
	for b := range 32 {
		f.Add([]byte{byte(b), 0, 0, 3, 0, 0, 10, 1, 1})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := runTreeModel(data, t.TempDir(), false); err != nil {
			t.Fatal(err)
		}
	})
}

// TestTreeModelRandom runs random operation sequences on every test run.
func TestTreeModelRandom(t *testing.T) {
	strict := os.Getenv("MMDBWRITER_MODEL_STRICT") != ""
	runRandomInputs(t, 200, func(data []byte, dir string) error {
		return runTreeModel(data, dir, strict)
	})
}

func FuzzValueModel(f *testing.F) {
	for b := range 16 {
		f.Add([]byte{byte(b * 16), 0, 1, 2, 3})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := runValueModel(data, t.TempDir()); err != nil {
			t.Fatal(err)
		}
	})
}

// TestValueModelRandom checks random values on every test run.
func TestValueModelRandom(t *testing.T) {
	runRandomInputs(t, 200, runValueModel)
}

// runRandomInputs checks a property against random inputs and shrinks the
// first failing input. MMDBWRITER_MODEL_SEED replays a seed,
// MMDBWRITER_MODEL_RUNS sets the number of inputs, and
// MMDBWRITER_MODEL_NOSHRINK reports a failure without shrinking it.
func runRandomInputs(t *testing.T, runs int, property func(data []byte, dir string) error) {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("MMDBWRITER_MODEL_SEED"); s != "" {
		var err error
		seed, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	if s := os.Getenv("MMDBWRITER_MODEL_RUNS"); s != "" {
		var err error
		runs, err = strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewPCG(seed, 0))
	dir := t.TempDir()
	for run := range runs {
		data := make([]byte, 32+rng.IntN(1500))
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		err := property(data, dir)
		if err == nil {
			continue
		}
		if os.Getenv("MMDBWRITER_MODEL_NOSHRINK") != "" {
			t.Fatalf("seed %d run %d failed\n%v", seed, run, err)
		}
		tag := failureTag(err)
		minimal := shrinkModelInput(data, func(candidate []byte) bool {
			err := property(candidate, dir)
			return err != nil && failureTag(err) == tag
		})
		t.Fatalf(
			"seed %d run %d failed\nminimized input %s\n%v",
			seed, run, hex.EncodeToString(minimal), property(minimal, dir),
		)
	}
}

func failureTag(err error) string {
	tag, _, _ := strings.Cut(err.Error(), "]")
	return tag
}

// shrinkModelInput removes chunks of input, then lowers bytes, while the
// failure keeps its tag.
func shrinkModelInput(data []byte, fails func([]byte) bool) []byte {
	data = slices.Clone(data)
	for changed := true; changed; {
		changed = false
		for chunk := len(data) / 2; chunk >= 1; chunk /= 2 {
			for i := 0; i+chunk <= len(data); {
				candidate := slices.Concat(data[:i], data[i+chunk:])
				if fails(candidate) {
					data = candidate
					changed = true
				} else {
					i += chunk
				}
			}
		}
		for i := range data {
			for _, v := range []byte{0, data[i] / 2, data[i] - 1} {
				if v >= data[i] {
					continue
				}
				candidate := slices.Clone(data)
				candidate[i] = v
				if fails(candidate) {
					data = candidate
					changed = true
					break
				}
			}
		}
	}
	return data
}

func TestTreeModelReplay(t *testing.T) {
	input := os.Getenv("MMDBWRITER_MODEL_INPUT")
	if input == "" {
		t.Skip("set MMDBWRITER_MODEL_INPUT to a hex input")
	}
	data, err := hex.DecodeString(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := runTreeModel(data, t.TempDir(), os.Getenv("MMDBWRITER_MODEL_STRICT") != ""); err != nil {
		t.Fatal(err)
	}
}
