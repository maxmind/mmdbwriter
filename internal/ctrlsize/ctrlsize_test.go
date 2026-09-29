package ctrlsize

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplit(t *testing.T) {
	for _, test := range []struct {
		size       int
		bits       byte
		extra      int
		extraBytes int
	}{
		{0, 0, 0, 0},
		{28, 28, 0, 0},
		{29, 29, 0, 1},
		{284, 29, 255, 1},
		{285, 30, 0, 2},
		{65820, 30, 65535, 2},
		{65821, 31, 0, 3},
		{16843036, 31, 1<<24 - 1, 3},
	} {
		bits, extra, extraBytes := Split(test.size)
		assert.Equal(t, test.bits, bits, "size %d", test.size)
		assert.Equal(t, test.extra, extra, "size %d", test.size)
		assert.Equal(t, test.extraBytes, extraBytes, "size %d", test.size)
	}
	assert.Equal(t, 16843036, Max)
}
