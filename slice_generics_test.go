//go:build generate
// +build generate

//go:generate genny -in=$GOFILE -out=gen_$GOFILE gen "NumType=int8,int16,int32,int64,float32,float64"

package opio

import (
	"fmt"
	"testing"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestFixedSliceNumType(t *testing.T) {
	test_slice_NumType := []NumType{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// test correctness
	standard_n, standard_result := EncodeSlice(test_slice_NumType)
	n, result := EncodeSliceNumType(test_slice_NumType)
	zlog.Info().Msgf("standard_result: %x", standard_result)
	assert.Equal(t, standard_n, n, "")
	assert.Equal(t, standard_result, result, "")

	// test speed
	maxSize := 10000 * 1000

	encodeES := time.Now()
	for i := 0; i < maxSize; i++ {
		EncodeSlice(test_slice_NumType)
	}
	fmt.Println("encode slice elapsed: ", time.Since(encodeES))

	encodeES = time.Now()
	for i := 0; i < maxSize; i++ {
		EncodeSliceNumType(test_slice_NumType)
	}
	fmt.Println("encode NumType slice elapsed: ", time.Since(encodeES))
}
