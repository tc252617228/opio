package utils

import (
	"errors"
	"time"
)

const maxLengthSize = 65535

type MBuffer struct {
	buf       []byte   // contents are the bytes buf[off : len(buf)]
	off       int      // read at &buf[off], write at &buf[len(buf)]
	bootstrap [64]byte // memory to hold first slice; helps small buffers avoid allocation.
}

var ErrTooLarge = errors.New("bytes.MBuffer: too large")

const maxInt = int(^uint(0) >> 1)

func (this *MBuffer) Bytes() []byte { return this.buf[this.off:] }

func (this *MBuffer) empty() bool { return len(this.buf) <= this.off }

func (this *MBuffer) Len() int { return len(this.buf) - this.off }

func (this *MBuffer) Cap() int { return cap(this.buf) }

func (this *MBuffer) Reset() {
	this.buf = this.buf[:0]
	this.off = 0
}

func (this *MBuffer) tryGrowByReslice(n int) (int, bool) {
	if l := len(this.buf); n <= cap(this.buf)-l {
		this.buf = this.buf[:l+n]
		return l, true
	}
	return 0, false
}

func (this *MBuffer) grow(n int) int {
	m := this.Len()
	// If buffer is empty, reset to recover space.
	if m == 0 && this.off != 0 {
		this.Reset()
	}
	// Try to grow by means of a reslice.
	if i, ok := this.tryGrowByReslice(n); ok {
		return i
	}
	// Check if we can make use of bootstrap array.
	if this.buf == nil && n <= len(this.bootstrap) {
		this.buf = this.bootstrap[:n]
		return 0
	}
	c := cap(this.buf)
	if n <= c/2-m {
		// We can slide things down instead of allocating a new
		// slice. We only need m+n <= c to slide, but
		// we instead let capacity get twice as large so we
		// don't spend all our time copying.
		copy(this.buf, this.buf[this.off:])
	} else if c > maxInt-c-n {
		panic(ErrTooLarge)
	} else {
		// Not enough space anywhere, we need to allocate.
		buf := makeSlice(2*c + n)
		copy(buf, this.buf[this.off:])
		this.buf = buf
	}
	// Restore this.off and len(this.buf).
	this.off = 0
	this.buf = this.buf[:m+n]
	return m
}

func (this *MBuffer) WriteNull() (n int, err error) {
	p := PackNull()
	return this.Write(p)
}

func (this *MBuffer) WriteBool(v bool) (n int, err error) {
	p := PackBool(v)
	return this.Write(p)
}

func (this *MBuffer) WriteInt8(v int8) (n int, err error) {
	p := PackInt8(v)
	return this.Write(p)
}

func (this *MBuffer) WriteInt16(v int16) (n int, err error) {
	p := PackInt16(v)
	return this.Write(p)
}

func (this *MBuffer) WriteInt32(v int32) (n int, err error) {
	p := PackInt32(v)
	return this.Write(p)
}

func (this *MBuffer) WriteInt64(v int64) (n int, err error) {
	p := PackInt64(v)
	return this.Write(p)
}

func (this *MBuffer) WriteFloat(v float32) (n int, err error) {
	p := PackFloat(v)
	return this.Write(p)
}

func (this *MBuffer) WriteDouble(v float64) (n int, err error) {
	p := PackDouble(v)
	return this.Write(p)
}

func (this *MBuffer) WriteDateTime(v time.Time) (n int, err error) {
	p := PackDateTime(DateTimeToDouble(v))
	return this.Write(p)
}

func (this *MBuffer) WriteDateTimeToDouble(v float64) (n int, err error) {
	p := PackDateTime(v)
	return this.Write(p)
}

func (this *MBuffer) WriteString(s string) (n int, err error) {
	data := []byte(s)
	len_v := len(data)
	if len_v <= maxLengthSize {
		_, _ = this.Write([]byte{type_string_code, byte((len_v >> 8) & mask), byte(len_v & mask)})
	} else {
		return 0, errors.New("Data Length exceeding maximum limit")
	}
	return this.Write(data)
}

func (this *MBuffer) WriteBinary(v []byte) (n int, err error) {
	len_v := len(v)
	if len_v <= maxLengthSize {
		_, _ = this.Write([]byte{type_binary_code, byte((len_v >> 8) & mask), byte(len_v & mask)})
	} else {
		return 0, errors.New("Data Length exceeding maximum limit")
	}
	return this.Write(v)
}

func (this *MBuffer) Write(p []byte) (n int, err error) {
	m, ok := this.tryGrowByReslice(len(p))
	if !ok {
		m = this.grow(len(p))
	}
	return copy(this.buf[m:], p), nil
}

func makeSlice(n int) []byte {

	defer func() {
		if recover() != nil {
			panic(ErrTooLarge)
		}
	}()
	return make([]byte, n)
}
