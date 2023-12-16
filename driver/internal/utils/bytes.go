package utils

import (
	"crypto/sha1"
	"math"
	"time"
)

type BytesBase struct {
	headLen int
	bodyLen int
	data    []byte
}

func (base *BytesBase) SetData(data []byte) {
	dataLen := len(data)
	if 0 == dataLen {
		return
	}

	// 按照L(length)V(value)约定分析出headLen
	bodyLen := 0
	headLen := 0
	lenCode := uint8(data[0])
	headLen++
	switch lenCode {
	case mpBin8:
		bodyLen = int(data[1])
		headLen++
	case mpBin16:
		bodyLen = int(data[1])<<8 | int(data[2])
		headLen += 2
	case mpBin32:
		bodyLen = int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])
		headLen += 4
	}
	base.headLen = headLen
	base.bodyLen = bodyLen
	base.data = data
}

func (base *BytesBase) GetBody() []byte {
	data := base.data
	if 0 == len(data) {
		return nil
	}
	headLen := base.headLen
	return data[headLen:]
}

func (base *BytesBase) GetBodyLen() int {
	return base.bodyLen
}

func (base *BytesBase) GetData() []byte {
	return base.data
}

func (base *BytesBase) GetDataLen() int {
	return len(base.data)
}

func (base *BytesBase) GetHead() []byte {
	data := base.data
	if 0 == len(data) {
		return nil
	}
	headLen := base.headLen
	return data[:headLen]
}

func (base *BytesBase) GetHeadLen() int {
	return base.headLen
}

func PutBool(b []byte, v bool) {
	if v {
		b[0] = byte(1)
	} else {
		b[0] = byte(0)
	}
}

// PutInt8 -
func PutInt8(b []byte, i int8) {
	b[0] = byte(i)
}

func PutUint8(b []byte, i uint8) {
	b[0] = byte(i)
}

// PutInt16 -
func PutInt16(b []byte, i int16) {
	b[0] = byte(i >> 8)
	b[1] = byte(i)
}

func PutUint16(b []byte, i uint16) {
	b[0] = byte(i >> 8)
	b[1] = byte(i)
}

// PutInt32 -
func PutInt32(b []byte, i int32) {
	b[0] = byte(i >> 24)
	b[1] = byte(i >> 16)
	b[2] = byte(i >> 8)
	b[3] = byte(i)
}

func PutUint32(b []byte, i uint32) {
	b[0] = byte(i >> 24)
	b[1] = byte(i >> 16)
	b[2] = byte(i >> 8)
	b[3] = byte(i)
}

// PutInt64 -
func PutInt64(b []byte, i int64) {
	b[0] = byte(i >> 56)
	b[1] = byte(i >> 48)
	b[2] = byte(i >> 40)
	b[3] = byte(i >> 32)
	b[4] = byte(i >> 24)
	b[5] = byte(i >> 16)
	b[6] = byte(i >> 8)
	b[7] = byte(i)
}

func PutUint64(b []byte, i uint64) {
	b[0] = byte(i >> 56)
	b[1] = byte(i >> 48)
	b[2] = byte(i >> 40)
	b[3] = byte(i >> 32)
	b[4] = byte(i >> 24)
	b[5] = byte(i >> 16)
	b[6] = byte(i >> 8)
	b[7] = byte(i)
}

// PutFloat32 -
func PutFloat32(b []byte, v float32) {
	i := math.Float32bits(v)
	b[0] = byte(i >> 24)
	b[1] = byte(i >> 16)
	b[2] = byte(i >> 8)
	b[3] = byte(i)
}

// PutDateTime -
func PutDateTime(buf []byte, v time.Time) {
	i := math.Float64bits(float64(v.UnixNano()/1e6) / 1e3)
	buf[0] = byte(i >> 56)
	buf[1] = byte(i >> 48)
	buf[2] = byte(i >> 40)
	buf[3] = byte(i >> 32)
	buf[4] = byte(i >> 24)
	buf[5] = byte(i >> 16)
	buf[6] = byte(i >> 8)
	buf[7] = byte(i)
}

// PutFloat64 -
func PutFloat64(buf []byte, v float64) {
	i := math.Float64bits(v)
	buf[0] = byte(i >> 56)
	buf[1] = byte(i >> 48)
	buf[2] = byte(i >> 40)
	buf[3] = byte(i >> 32)
	buf[4] = byte(i >> 24)
	buf[5] = byte(i >> 16)
	buf[6] = byte(i >> 8)
	buf[7] = byte(i)
}

// PutString -
func PutString(value string) (int, []byte) {
	var headLen = 2
	var rawData []byte = nil

	valueLen := len(value)
	switch {
	case valueLen < 0x100:
		rawData = make([]byte, valueLen+headLen)
		rawData[0] = mpBin8
		rawData[1] = byte(valueLen)

	case valueLen < 0x10000:
		headLen = 3
		rawData = make([]byte, valueLen+headLen)
		rawData[0] = mpBin16
		rawData[1] = byte(valueLen >> 8)
		rawData[2] = byte(valueLen)

	case valueLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, valueLen+headLen)
		rawData[0] = mpBin32
		rawData[1] = byte(valueLen >> 24)
		rawData[2] = byte(valueLen >> 16)
		rawData[3] = byte(valueLen >> 8)
		rawData[4] = byte(valueLen)
	}
	copy(rawData[headLen:], value)

	return headLen, rawData
}

// PutBinary -
func PutBinary(value []byte) (int, []byte) {
	var headLen = 2
	var rawData []byte

	valueLen := len(value)
	switch {
	case valueLen < 0x100:
		rawData = make([]byte, valueLen+headLen)
		rawData[0] = mpBin8
		rawData[1] = byte(valueLen)

	case valueLen < 0x10000:
		headLen = 3
		rawData = make([]byte, valueLen+headLen)
		rawData[0] = mpBin16
		rawData[1] = byte(valueLen >> 8)
		rawData[2] = byte(valueLen)

	case valueLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, valueLen+headLen)
		rawData[0] = mpBin32
		rawData[1] = byte(valueLen >> 24)
		rawData[2] = byte(valueLen >> 16)
		rawData[3] = byte(valueLen >> 8)
		rawData[4] = byte(valueLen)
	}
	copy(rawData[headLen:], value)

	return headLen, rawData
}

// GetInt8 -
func GetBool(b []byte) bool {
	return b[0] > 0
}

// GetInt8 -
func GetInt8(b []byte) int8 {
	return int8(b[0])
}

// GetInt16 -
func GetInt16(b []byte) int16 {
	return int16(b[0])<<8 | int16(b[1])
}

// GetInt32 -
func GetInt32(b []byte) int32 {
	return int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
}

// GetInt64 -
func GetInt64(b []byte) int64 {
	return int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 | int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
}

// GetUInt8 -
func GetUInt8(b []byte) uint8 {
	return uint8(b[0])
}

// GetUInt16 -
func GetUInt16(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

// GetUInt32 -
func GetUInt32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// GetUInt64 -
// noinspection GoUnusedExportedFunction
func GetUInt64(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 | uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// GetFloat32 -
func GetFloat32(b []byte) float32 {
	return math.Float32frombits(uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]))
}

// GetFloat64 -
func GetFloat64(b []byte) float64 {
	return math.Float64frombits(uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 | uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]))
}

// GetFloat64 -
func GetDateTime(b []byte) time.Time {
	timeValue := math.Float64frombits(uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 | uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]))
	return Float2DateTime(timeValue)
}

// Float2DateTime -
func Float2DateTime(b float64) time.Time {
	sec := int64(b)
	nsec := int64(b*1e3) % 1000 * 1e6
	return time.Unix(sec, nsec)
}

// GetString -
func GetString(rawData []byte) string {

	offset := 0
	lenCode := rawData[offset]
	offset++

	switch lenCode {
	case mpBin8:
		offset++
	case mpBin16:
		offset += 2
	case mpBin32:
		offset += 4
	}
	return string(rawData[offset:])
}

// GetBytes -
// noinspection GoUnusedExportedFunction
func GetBytes(cells uint32, rawData []byte) []byte {
	v_index := 0
	v_len := 0
	//计算所需获取数据的偏差
	for i := uint32(0); i <= cells; i++ {
		v_type := rawData[v_index]
		v_index += 1
		if v_type == mpBin8 {
			v_len = int(GetInt8(rawData[v_index : v_index+1]))
			v_index += 1
		} else if v_type == mpBin16 {
			v_len = int(GetInt16(rawData[v_index : v_index+2]))
			v_index += 2
		} else if v_type == mpBin32 {
			v_len = int(GetInt32(rawData[v_index : v_index+4]))
			v_index += 4
		}
		if v_len > 0 {
			v_index += v_len
		}
	}
	return rawData[v_index-v_len : v_index]
}

// Memset - fast
func Memset(b []byte, v byte) {
	if len(b) == 0 {
		return
	}
	b[0] = v
	for i := 1; i < len(b); i *= 2 {
		copy(b[i:], b[:i])
	}
}

// Scramle - scramle login password
func Scramle(message []byte, pass []byte) []byte {
	stage1 := sha1.Sum(pass)
	stage2 := sha1.Sum(stage1[0:])

	s := sha1.New()
	s.Write(message)
	s.Write(stage2[0:])
	stage3 := s.Sum(nil)
	for i := 0; i < sha1.Size; i++ {
		stage3[i] ^= stage1[i]
	}
	return stage3
}
