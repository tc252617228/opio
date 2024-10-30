package utils

import (
	"math"
	"time"
)

func PackNull() []byte {
	return []byte{type_null_code}
}

func PackBool(value bool) []byte {
	if value {
		return []byte{type_bool_code, byte(1)}
	} else {
		return []byte{type_bool_code, byte(0)}
	}
}

func PackInt8(value int8) []byte {
	return []byte{type_int8_code, byte(value)}
}

func PackInt16(value int16) []byte {
	return []byte{type_int16_code, byte((value >> 8) & mask), byte(value & mask)}
}

func PackInt32(value int32) []byte {
	return []byte{type_int32_code, byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

func PackInt64(value int64) []byte {
	return []byte{type_int64_code, byte((value >> 56) & mask), byte((value >> 48) & mask), byte((value >> 40) & mask), byte((value >> 32) & mask), byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}

}

func PackFloat(v float32) []byte {
	value := math.Float32bits(v)
	return []byte{type_float_code, byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

func PackDouble(v float64) []byte {
	value := math.Float64bits(v)
	return []byte{type_double_code, byte((value >> 56) & mask), byte((value >> 48) & mask), byte((value >> 40) & mask), byte((value >> 32) & mask), byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

func DateTimeToDouble(dateTime time.Time) float64 {
	time_ms := float64(dateTime.UnixNano()/1e6) / 1e3
	return time_ms
}

func PackDateTime(dateTime float64) []byte {
	value := math.Float64bits(dateTime)
	return []byte{type_datetime_code, byte((value >> 56) & mask), byte((value >> 48) & mask), byte((value >> 40) & mask), byte((value >> 32) & mask), byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

// noinspection GoUnusedExportedFunction
func PackString(buf []byte, value string) []byte {
	data := []byte(value)
	len_v := uint16(len(data))

	copy(buf[0:4], []byte{type_string_code, type_int16_code, byte((len_v >> 8) & mask), byte(len_v & mask)})
	copy(buf[4:], data)
	newDate := buf[:(len_v + 4)]
	return newDate
}

func PackBytes(buf []byte, value []byte) []byte {
	len_v := uint16(len(value))
	copy(buf[0:4], []byte{type_binary_code, type_int16_code, byte((len_v >> 8) & mask), byte(len_v & mask)})
	copy(buf[4:], value)
	newDate := buf[:(len_v + 4)]
	return newDate
}

// noinspection GoUnusedExportedFunction
func PackBinary(buf []byte, value []byte) []byte {
	return PackBytes(buf, value)
}

// noinspection GoUnusedExportedFunction
func StringToBytes(value string) []byte {
	return []byte(value)
}

// noinspection GoUnusedExportedFunction
func BoolToByte(value bool) []byte {
	if value {
		return []byte{byte(1)}
	} else {
		return []byte{byte(0)}
	}
}

// INT 转换
func Int8ToByte(value int8) []byte {
	return []byte{byte(value)}
}

func Int16ToByte(value int16) []byte {
	return []byte{byte((value >> 8) & mask), byte(value & mask)}
}

func Int32ToByte(value int32) []byte {
	return []byte{byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

func Int64ToByte(value int64) []byte {
	return []byte{byte((value >> 56) & mask), byte((value >> 48) & mask), byte((value >> 40) & mask), byte((value >> 32) & mask), byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

// UINT 转换
func UInt8ToByte(value uint8) []byte {
	return []byte{byte(value)}
}

func UInt16ToByte(value uint16) []byte {
	return []byte{byte((value >> 8) & mask), byte(value & mask)}
}

func UInt32ToByte(value uint32) []byte {
	return []byte{byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

func UInt64ToByte(value uint64) []byte {
	return []byte{byte((value >> 56) & mask), byte((value >> 48) & mask), byte((value >> 40) & mask), byte((value >> 32) & mask), byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

// Float 转换
func Float32ToByte(v float32) []byte {
	value := math.Float32bits(v)
	return []byte{byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

func Float64ToByte(v float64) []byte {
	value := math.Float64bits(v)
	return []byte{byte((value >> 56) & mask), byte((value >> 48) & mask), byte((value >> 40) & mask), byte((value >> 32) & mask), byte((value >> 24) & mask), byte((value >> 16) & mask), byte((value >> 8) & mask), byte(value & mask)}
}

func DateTimeToByte(value time.Time) []byte {
	time_ms := float64(value.UnixNano()/1e6) / 1e3
	return Float64ToByte(time_ms)
}

func ByteToDate(value []byte) time.Time {
	timeValue := ByteToFloat64(value)
	return DoubleToDate(timeValue)
}

func ByteToDay(value []byte) time.Time {
	timeValue := math.Float64frombits(uint64(value[0])<<56 | uint64(value[1])<<48 | uint64(value[2])<<40 | uint64(value[3])<<32 | uint64(value[4])<<24 | uint64(value[5])<<16 | uint64(value[6])<<8 | uint64(value[7]))
	sec := int64(timeValue)
	return time.Unix((sec/86400)*86400, 0)
}

func ByteToDayUnix(value []byte) int64 {
	timeValue := math.Float64frombits(uint64(value[0])<<56 | uint64(value[1])<<48 | uint64(value[2])<<40 | uint64(value[3])<<32 | uint64(value[4])<<24 | uint64(value[5])<<16 | uint64(value[6])<<8 | uint64(value[7]))
	sec := int64(timeValue)
	return time.Unix((sec/86400)*86400, 0).Unix()
}

func ByteToDateTime(value []byte) float64 {
	timeValue := ByteToFloat64(value)
	return timeValue
}

func ByteToInt(value []byte) int {
	switch len(value) {
	case 1:
		return int(ByteToInt8(value))
	case 2:
		return int(ByteToInt16(value))
	case 4:
		return int(ByteToInt32(value))
	case 8:
		return int(ByteToInt64(value))
	default:
		return 0
	}
}

func BytetoBool(value []byte) bool {
	if len(value) > 0 {
		return value[0] != 0
	} else {
		return false
	}
}

func BytesToString(value []byte) string {
	return string(value)
}

func ByteToInt8(value []byte) int8 {
	return int8(value[0])
}

func ByteToInt16(value []byte) int16 {
	return int16(value[0])<<8 | int16(value[1])
}

func ByteToInt32(value []byte) int32 {
	return int32(value[0])<<24 | int32(value[1])<<16 | int32(value[2])<<8 | int32(value[3])
}

func ByteToInt64(value []byte) int64 {
	return int64(value[0])<<56 | int64(value[1])<<48 | int64(value[2])<<40 | int64(value[3])<<32 | int64(value[4])<<24 | int64(value[5])<<16 | int64(value[6])<<8 | int64(value[7])
}

func ByteToUInt8(value []byte) uint8 {
	return uint8(value[0])
}

func ByteToUInt16(value []byte) uint16 {
	return uint16(value[0])<<8 | uint16(value[1])
}

func ByteToUInt32(value []byte) uint32 {
	return uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3])
}

func ByteToUInt64(value []byte) uint64 {
	return uint64(value[0])<<56 | uint64(value[1])<<48 | uint64(value[2])<<40 | uint64(value[3])<<32 | uint64(value[4])<<24 | uint64(value[5])<<16 | uint64(value[6])<<8 | uint64(value[7])
}

func ByteToFloat32(value []byte) float32 {
	return math.Float32frombits(uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3]))
}

func ByteToFloat64(value []byte) float64 {
	return math.Float64frombits(uint64(value[0])<<56 | uint64(value[1])<<48 | uint64(value[2])<<40 | uint64(value[3])<<32 | uint64(value[4])<<24 | uint64(value[5])<<16 | uint64(value[6])<<8 | uint64(value[7]))
}

func ByteToDouble(value []byte) float64 {
	return math.Float64frombits(uint64(value[0])<<56 | uint64(value[1])<<48 | uint64(value[2])<<40 | uint64(value[3])<<32 | uint64(value[4])<<24 | uint64(value[5])<<16 | uint64(value[6])<<8 | uint64(value[7]))
}

func DoubleToDate(timeValue float64) time.Time {
	sec := int64(timeValue)
	nsec := int64(timeValue*1e3) % 1000 * 1e6
	return time.Unix(sec, nsec)

}
