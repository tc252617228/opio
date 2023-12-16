package opio

import (
	"fmt"
	"math"
)

func (value *Value) Pack() []byte {
	v := math.Float64bits(value.AV)
	return []byte{byte(value.ID >> 24), byte(value.ID >> 16), byte(value.ID >> 8), byte(value.ID),
		byte(value.TM >> 24), byte(value.TM >> 16), byte(value.TM >> 8), byte(value.TM),
		byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func UnPack(data []byte) Value {
	value := Value{}
	value.ID = (int32(data[0]) << 24) | (int32(data[1]) << 16) | (int32(data[2]) << 8) | int32(data[3])
	value.TM = (int32(data[4]) << 24) | (int32(data[5]) << 16) | (int32(data[6]) << 8) | int32(data[7])
	value.AV = math.Float64frombits(
		(uint64(data[8]) << 56) | (uint64(data[9]) << 48) |
			(uint64(data[10]) << 40) | (uint64(data[11]) << 32) |
			(uint64(data[12]) << 24) | (uint64(data[13]) << 16) |
			(uint64(data[14]) << 8) | uint64(data[15]))
	return value
}

// WriteArchive -
func (op *IOConnect) WriteArchiveValue(v []Value, cache bool) (err error) {
	io := op.io
	count := len(v)
	flag := flagWall
	if cache {
		flag |= flagCache
	}
	_ = io.PutInt32(MAGIC)
	_ = io.PutInt32(cmdInsert)
	_ = io.PutInt32(urlArchive)
	_ = io.PutInt16(0)
	_ = io.PutInt16(flag)
	_ = io.PutInt32(int32(count))
	for i := 0; i < count && err == nil; i++ {
		value := v[i]
		_ = io.PutInt32(value.ID)
		_ = io.PutInt8(value.RT)
		_ = io.PutInt32(1)
		_ = io.PutInt32(value.TM)
		_ = io.PutInt16(value.DS)
		switch value.RT {
		case TypeAX:
			err = io.PutFloat32(float32(value.AV))
		case TypeDX:
			err = io.PutInt8(int8(value.AV))
		case TypeI2:
			err = io.PutInt16(int16(value.AV))
		case TypeI4:
			err = io.PutInt32(int32(value.AV))
		case TypeR8:
			err = io.PutFloat64(float64(value.AV))
		}
	}
	if err == nil {
		_ = io.PutInt32(MAGIC)
		err = io.Flush(true)
	}
	if err != nil {
		fmt.Println(err)
		return err
	}
	var echo int8
	echo, err = io.ReadEcho()
	if echo != 0 {
		err = fmt.Errorf("WriteArchive error %d", int32(echo))
	}
	return err
}
