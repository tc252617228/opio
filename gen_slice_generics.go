// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package opio

import (
	"time"

	"github.com/tc252617228/opio/internal/utils"
)

func EncodeSliceBool(value []bool) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtBool
	fixedLen := VtBoolLen

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		//logs.Error("bodyLen too long: %v", bodyLen)
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutBool(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}

func EncodeSliceInt8(value []int8) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtInt8
	fixedLen := VtInt8Len

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		//logs.Error("bodyLen too long: %v", bodyLen)
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutInt8(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}

func EncodeSliceInt16(value []int16) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtInt16
	fixedLen := VtInt16Len

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		//logs.Error("bodyLen too long: %v", bodyLen)
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutInt16(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}

func EncodeSliceInt32(value []int32) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtInt32
	fixedLen := VtInt32Len

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("bodyLen too long: %v", bodyLen)
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutInt32(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}

func EncodeSliceInt64(value []int64) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtInt64
	fixedLen := VtInt64Len

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("bodyLen too long: %v", bodyLen)
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutInt64(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}

func EncodeSliceFloat32(value []float32) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtFloat32
	fixedLen := VtFloat32Len

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("bodyLen too long: %v", bodyLen)
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutFloat32(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}

func EncodeSliceFloat64(value []float64) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtFloat64
	fixedLen := VtFloat64Len

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("bodyLen too long: %v", bodyLen)
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutFloat64(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}

func EncodeSliceDateTime(value []time.Time) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtDateTime
	fixedLen := VtDateTimeLen

	var rawData []byte
	bodyLen := eleNum*fixedLen + 1
	headLen := 0
	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY长度编码
		rawData[1] = byte(bodyLen) // BODY长度
	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY长度编码
		rawData[1] = byte(bodyLen >> 8) // BODY长度
		rawData[2] = byte(bodyLen)
	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		// fmt.Printf("bodyLen too long: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("bodyLen too long: %v", bodyLen)
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		// fmt.Printf("make head failed, bodyLen: %v\n", bodyLen) // 使用 Printf 并添加换行符
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutDateTime(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}
