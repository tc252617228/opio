//go:build generate
// +build generate

//go:generate genny -in=$GOFILE -out=gen_$GOFILE gen "EleType=bool,int8,int16,int32,int64,float32,float64,dateTime"

package opio

import (
	"opio/internal/utils"

	"github.com/cheekybits/genny/generic"
)

type EleType generic.Type

func EncodeSliceEleType(value []EleType) (int, []byte) {
	eleNum := len(value)
	// nil; empty slice
	if eleNum == 0 {
		return 0, MakeEmptyBinary()
	}

	dataType := VtEleType
	fixedLen := VtEleTypeLen

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
	case bodyLen < 0x100000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	default:
		logs.Error("bodyLen too long: %v", bodyLen)
		return 0, nil
	}

	if rawData == nil || headLen == 0 {
		logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}
	offset := headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := value[i]
		end := offset + fixedLen
		utils.PutEleType(rawData[offset:end], val)
		offset = end
	}
	return headLen, rawData
}
