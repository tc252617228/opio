package opio

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/tc252617228/opio/internal/utils"
)

const (
	minSliceBodyLen = 1 // body中只有元素的数据类型
)

type OPSlice struct {
	utils.BytesBase
	eleType int8     // 元素类型, see "Value type" in environment/opio/driver/const.go
	iter    Iterator // 迭代器
}

func (ops *OPSlice) GetDataType() int8 {
	return ops.eleType
}

func (ops *OPSlice) Iterator() Iterator {
	return ops.iter
}

func (ops *OPSlice) IsEmpty() bool {
	return ops.iter == nil
}

func (ops *OPSlice) Number() int {
	iter := ops.iter
	if nil == iter {
		return 0
	}
	return iter.Number()
}

func (ops *OPSlice) GetBool() bool {
	if ops.IsEmpty() {
		return false
	}
	if ops.eleType != VtBool {
		return false
	}
	data := ops.GetData()
	if 0 == len(data) {
		return false
	}
	start, end := ops.iter.curr()
	val := utils.GetInt8(data[start:end])
	return val > 0
}

func (ops *OPSlice) GetBoolValues() []bool {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtBool {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]bool, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		val := utils.GetInt8(data[start:end])
		res[i] = val > 0
	}
	return res
}

func (ops *OPSlice) GetInt8() int8 {
	if ops.IsEmpty() {
		return 0
	}
	if ops.eleType != VtInt8 {
		return 0
	}
	data := ops.GetData()
	if 0 == len(data) {
		return 0
	}
	start, end := ops.iter.curr()
	return utils.GetInt8(data[start:end])
}

func (ops *OPSlice) GetInt8Values() []int8 {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtInt8 {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]int8, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = utils.GetInt8(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetInt16() int16 {
	if ops.IsEmpty() {
		return 0
	}
	if ops.eleType != VtInt16 {
		return 0
	}
	data := ops.GetData()
	if 0 == len(data) {
		return 0
	}
	start, end := ops.iter.curr()
	return utils.GetInt16(data[start:end])
}

func (ops *OPSlice) GetInt16Values() []int16 {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtInt16 {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]int16, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = utils.GetInt16(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetInt32() int32 {
	if ops.IsEmpty() {
		return 0
	}
	if ops.eleType != VtInt32 {
		return 0
	}
	data := ops.GetData()
	if 0 == len(data) {
		return 0
	}
	start, end := ops.iter.curr()
	return utils.GetInt32(data[start:end])
}

func (ops *OPSlice) GetInt32Values() []int32 {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtInt32 {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]int32, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = utils.GetInt32(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetInt64() int64 {
	if ops.IsEmpty() {
		return 0
	}
	if ops.eleType != VtInt64 {
		return 0
	}
	data := ops.GetData()
	if 0 == len(data) {
		return 0
	}
	start, end := ops.iter.curr()
	return utils.GetInt64(data[start:end])
}

func (ops *OPSlice) GetInt64Values() []int64 {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtInt64 {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]int64, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = utils.GetInt64(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetFloat32() float32 {
	if ops.IsEmpty() {
		return 0
	}
	if ops.eleType != VtFloat {
		return 0
	}
	data := ops.GetData()
	if 0 == len(data) {
		return 0
	}
	start, end := ops.iter.curr()
	return utils.GetFloat32(data[start:end])
}

func (ops *OPSlice) GetFloat32Values() []float32 {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtFloat {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]float32, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = utils.GetFloat32(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetFloat64() float64 {
	if ops.IsEmpty() {
		return 0
	}
	if ops.eleType != VtDouble {
		return 0
	}
	data := ops.GetData()
	if 0 == len(data) {
		return 0
	}
	start, end := ops.iter.curr()
	return utils.GetFloat64(data[start:end])
}

func (ops *OPSlice) GetFloat64Values() []float64 {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtDouble {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]float64, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = utils.GetFloat64(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetString() string {
	if ops.IsEmpty() {
		return ""
	}
	if ops.eleType != VtString {
		return ""
	}
	data := ops.GetData()
	if 0 == len(data) {
		return ""
	}
	start, end := ops.iter.curr()
	return utils.GetString(data[start:end])
}

func (ops *OPSlice) GetStringValues() []string {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtString {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]string, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = utils.GetString(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetSlice() *OPSlice {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtSlice {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	start, end := ops.iter.curr()
	return DecodeSlice(data[start:end])
}

func (ops *OPSlice) GetSlices() []*OPSlice {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtSlice {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]*OPSlice, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = DecodeSlice(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetMap() *OPMap {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtMap {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	start, end := ops.iter.curr()
	return DecodeMap(data[start:end])
}

func (ops *OPSlice) GetMaps() []*OPMap {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtMap {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]*OPMap, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = DecodeMap(data[start:end])
	}
	return res
}

func (ops *OPSlice) GetStructure() *OPStructure {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtStructure {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	start, end := ops.iter.curr()
	return DecodeStructure(data[start:end])
}

func (ops *OPSlice) GetStructures() []*OPStructure {
	if ops.IsEmpty() {
		return nil
	}
	if ops.eleType != VtStructure {
		return nil
	}
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}
	num := ops.Number()
	if 0 == num {
		return nil
	}

	res := make([]*OPStructure, num)

	iter := ops.Iterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		res[i] = DecodeStructure(data[start:end])
	}
	return res
}

func (ops *OPSlice) Get() interface{} {
	if ops.IsEmpty() {
		return nil
	}
	start, end := ops.iter.curr()
	data := ops.GetData()
	if 0 == len(data) {
		return nil
	}

	raw := data[start:end]

	switch ops.eleType {
	case VtBool:
		val := utils.GetInt8(raw)
		return val > 0

	case VtInt8:
		return utils.GetInt8(raw)

	case VtInt16:
		return utils.GetInt16(raw)

	case VtInt32:
		return utils.GetInt32(raw)

	case VtInt64:
		return utils.GetInt64(raw)

	case VtFloat:
		return utils.GetFloat32(raw)

	case VtDouble:
		return utils.GetFloat64(raw)

	case VtString:
		return utils.GetString(raw)

	case VtSlice:
		return DecodeSlice(raw)

	case VtMap:
		return DecodeMap(raw)

	case VtStructure:
		return DecodeStructure(raw)

	default:
	}
	return nil
}

func (ops *OPSlice) String(prettify bool) string {
	if ops.IsEmpty() {
		return "[]"
	}
	data := ops.GetData()
	res := "["

	num := ops.Number()
	i := 0

	typ := ops.eleType
	iter := ops.iter
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		start, end := ops.iter.curr()
		raw := data[start:end]

		switch typ {
		case VtBool:
			val := utils.GetInt8(raw)
			res += strconv.FormatBool(val > 0)

		case VtInt8:
			res += strconv.FormatInt(int64(utils.GetInt8(raw)), 10)

		case VtInt16:
			res += strconv.FormatInt(int64(utils.GetInt16(raw)), 10)

		case VtInt32:
			res += strconv.FormatInt(int64(utils.GetInt32(raw)), 10)

		case VtInt64:
			res += strconv.FormatInt(utils.GetInt64(raw), 10)

		case VtFloat:
			res += strconv.FormatFloat(float64(utils.GetFloat32(raw)), 'f', -1, 32)

		case VtDouble:
			res += strconv.FormatFloat(utils.GetFloat64(raw), 'f', -1, 64)

		case VtString:
			res += utils.GetString(raw)

		case VtSlice:
			opSlice := DecodeSlice(raw)
			if opSlice != nil {
				res += opSlice.String(prettify)
			} else {
				res += "nil"
			}

		case VtMap:
			opMap := DecodeMap(raw)
			if opMap != nil {
				res += opMap.String(prettify)
			} else {
				res += "nil"
			}

		case VtStructure:
			opStr := DecodeStructure(raw)
			if opStr != nil {
				res += opStr.String(prettify)
			} else {
				res += "nil"
			}

		default:
		}
		if i < num-1 {
			if prettify {
				if typ != VtSlice && typ != VtMap && typ != VtStructure && num <= 30 {
					res += ", "
				} else {
					if VtSlice == typ || VtMap == typ || VtStructure == typ {
						res += ",\n"
					} else {
						if i > 0 && i%30 == 0 {
							res += ",\n"
						} else {
							res += ", "
						}
					}
				}
			} else {
				res += ", "
			}
		}
		i++
	}

	res += "]"

	return res
}

// slice bytes:
/*
    head:
        element data len code	(1 byte)
		element data len  		(variable-length bytes)
    body:
		element type    		(1 byte)
		elements 	    		(variable-length bytes)
*/

func EncodeSlice(value interface{}) (int, []byte) {
	if nil == value {
		return 0, MakeEmptyBinary()
	}
	rv := reflect.Indirect(reflect.ValueOf(value))

	// fault return
	rvKind := rv.Kind()
	if rvKind != reflect.Slice && rvKind != reflect.Array {
		fmt.Println("value is not slice or array")
		//logs.Error("value is not slice or array")
		return 0, nil
	}

	rt := rv.Type()
	eleType := rt.Elem()
	eleKind := eleType.Kind()
	eleNum := rv.Len()

	// 空数组或空切片
	if 0 == eleNum {
		return 0, MakeEmptyBinary()
	}

	dataType, ok := fixedTypeMap[eleKind]
	if ok {
		fixedLen, ok := fixedTypeLenMap[dataType]
		if !ok {
			// fault return
			fmt.Println("unsupported slice data type:%v", eleKind)
			//logs.Error("unsupported slice data type:%v", eleKind)
			return 0, nil
		}
		return putFixedSlice(rv, eleKind, eleNum, dataType, fixedLen)
	}
	dataType, ok = varTypeMap[eleKind]
	if !ok {
		fmt.Println("unsupported slice data type:%v", eleKind)
		//logs.Error("unsupported slice data type:%v", eleKind)
		return 0, nil
	}
	return putVarSlice(rv, eleKind, eleNum, dataType)
}

func putFixedSlice(rv reflect.Value, eleKind reflect.Kind, eleNum int, dataType, dataTypeLen int8) (int, []byte) {

	fixedLen := int(dataTypeLen)

	offset := 0

	// write head and body
	var rawData []byte = nil
	headLen := 0
	bodyLen := eleNum*fixedLen + 1 // 元素个数*元素长度+元素类型(1 byte)

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
	}
	if nil == rawData || 0 == headLen {
		fmt.Println("make head failed, bodyLen:%v", bodyLen)
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
		return 0, nil
	}

	offset = headLen
	// 元素类型
	rawData[offset] = byte(dataType)
	offset++

	for i := 0; i < eleNum; i++ {
		val := rv.Index(i)
		end := offset + fixedLen

		switch eleKind {
		case reflect.Bool:
			boolVal := val.Bool()
			if boolVal {
				utils.PutInt8(rawData[offset:end], 1)
			} else {
				utils.PutInt8(rawData[offset:end], 0)
			}
		case reflect.Int8:
			utils.PutInt8(rawData[offset:end], int8(val.Int()))

		case reflect.Uint8:
			utils.PutUint8(rawData[offset:end], uint8(val.Uint()))

		case reflect.Int16:
			utils.PutInt16(rawData[offset:end], int16(val.Int()))

		case reflect.Uint16:
			utils.PutUint16(rawData[offset:end], uint16(val.Uint()))

		case reflect.Int32:
			utils.PutInt32(rawData[offset:end], int32(val.Int()))

		case reflect.Uint32:
			utils.PutUint32(rawData[offset:end], uint32(val.Uint()))

		case reflect.Int:
			fallthrough
		case reflect.Int64:
			utils.PutInt64(rawData[offset:end], val.Int())

		case reflect.Uint:
			fallthrough
		case reflect.Uint64:
			utils.PutUint64(rawData[offset:end], val.Uint())

		case reflect.Float32:
			utils.PutFloat32(rawData[offset:end], float32(val.Float()))

		case reflect.Float64:
			utils.PutFloat64(rawData[offset:end], val.Float())
		}
		offset += fixedLen
	}

	return headLen, rawData
}

func putVarSlice(rv reflect.Value, eleKind reflect.Kind, eleNum int, dataType int8) (int, []byte) {

	bodyBuff := utils.MBuffer{}

	// 元素类型
	_, _ = bodyBuff.Write([]byte{byte(dataType)})

	var raw []byte = nil

	for i := 0; i < eleNum; i++ {
		val := rv.Index(i)
		switch eleKind {
		case reflect.String:
			_, raw = utils.PutString(val.String())

		case reflect.Array:
			fallthrough
		case reflect.Slice:
			_, raw = EncodeSlice(val.Interface())

		case reflect.Map:
			_, raw = EncodeMap(val.Interface())

		case reflect.Struct:
			_, raw = EncodeStructure(val.Interface())
		}
		rawLen := len(raw)

		if 0 == rawLen {
			fmt.Println("data was damaged")
			//logs.Error("data was damaged")
			return 0, nil
		}
		_, _ = bodyBuff.Write(raw)

		raw = nil
	}
	// write head and body
	var rawData []byte = nil
	headLen := 0
	bodyLen := bodyBuff.Len()

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
	}
	if 0 == headLen || 0 == len(rawData) {
		fmt.Println("make head failed, bodyLen:%v", bodyLen)
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
	}

	copy(rawData[headLen:], bodyBuff.Bytes())

	return headLen, rawData
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

func DecodeSlice(src []byte) *OPSlice {
	// 对空值做处理
	if IsEmptyBinary(src) {
		return nil
	}

	srcLen := len(src)

	// copy全数据
	data := make([]byte, srcLen)
	copy(data, src)

	// new OPSlice
	res := &OPSlice{}
	res.SetData(data) // SetData会将head和body分析出来

	// 取元素类型
	offset := res.GetHeadLen()
	dataType := int8(src[offset])
	offset++

	// set element data type
	res.eleType = dataType

	bodyLen := res.GetBodyLen()
	if minSliceBodyLen == bodyLen {
		// 说明数据是空的，只有一个数据类型
		return res
	}

	// 真正的数据起始位置
	dataStart := offset

	fixedLen, ok := fixedTypeLenMap[dataType]
	if ok {
		// 定长元素slice
		// 创建返回对象
		res.iter = newFixByteIterator(dataStart, int(fixedLen), srcLen)
	} else {
		// 计算元素位置
		// 求各个元素的长度
		posList := make([]int, 0, 64*utils.KB)

		eleHeadLen := 0
		eleBodyLen := 0

		for offset != srcLen {
			eleHeadLen = 0
			eleBodyLen = 0

			lenCode := uint8(data[offset])
			offset++
			eleHeadLen++

			switch lenCode {
			case mpBin8:
				eleBodyLen = int(data[offset])
				offset++
				eleHeadLen++

			case mpBin16:
				eleBodyLen = int(data[offset])<<8 | int(data[offset+1])
				offset += 2
				eleHeadLen += 2

			case mpBin32:
				eleBodyLen = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
				offset += 4
				eleHeadLen += 4
			}
			posList = append(posList, eleHeadLen+eleBodyLen)
			offset += eleBodyLen
		}
		res.iter = newVarByteIterator(posList, dataStart, srcLen)
	}
	return res
}
