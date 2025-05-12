package opio

import (
	"fmt"
	"reflect"
	"strconv"

	// time 包不再需要，移除

	"github.com/tc252617228/opio/internal/utils"
)

const (
	minSliceBodyLen = 1 // body中只有元素的数据类型
)

type OPSlice struct {
	utils.BytesBase
	eleType int8     // 元素类型, 参考 opio.Vt* 常量
	iter    Iterator // 迭代器，用于遍历 slice 中的元素
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
	// Handle error from DecodeSlice
	opSlice, _ := DecodeSlice(data[start:end]) // Ignore error for simplicity in getter
	return opSlice
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
		// Handle error from DecodeSlice
		opSlice, _ := DecodeSlice(data[start:end]) // Ignore error for simplicity in getter
		res[i] = opSlice
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
	// Handle error from DecodeMap, ignore for getter simplicity
	opMap, _ := DecodeMap(data[start:end])
	return opMap
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
		// Handle error from DecodeMap, ignore for getter simplicity
		opMap, _ := DecodeMap(data[start:end])
		res[i] = opMap
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
	// Handle error from DecodeStructure, ignore for getter simplicity
	opStruct, _ := DecodeStructure(data[start:end])
	return opStruct
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
		// Handle error from DecodeStructure, ignore for getter simplicity
		opStruct, _ := DecodeStructure(data[start:end])
		res[i] = opStruct
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
		// Handle error from DecodeSlice
		opSlice, _ := DecodeSlice(raw) // Ignore error for simplicity in getter
		return opSlice

	case VtMap:
		// Handle error from DecodeMap, ignore for getter simplicity
		opMap, _ := DecodeMap(raw)
		return opMap

	case VtStructure:
		// Handle error from DecodeStructure, ignore for getter simplicity
		opStruct, _ := DecodeStructure(raw)
		return opStruct

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
			// Handle error from DecodeSlice
			opSlice, _ := DecodeSlice(raw) // Ignore error for simplicity here
			if opSlice != nil {
				res += opSlice.String(prettify)
			} else {
				res += "nil"
			}

		case VtMap:
			// Handle error from DecodeMap, ignore for String simplicity
			opMap, _ := DecodeMap(raw)
			if opMap != nil {
				res += opMap.String(prettify)
			} else {
				res += "nil"
			}

		case VtStructure:
			// Handle error from DecodeStructure, ignore for String simplicity
			opStr, _ := DecodeStructure(raw)
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

// slice 编码格式:
/*
    head: (头部)
        body len code (1 byte)     	- body 部分长度的编码类型 (mpBin8/mpBin16/mpBin32)
		body len      (variable) 	- body 部分的实际长度 (根据 code 决定占 1, 2, 或 4 字节)
    body: (主体)
		element type  (1 byte)     	- slice 中元素的统一 opio 数据类型 (Vt*)
		elements      (variable) 	- 连续存放的元素数据
			- 定长类型 (bool, int*, float*): 直接存放元素值
			- 变长类型 (string, slice, map, struct): 存放每个元素的 opio 编码 (head + body)
*/

// EncodeSlice 将 Go 的 slice 或 array 编码为 opio slice 格式的字节流。
// 返回值: headLen (头部长度), rawData (编码后的完整字节流), error (错误信息)
func EncodeSlice(value interface{}) (int, []byte, error) {
	if nil == value {
		// 对于 nil 输入，返回空二进制和 nil 错误是合理的
		// 假设 MakeEmptyBinary 返回 []byte
		return 0, MakeEmptyBinary(), nil
	}
	rv := reflect.Indirect(reflect.ValueOf(value))

	// 类型检查
	rvKind := rv.Kind()
	if rvKind != reflect.Slice && rvKind != reflect.Array {
		return 0, nil, fmt.Errorf("EncodeSlice 错误: 输入值不是 slice 或 array, 类型为 %v", rvKind)
	}

	rt := rv.Type()
	eleType := rt.Elem()
	eleKind := eleType.Kind()
	eleNum := rv.Len()

	// 空数组或空切片
	if 0 == eleNum {
		// 假设 MakeEmptyBinary 返回 []byte
		return 0, MakeEmptyBinary(), nil
	}

	dataType, ok := fixedTypeMap[eleKind]
	if ok {
		fixedLen, ok := fixedTypeLenMap[dataType]
		if !ok {
			// 不支持的定长类型
			return 0, nil, fmt.Errorf("EncodeSlice 错误: 不支持的定长 slice 元素类型: %v", eleKind)
		}
		// 处理定长类型 slice
		headLen, rawData, err := putFixedSlice(rv, eleKind, eleNum, dataType, fixedLen)
		return headLen, rawData, err // 直接返回 putFixedSlice 的结果
	}

	// 检查是否为支持的变长类型
	dataType, ok = varTypeMap[eleKind]
	if !ok {
		// 不支持的变长类型
		return 0, nil, fmt.Errorf("EncodeSlice 错误: 不支持的变长 slice 元素类型: %v", eleKind)
	}
	// 处理变长类型 slice
	headLen, rawData, err := putVarSlice(rv, eleKind, eleNum, dataType)
	return headLen, rawData, err // 直接返回 putVarSlice 的结果
}

func putFixedSlice(rv reflect.Value, eleKind reflect.Kind, eleNum int, dataType, dataTypeLen int8) (int, []byte, error) {

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
		headLen = 5 // 移除行首多余的 'g'
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY长度编码
		rawData[1] = byte(bodyLen >> 24) // BODY长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	}
	if nil == rawData || 0 == headLen {
		// 头部创建失败，通常是 bodyLen 过大或其他逻辑错误
		return 0, nil, fmt.Errorf("putFixedSlice 错误: 创建头部失败, bodyLen:%v", bodyLen)
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

	return headLen, rawData, nil
}

func putVarSlice(rv reflect.Value, eleKind reflect.Kind, eleNum int, dataType int8) (int, []byte, error) {

	bodyBuff := utils.MBuffer{}

	// 元素类型
	_, _ = bodyBuff.Write([]byte{byte(dataType)})

	var raw []byte
	var err error
	var headLen int

	for i := 0; i < eleNum; i++ {
		val := rv.Index(i)
		// 重置循环变量
		raw = nil
		err = nil
		headLen = 0

		// 在 switch 内部处理错误并检查
		switch eleKind {
		case reflect.String:
			_, raw = utils.PutString(val.String())
			// err 保持 nil

		case reflect.Array, reflect.Slice: // 合并 Array 和 Slice
			headLen, raw, err = EncodeSlice(val.Interface())
			if err != nil { // 在 case 内部检查错误
				return 0, nil, fmt.Errorf("putVarSlice error: encoding element %d (type %v): %w", i, eleKind, err)
			}

		case reflect.Map:
			headLen, raw, err = EncodeMap(val.Interface())
			if err != nil { // 在 case 内部检查错误
				return 0, nil, fmt.Errorf("putVarSlice error: encoding element %d (type %v): %w", i, eleKind, err)
			}

		case reflect.Struct:
			headLen, raw, err = EncodeStructure(val.Interface())
			if err != nil { // 在 case 内部检查错误
				return 0, nil, fmt.Errorf("putVarSlice error: encoding element %d (type %v): %w", i, eleKind, err)
			}
		default:
			// 不应到达这里，因为 EncodeSlice 已经做了类型检查
			return 0, nil, fmt.Errorf("putVarSlice internal error: unsupported element kind %v at index %d", eleKind, i)
		}

		// 如果 switch 没有因错误返回，则 err 应该为 nil (或来自 PutString 的隐式 nil)
		// 检查 raw 是否为 nil (异常情况)
		if raw == nil {
			// 这表示 EncodeSlice/Map/Structure 或 PutString 返回了 nil, nil
			// 对于有效输入（即使是空集合），它们应该返回非 nil 的字节表示（例如 MakeEmptyBinary 的结果）
			return 0, nil, fmt.Errorf("putVarSlice internal error: element %d (type %v) encoded successfully but resulted in nil data", i, eleKind)
		}

		// 将编码后的数据写入缓冲区
		_, writeErr := bodyBuff.Write(raw)
		if writeErr != nil {
			// 处理写入 bodyBuff 时的错误
			return 0, nil, fmt.Errorf("putVarSlice error: writing element %d to buffer: %w", i, writeErr)
		}
	}
	// write head and body
	var rawData []byte // 显式声明 rawData
	headLen = 0        // 使用 = 赋值，而不是 := 重新声明
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
		// 头部创建失败
		return 0, nil, fmt.Errorf("putVarSlice 错误: 创建头部失败, bodyLen:%v", bodyLen)
	}

	copy(rawData[headLen:], bodyBuff.Bytes())

	return headLen, rawData, nil
}

// DecodeSlice 将 opio slice 格式的字节流解码为 OPSlice 对象。
// src: 包含 opio slice 编码的字节流 (head + body)。
// 返回值: *OPSlice (解码后的对象), error (错误信息)
func DecodeSlice(src []byte) (*OPSlice, error) {
	// 对空值做处理
	// 假设 IsEmptyBinary 返回 bool
	if IsEmptyBinary(src) {
		// 对于空二进制，返回 nil 对象和 nil 错误是合理的
		return nil, nil
	}

	srcLen := len(src)

	// copy全数据
	data := make([]byte, srcLen)
	copy(data, src)

	// 创建 OPSlice 对象
	res := &OPSlice{}
	// SetData 会解析头部信息 (headLen, bodyLen) 并设置内部数据引用
	// 注意：假设 utils.BytesBase.SetData 不返回错误。如果它可能失败，需要调整。
	res.SetData(data)

	// 获取头部长度，计算 body 的起始偏移量
	offset := res.GetHeadLen()
	if offset >= srcLen {
		return nil, fmt.Errorf("DecodeSlice 错误: 头部长度 (%d) 超出总长度 (%d)", offset, srcLen)
	}
	// 读取 body 的第一个字节：元素类型
	dataType := int8(data[offset])
	offset++

	// set element data type
	res.eleType = dataType

	// 检查 body 长度是否至少包含元素类型字节
	bodyLen := res.GetBodyLen()
	if bodyLen < minSliceBodyLen {
		return nil, fmt.Errorf("DecodeSlice 错误: body 长度 (%d) 小于最小长度 (%d)", bodyLen, minSliceBodyLen)
	}
	// 如果 body 长度正好等于最小长度，说明 slice 为空（只有类型信息）
	if minSliceBodyLen == bodyLen {
		// 空 slice，迭代器为 nil
		return res, nil
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
			eleHeadLen++ // 计入长度编码字节

			// 检查偏移量是否越界 (读取 lenCode 后)
			if offset >= srcLen {
				return nil, fmt.Errorf("DecodeSlice 错误: 解析变长元素头部时偏移量 (%d) 越界 (总长 %d)", offset, srcLen)
			}

			switch lenCode {
			case mpBin8:
				// 检查读取 body 长度是否越界
				if offset+1 > srcLen {
					return nil, fmt.Errorf("DecodeSlice 错误: 读取 mpBin8 长度时偏移量 (%d) 越界 (总长 %d)", offset, srcLen)
				}
				eleBodyLen = int(data[offset])
				offset++
				eleHeadLen++ // 计入 body 长度字节

			case mpBin16:
				// 检查读取 body 长度是否越界
				if offset+2 > srcLen {
					return nil, fmt.Errorf("DecodeSlice 错误: 读取 mpBin16 长度时偏移量 (%d) 越界 (总长 %d)", offset, srcLen)
				}
				eleBodyLen = int(data[offset])<<8 | int(data[offset+1])
				offset += 2
				eleHeadLen += 2 // 计入 body 长度字节

			case mpBin32:
				// 检查读取 body 长度是否越界
				if offset+4 > srcLen {
					return nil, fmt.Errorf("DecodeSlice 错误: 读取 mpBin32 长度时偏移量 (%d) 越界 (总长 %d)", offset, srcLen)
				}
				eleBodyLen = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
				offset += 4
				eleHeadLen += 4 // 计入 body 长度字节
			default:
				// 无效的长度编码
				return nil, fmt.Errorf("DecodeSlice 错误: 无效的变长元素长度编码: %x", lenCode)
			}

			// 检查 body 是否越界
			if offset+eleBodyLen > srcLen {
				return nil, fmt.Errorf("DecodeSlice 错误: 元素 body 越界 (offset=%d, bodyLen=%d, srcLen=%d)", offset, eleBodyLen, srcLen)
			}

			// 记录当前元素的总长度 (head + body)
			posList = append(posList, eleHeadLen+eleBodyLen)
			// 移动偏移量到下一个元素的开始位置
			offset += eleBodyLen
		}
		// 创建变长迭代器
		res.iter = newVarByteIterator(posList, dataStart, srcLen)
	}
	return res, nil
}
