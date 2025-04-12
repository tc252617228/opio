package opio

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/tc252617228/opio/internal/utils"
)

const (
	minMapBodyLen = 2 // body 中只包含 keyType 和 valType
)

type OPMap struct {
	utils.BytesBase
	keyType int8   // key 的类型, 参考 environment/opio/driver/const.go 中的 "Value type"
	valType int8   // value 的类型, 参考 environment/opio/driver/const.go 中的 "Value type"
	pair    OPPair // key-value 对记录器
}

func (opm *OPMap) GetKeyType() int8 {
	return opm.keyType
}

func (opm *OPMap) GetValType() int8 {
	return opm.valType
}

func (opm *OPMap) IsEmpty() bool {
	return opm.pair == nil
}

func (opm *OPMap) FindBool(key interface{}) bool {
	if opm.IsEmpty() {
		return false
	}
	return opm.pair.GetBool(key)
}

func (opm *OPMap) FindInt8(key interface{}) int8 {
	if opm.IsEmpty() {
		return 0
	}
	return opm.pair.GetInt8(key)
}

func (opm *OPMap) FindInt16(key interface{}) int16 {
	if opm.IsEmpty() {
		return 0
	}
	return opm.pair.GetInt16(key)
}

func (opm *OPMap) FindInt32(key interface{}) int32 {
	if opm.IsEmpty() {
		return 0
	}
	return opm.pair.GetInt32(key)
}

func (opm *OPMap) FindInt64(key interface{}) int64 {
	if opm.IsEmpty() {
		return 0
	}
	return opm.pair.GetInt64(key)
}

func (opm *OPMap) FindFloat(key interface{}) float32 {
	if opm.IsEmpty() {
		return 0
	}
	return opm.pair.GetFloat(key)
}

func (opm *OPMap) FindDouble(key interface{}) float64 {
	if opm.IsEmpty() {
		return 0
	}
	return opm.pair.GetDouble(key)
}

func (opm *OPMap) FindString(key interface{}) string {
	if opm.IsEmpty() {
		return ""
	}
	return opm.pair.GetString(key)
}

func (opm *OPMap) FindSlice(key interface{}) *OPSlice {
	if opm.IsEmpty() {
		return nil
	}
	return opm.pair.GetSlice(key)
}

func (opm *OPMap) FindMap(key interface{}) *OPMap {
	if opm.IsEmpty() {
		return nil
	}
	// opm.pair.GetMap currently returns *OPMap (will be updated later in pair.go)
	return opm.pair.GetMap(key)
}

func (opm *OPMap) FindStructure(key interface{}) *OPStructure {
	if opm.IsEmpty() {
		return nil
	}
	// opm.pair.GetStructure currently returns *OPStructure (will be updated later in pair.go)
	return opm.pair.GetStructure(key)
}

func (opm *OPMap) Find(key interface{}) interface{} {
	if opm.IsEmpty() {
		return nil
	}
	pair := opm.pair
	if nil == pair {
		return nil
	}
	valType := opm.valType
	switch valType {
	case VtBool:
		return pair.GetBool(key)

	case VtInt8:
		return pair.GetInt8(key)

	case VtInt16:
		return pair.GetInt16(key)

	case VtInt32:
		return pair.GetInt32(key)

	case VtInt64:
		return pair.GetInt64(key)

	case VtFloat:
		return pair.GetFloat(key)

	case VtDouble:
		return pair.GetDouble(key)

	case VtString:
		return pair.GetString(key)

	case VtSlice:
		// GetSlice already handles DecodeSlice error internally, returning *OPSlice
		return pair.GetSlice(key)

	case VtMap:
		// GetMap will be modified to handle DecodeMap error internally, returning *OPMap
		return pair.GetMap(key) // Keep as is for now

	case VtStructure:
		// GetStructure will be modified to handle DecodeStructure error internally, returning *OPStructure
		return pair.GetStructure(key) // Keep as is for now

	default:
	}
	return nil
}

func (opm *OPMap) Range(f func(key, value interface{}) bool) {
	if opm.IsEmpty() {
		return
	}
	pair := opm.pair
	allKeys := pair.AllKeys()
	if 0 == len(allKeys) {
		return
	}
	breakLoop := false
	for _, dataKey := range allKeys {
		valType := opm.valType
		switch valType {
		case VtBool:
			if !f(dataKey, pair.GetBool(dataKey)) {
				breakLoop = true
				break
			}
		case VtInt8:
			if !f(dataKey, pair.GetInt8(dataKey)) {
				breakLoop = true
				break
			}
		case VtInt16:
			if !f(dataKey, pair.GetInt16(dataKey)) {
				breakLoop = true
				break
			}
		case VtInt32:
			if !f(dataKey, pair.GetInt32(dataKey)) {
				breakLoop = true
				break
			}
		case VtInt64:
			if !f(dataKey, pair.GetInt64(dataKey)) {
				breakLoop = true
				break
			}
		case VtFloat:
			if !f(dataKey, pair.GetFloat(dataKey)) {
				breakLoop = true
				break
			}
		case VtDouble:
			if !f(dataKey, pair.GetDouble(dataKey)) {
				breakLoop = true
				break
			}
		case VtString:
			if !f(dataKey, pair.GetString(dataKey)) {
				breakLoop = true
				break
			}
		case VtSlice:
			// GetSlice already handles DecodeSlice error internally
			if !f(dataKey, pair.GetSlice(dataKey)) {
				breakLoop = true
				break
			}
		case VtMap:
			// GetMap will handle DecodeMap error internally
			if !f(dataKey, pair.GetMap(dataKey)) { // Keep as is for now
				breakLoop = true
				break
			}
		case VtStructure:
			// GetStructure will handle DecodeStructure error internally
			if !f(dataKey, pair.GetStructure(dataKey)) { // Keep as is for now
				breakLoop = true
				break
			}

		default:
		}
		if breakLoop {
			break
		}
	}
}

func (opm *OPMap) String(prettify bool) string {
	if opm.IsEmpty() {
		return "{}"
	}

	pair := opm.pair
	allKeys := pair.AllKeys()
	if 0 == len(allKeys) {
		return "{}"
	}

	keyType := opm.keyType
	valType := opm.valType

	num := len(allKeys)
	i := 0

	res := "{"

	for _, dataKey := range allKeys {
		// simple kv start

		// write key
		switch keyType {
		case VtInt8:
			fallthrough
		case VtInt16:
			fallthrough
		case VtInt32:
			fallthrough
		case VtInt64:
			res += strconv.FormatInt(parseInt(dataKey), 10)

		case VtFloat:
			fallthrough
		case VtDouble:
			res += strconv.FormatFloat(parseFloat(dataKey), 'f', -1, 64)

		case VtString:
			res += parseString(dataKey)
		}

		res += ":"

		// write value
		switch valType {
		case VtBool:
			res += strconv.FormatBool(pair.GetBool(dataKey))

		case VtInt8:
			res += strconv.FormatInt(int64(pair.GetInt8(dataKey)), 10)

		case VtInt16:
			res += strconv.FormatInt(int64(pair.GetInt16(dataKey)), 10)

		case VtInt32:
			res += strconv.FormatInt(int64(pair.GetInt32(dataKey)), 10)

		case VtInt64:
			res += strconv.FormatInt(pair.GetInt64(dataKey), 10)

		case VtFloat:
			res += strconv.FormatFloat(float64(pair.GetFloat(dataKey)), 'f', -1, 32)

		case VtDouble:
			res += strconv.FormatFloat(pair.GetDouble(dataKey), 'f', -1, 64)

		case VtString:
			res += pair.GetString(dataKey)

		case VtSlice:
			// GetSlice already handles DecodeSlice error internally
			opSlice := pair.GetSlice(dataKey)
			if opSlice != nil {
				res += opSlice.String(prettify)
			} else {
				res += "nil" // Or indicate error if GetSlice returned nil due to error
			}

		case VtMap:
			// GetMap will handle DecodeMap error internally
			opMap := pair.GetMap(dataKey) // Keep as is for now
			if opMap != nil {
				res += opMap.String(prettify)
			} else {
				res += "nil" // Or indicate error
			}

		case VtStructure:
			// GetStructure will handle DecodeStructure error internally
			opStr := pair.GetStructure(dataKey) // Keep as is for now
			if opStr != nil {
				res += opStr.String(prettify)
			} else {
				res += "nil" // Or indicate error
			}

		default:
		}
		if i < num-1 {
			if prettify {
				res += ",\n"
			} else {
				res += ", "
			}
		}
		i++
	}
	res += "}"

	return res
}

// map bytes:
/*
	head:
		data len code 	(1 byte)
		data len 		(variable-length bytes)
	body:
		key data type 	(1 byte)
		value data type (1 byte)
		data 			(variable-length bytes)
*/

// EncodeMap encodes a Go map into opio map format bytes.
// Returns headLen, rawData, error.
func EncodeMap(value interface{}) (int, []byte, error) {
	if nil == value {
		// Return empty binary for nil input
		return 0, MakeEmptyBinary(), nil
	}
	rv := reflect.Indirect(reflect.ValueOf(value))

	// Check if the input is a map
	rvKind := rv.Kind()
	if rvKind != reflect.Map {
		return 0, nil, fmt.Errorf("EncodeMap: input value is not a map, type is %v", rvKind)
	}

	// Determine key and value types
	rt := rv.Type()
	keyKind := rt.Key().Kind()
	valKind := rt.Elem().Kind()

	mapKeys := rv.MapKeys()
	if 0 == len(mapKeys) {
		// Return empty binary for empty map
		return 0, MakeEmptyBinary(), nil
	}

	// Check if key type is supported
	keyType, ok := opMapKeyTypeMap[keyKind]
	if !ok {
		return 0, nil, fmt.Errorf("EncodeMap: unsupported map key type: %v", keyKind)
	}
	// Bool keys are not supported
	if VtBool == keyType {
		return 0, nil, fmt.Errorf("EncodeMap: bool type key is not supported")
	}
	// Check if value type is supported
	valType, ok := wholeReflectTypeMap[valKind]
	if !ok {
		return 0, nil, fmt.Errorf("EncodeMap: unsupported map value type: %v", valKind)
	}

	bodyBuff := utils.MBuffer{}

	_, _ = bodyBuff.Write([]byte{byte(keyType)}) // Write KEY type
	_, _ = bodyBuff.Write([]byte{byte(valType)}) // Write VALUE type

	var raw []byte
	var err error
	var headLen int // To receive headLen from Encode* calls

	for _, kVal := range mapKeys {
		// Append key
		switch keyKind {
		case reflect.Int8:
			_, _ = bodyBuff.Write(utils.Int8ToByte(int8(kVal.Int())))

		case reflect.Uint8:
			_, _ = bodyBuff.Write(utils.UInt8ToByte(uint8(kVal.Uint())))

		case reflect.Int16:
			_, _ = bodyBuff.Write(utils.Int16ToByte(int16(kVal.Int())))

		case reflect.Uint16:
			_, _ = bodyBuff.Write(utils.UInt16ToByte(uint16(kVal.Uint())))

		case reflect.Int32:
			_, _ = bodyBuff.Write(utils.Int32ToByte(int32(kVal.Int())))

		case reflect.Uint32:
			_, _ = bodyBuff.Write(utils.UInt32ToByte(uint32(kVal.Uint())))

		case reflect.Int:
			fallthrough
		case reflect.Int64:
			_, _ = bodyBuff.Write(utils.Int64ToByte(kVal.Int()))

		case reflect.Uint:
			fallthrough
		case reflect.Uint64:
			_, _ = bodyBuff.Write(utils.UInt64ToByte(kVal.Uint()))

		case reflect.Float32:
			_, _ = bodyBuff.Write(utils.Float32ToByte(float32(kVal.Float())))

		case reflect.Float64:
			_, _ = bodyBuff.Write(utils.Float64ToByte(kVal.Float()))

		case reflect.String:
			_, raw = utils.PutString(kVal.String()) // PutString returns (int, []byte)
			_, _ = bodyBuff.Write(raw)
		}

		// Append value
		val := rv.MapIndex(kVal)
		raw = nil // Reset raw before encoding value
		err = nil // Reset err

		switch valKind {
		case reflect.Int8:
			_, _ = bodyBuff.Write(utils.Int8ToByte(int8(val.Int())))

		case reflect.Uint8:
			_, _ = bodyBuff.Write(utils.UInt8ToByte(uint8(val.Uint())))

		case reflect.Int16:
			_, _ = bodyBuff.Write(utils.Int16ToByte(int16(val.Int())))

		case reflect.Uint16:
			_, _ = bodyBuff.Write(utils.UInt16ToByte(uint16(val.Uint())))

		case reflect.Int32:
			_, _ = bodyBuff.Write(utils.Int32ToByte(int32(val.Int())))

		case reflect.Uint32:
			_, _ = bodyBuff.Write(utils.UInt32ToByte(uint32(val.Uint())))

		case reflect.Int:
			fallthrough
		case reflect.Int64:
			_, _ = bodyBuff.Write(utils.Int64ToByte(val.Int()))

		case reflect.Uint:
			fallthrough
		case reflect.Uint64:
			_, _ = bodyBuff.Write(utils.UInt64ToByte(val.Uint()))

		case reflect.Float32:
			_, _ = bodyBuff.Write(utils.Float32ToByte(float32(val.Float())))

		case reflect.Float64:
			_, _ = bodyBuff.Write(utils.Float64ToByte(val.Float()))

		case reflect.String:
			_, raw = utils.PutString(val.String()) // PutString returns (int, []byte)
			_, _ = bodyBuff.Write(raw)

		case reflect.Array:
			fallthrough
		case reflect.Slice:
			headLen, raw, err = EncodeSlice(val.Interface()) // Now returns error
			if err != nil {
				return 0, nil, fmt.Errorf("EncodeMap: encoding slice value for key %v failed: %w", kVal, err)
			}
			_, _ = bodyBuff.Write(raw)

		case reflect.Map:
			// Recursive call, handle error
			var tmpHeadLen int
			var tmpRaw []byte
			var tmpErr error
			tmpHeadLen, tmpRaw, tmpErr = EncodeMap(val.Interface())
			// 恢复为 =，因为 := 是不正确的
			headLen, raw, err = tmpHeadLen, tmpRaw, tmpErr // Assign back to loop variables
			if err != nil {
				return 0, nil, fmt.Errorf("EncodeMap: encoding map value for key %v failed: %w", kVal, err)
			}
			_, _ = bodyBuff.Write(raw)
		case reflect.Struct:
			// EncodeStructure now returns (int, []byte, error). Handle the error.
			// Use temporary variables inside case block.
			var tmpHeadLen int
			var tmpRaw []byte
			var tmpErr error
			tmpHeadLen, tmpRaw, tmpErr = EncodeStructure(val.Interface())
			headLen, raw, err = tmpHeadLen, tmpRaw, tmpErr // Assign back to loop variables

			if err != nil {
				return 0, nil, fmt.Errorf("EncodeMap: encoding struct value for key %v failed: %w", kVal, err)
			}
			_, _ = bodyBuff.Write(raw)
		}
	}

	// Write head and body
	var rawData []byte = nil
	// 使用外部声明的 headLen，而不是重新声明
	headLen = 0
	bodyLen := bodyBuff.Len()

	switch {
	case bodyLen < 0x100:
		headLen = 2
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin8        // BODY 长度编码 (1 byte)
		rawData[1] = byte(bodyLen) // BODY 长度

	case bodyLen < 0x10000:
		headLen = 3
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin16            // BODY 长度编码 (2 bytes)
		rawData[1] = byte(bodyLen >> 8) // BODY 长度
		rawData[2] = byte(bodyLen)

	case bodyLen < 0x10000000:
		headLen = 5
		rawData = make([]byte, headLen+bodyLen)
		rawData[0] = mpBin32             // BODY 长度编码 (4 bytes)
		rawData[1] = byte(bodyLen >> 24) // BODY 长度
		rawData[2] = byte(bodyLen >> 16)
		rawData[3] = byte(bodyLen >> 8)
		rawData[4] = byte(bodyLen)
	}
	if 0 == headLen || rawData == nil {
		// Head creation failed
		return 0, nil, fmt.Errorf("EncodeMap: failed to create head, bodyLen:%v", bodyLen)
	}

	copy(rawData[headLen:], bodyBuff.Bytes())

	return headLen, rawData, nil
}

// DecodeMap decodes opio map format bytes into an OPMap object.
// Returns *OPMap, error.
func DecodeMap(src []byte) (*OPMap, error) {
	// Handle empty binary
	if IsEmptyBinary(src) {
		return nil, nil // Return nil object and nil error for empty binary
	}

	srcLen := len(src)

	// 复制全部数据
	data := make([]byte, srcLen)
	copy(data, src)

	res := &OPMap{}
	res.SetData(data)

	offset := res.GetHeadLen()

	// Get KEY type
	keyType := int8(src[offset])
	// Get VALUE type
	valType := int8(src[offset+1])
	// Move offset past keyType and valType
	offset += 2

	// Set types in result object
	res.keyType = keyType
	res.valType = valType

	bodyLen := res.GetBodyLen()
	// If body only contains type info, it's an empty map
	if minMapBodyLen == bodyLen {
		return res, nil
	}

	// Data body start position
	dataStart := offset

	var pair OPPair = nil

	// 根据 key 类型解析出 Key 和其对应 value 在 data 中的位置
	switch keyType {
	case VtInt8:
		fallthrough
	case VtInt16:
		fallthrough
	case VtInt32:
		fallthrough
	case VtInt64:
		pair = newOPIntPair(data, keyType, valType, dataStart)

	case VtFloat:
		fallthrough
	case VtDouble:
		pair = newOPFloatPair(data, keyType, valType, dataStart)

	case VtString:
		pair = newOPStringPair(data, keyType, valType, dataStart)
	}
	if nil == pair {
		return nil, fmt.Errorf("DecodeMap: failed to initialize map pair, keyType:%v", keyType)
	}
	res.pair = pair
	return res, nil
}
