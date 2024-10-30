package opio

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/tc252617228/opio/internal/utils"
)

const (
	minMapBodyLen = 2 // body中只包含keyType和valType
)

type OPMap struct {
	utils.BytesBase
	keyType int8   // key的类型 , see "Value type" in environment/opio/driver/const.go
	valType int8   // value的类型 , see "Value type" in environment/opio/driver/const.go
	pair    OPPair // key记录器
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
	return opm.pair.GetMap(key)
}

func (opm *OPMap) FindStructure(key interface{}) *OPStructure {
	if opm.IsEmpty() {
		return nil
	}
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
		return pair.GetSlice(key)

	case VtMap:
		return pair.GetMap(key)

	case VtStructure:
		return pair.GetStructure(key)

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
			if !f(dataKey, pair.GetSlice(dataKey)) {
				breakLoop = true
				break
			}
		case VtMap:
			if !f(dataKey, pair.GetMap(dataKey)) {
				breakLoop = true
				break
			}
		case VtStructure:
			if !f(dataKey, pair.GetStructure(dataKey)) {
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
			opSlice := pair.GetSlice(dataKey)
			if opSlice != nil {
				res += opSlice.String(prettify)
			} else {
				res += "nil"
			}

		case VtMap:
			opMap := pair.GetMap(dataKey)
			if opMap != nil {
				res += opMap.String(prettify)
			} else {
				res += "nil"
			}

		case VtStructure:
			opStr := pair.GetStructure(dataKey)
			if opStr != nil {
				res += opStr.String(prettify)
			} else {
				res += "nil"
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

func EncodeMap(value interface{}) (int, []byte) {
	// fault return
	if nil == value {
		return 0, MakeEmptyBinary()
	}
	rv := reflect.Indirect(reflect.ValueOf(value))

	// fault return
	rvKind := rv.Kind()
	if rvKind != reflect.Map {
		fmt.Println("map value is not map, valueType:%v", rvKind)
		//logs.Error("map value is not map, valueType:%v", rvKind)
		return 0, nil
	}

	// make sure key and value type
	rt := rv.Type()
	keyKind := rt.Key().Kind()
	valKind := rt.Elem().Kind()

	mapKeys := rv.MapKeys()
	if 0 == len(mapKeys) {
		return 0, MakeEmptyBinary()
	}

	// 检查key的类型是否是内置类型,OPMap的key只支持内部数据类型
	keyType, ok := opMapKeyTypeMap[keyKind]
	if !ok {
		fmt.Println("unsupported map key type:%v", keyKind)
		//logs.Error("unsupported map key type:%v", keyKind)
		return 0, nil
	}
	if VtBool == keyType {
		fmt.Println("unsupported bool key type")
		//logs.Error("unsupported bool key type")
		return 0, nil
	}
	valType, ok := wholeReflectTypeMap[valKind]
	if !ok {
		fmt.Println("unsupported map value type:%v", valKind)
		//logs.Error("unsupported map value type:%v", valKind)
		return 0, nil
	}
	bodyBuff := utils.MBuffer{}

	_, _ = bodyBuff.Write([]byte{byte(keyType)}) // KEY类型
	_, _ = bodyBuff.Write([]byte{byte(valType)}) // VALUE类型

	for _, kVal := range mapKeys {
		// append key value
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
			_, raw := utils.PutString(kVal.String())
			_, _ = bodyBuff.Write(raw)
		}

		// append key value
		val := rv.MapIndex(kVal)
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
			_, raw := utils.PutString(val.String())
			_, _ = bodyBuff.Write(raw)

		case reflect.Array:
			fallthrough
		case reflect.Slice:
			_, raw := EncodeSlice(val.Interface())
			_, _ = bodyBuff.Write(raw)

		case reflect.Map:
			_, raw := EncodeMap(val.Interface())
			_, _ = bodyBuff.Write(raw)

		case reflect.Struct:
			_, raw := EncodeStructure(val.Interface())
			_, _ = bodyBuff.Write(raw)
		}
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
	if 0 == headLen {
		fmt.Println("make head failed, bodyLen:%v", bodyLen)
		//logs.Error("make head failed, bodyLen:%v", bodyLen)
	}

	copy(rawData[headLen:], bodyBuff.Bytes())

	return headLen, rawData
}

func DecodeMap(src []byte) *OPMap {
	// 对空值进行判断
	if IsEmptyBinary(src) {
		return nil
	}

	srcLen := len(src)

	// 全数据copy
	data := make([]byte, srcLen)
	copy(data, src)

	res := &OPMap{}
	res.SetData(data)

	offset := res.GetHeadLen()

	// 取KEY类型
	keyType := int8(src[offset])
	// 取VALUE类型
	valType := int8(src[offset+1])
	// 向前移动两个字节
	offset += 2

	// set
	res.keyType = keyType
	res.valType = valType

	bodyLen := res.GetBodyLen()
	if minMapBodyLen == bodyLen {
		return res
	}

	// 数据起始位置
	dataStart := offset

	var pair OPPair = nil

	// 解析出Key和其对应value在data中的位置
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
		fmt.Println("init map pair failed, keyType:%v", keyType)
		//logs.Error("init map pair failed, keyType:%v", keyType)
		return nil
	}
	res.pair = pair
	return res
}
