package opio

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/tc252617228/opio/driver/internal/utils"
)

type OPField struct {
	Name    string
	Tag     reflect.StructTag
	Type    int8
	pos     int
	dataLen int
}

type OPStructure struct {
	utils.BytesBase
	fields map[string]*OPField // first:fieldName, second:*OPField
}

func (ops *OPStructure) IsEmpty() bool {
	return 0 == len(ops.fields) || 0 == len(ops.GetData())
}

func (ops *OPStructure) GetAllField() []*OPField {
	fields := ops.fields
	fieldNum := len(fields)
	if 0 == fieldNum {
		return nil
	}

	res := make([]*OPField, fieldNum)
	i := 0
	for _, val := range fields {
		res[i] = val
		i++
	}
	return res
}

func (ops *OPStructure) GetField(fieldName string) *OPField {
	fields := ops.fields
	if 0 == len(fields) {
		return nil
	}
	field, ok := fields[fieldName]
	if !ok {
		return nil
	}
	return field
}

func (ops *OPStructure) GetBool(fieldName string) bool {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return false
	}
	field, ok := fields[fieldName]
	if !ok {
		return false
	}
	if field.Type != VtBool {
		return false
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	val := utils.GetInt8(valBytes)
	return val > 0
}

func (ops *OPStructure) GetInt8(fieldName string) int8 {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return 0
	}
	field, ok := fields[fieldName]
	if !ok {
		return 0
	}
	if field.Type != VtInt8 {
		return 0
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return utils.GetInt8(valBytes)
}

func (ops *OPStructure) GetInt16(fieldName string) int16 {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return 0
	}
	field, ok := fields[fieldName]
	if !ok {
		return 0
	}
	if field.Type != VtInt16 {
		return 0
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return utils.GetInt16(valBytes)
}

func (ops *OPStructure) GetInt32(fieldName string) int32 {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return 0
	}
	field, ok := fields[fieldName]
	if !ok {
		return 0
	}
	if field.Type != VtInt32 {
		return 0
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return utils.GetInt32(valBytes)
}

func (ops *OPStructure) GetInt64(fieldName string) int64 {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return 0
	}
	field, ok := fields[fieldName]
	if !ok {
		return 0
	}
	if field.Type != VtInt64 {
		return 0
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return utils.GetInt64(valBytes)
}

func (ops *OPStructure) GetFloat(fieldName string) float32 {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return 0
	}
	field, ok := fields[fieldName]
	if !ok {
		return 0
	}
	if field.Type != VtFloat {
		return 0
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return utils.GetFloat32(valBytes)
}

func (ops *OPStructure) GetDouble(fieldName string) float64 {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return 0
	}
	field, ok := fields[fieldName]
	if !ok {
		return 0
	}
	if field.Type != VtDouble {
		return 0
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return utils.GetFloat64(valBytes)
}

func (ops *OPStructure) GetString(fieldName string) string {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return ""
	}
	field, ok := fields[fieldName]
	if !ok {
		return ""
	}
	if field.Type != VtString {
		return ""
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return utils.GetString(valBytes)
}

func (ops *OPStructure) GetSlice(fieldName string) *OPSlice {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return nil
	}
	field, ok := fields[fieldName]
	if !ok {
		return nil
	}
	if field.Type != VtSlice {
		return nil
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return DecodeSlice(valBytes)
}

func (ops *OPStructure) GetMap(fieldName string) *OPMap {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return nil
	}
	field, ok := fields[fieldName]
	if !ok {
		return nil
	}
	if field.Type != VtMap {
		return nil
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return DecodeMap(valBytes)
}

func (ops *OPStructure) GetStructure(fieldName string) *OPStructure {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return nil
	}
	field, ok := fields[fieldName]
	if !ok {
		return nil
	}
	if field.Type != VtStructure {
		return nil
	}
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]
	return DecodeStructure(valBytes)
}

func (ops *OPStructure) Get(fieldName string) interface{} {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return nil
	}
	field, ok := fields[fieldName]
	if !ok {
		return nil
	}
	fieldType := field.Type
	pos := field.pos
	valLen := field.dataLen
	valBytes := data[pos : pos+valLen]

	switch fieldType {
	case VtBool:
		val := utils.GetInt8(valBytes)
		return val > 0

	case VtInt8:
		return utils.GetInt8(valBytes)

	case VtInt16:
		return utils.GetInt16(valBytes)

	case VtInt32:
		return utils.GetInt32(valBytes)

	case VtInt64:
		return utils.GetInt64(valBytes)

	case VtFloat:
		return utils.GetFloat32(valBytes)

	case VtDouble:
		return utils.GetFloat64(valBytes)

	case VtString:
		return utils.GetString(valBytes)

	case VtSlice:
		return DecodeSlice(valBytes)

	case VtMap:
		return DecodeMap(valBytes)

	case VtStructure:
		return DecodeStructure(valBytes)
	}
	return nil
}

func (ops *OPStructure) Range(f func(key string, value interface{}) bool) {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return
	}
	breakLoop := false
	for dataKey, field := range fields {
		fieldType := field.Type
		pos := field.pos
		valLen := field.dataLen

		raw := data[pos : pos+valLen]

		switch fieldType {
		case VtBool:
			val := utils.GetInt8(raw)
			if val > 0 {
				if !f(dataKey, true) {
					breakLoop = true
					break
				}
			} else {
				if !f(dataKey, false) {
					breakLoop = true
					break
				}
			}

		case VtInt8:
			if !f(dataKey, utils.GetInt8(raw)) {
				breakLoop = true
				break
			}

		case VtInt16:
			if !f(dataKey, utils.GetInt16(raw)) {
				breakLoop = true
				break
			}

		case VtInt32:
			if !f(dataKey, utils.GetInt32(raw)) {
				breakLoop = true
				break
			}

		case VtInt64:
			if !f(dataKey, utils.GetInt64(raw)) {
				breakLoop = true
				break
			}

		case VtFloat:
			if !f(dataKey, utils.GetFloat32(raw)) {
				breakLoop = true
				break
			}

		case VtDouble:
			if !f(dataKey, utils.GetFloat64(raw)) {
				breakLoop = true
				break
			}

		case VtString:
			if !f(dataKey, utils.GetString(raw)) {
				breakLoop = true
				break
			}

		case VtSlice:
			if !f(dataKey, DecodeSlice(raw)) {
				breakLoop = true
				break
			}

		case VtMap:
			if !f(dataKey, DecodeMap(raw)) {
				breakLoop = true
				break
			}

		case VtStructure:
			if !f(dataKey, DecodeStructure(raw)) {
				breakLoop = true
				break
			}

		}
		if breakLoop {
			break
		}
	}
}

func (ops *OPStructure) String(prettify bool) string {
	fields := ops.fields
	data := ops.GetData()
	if 0 == len(fields) || 0 == len(data) {
		return "{}"
	}

	num := len(fields)
	i := 0

	res := "{"

	for _, field := range fields {
		// make simple object
		res += "{"

		// name
		res += "field : " + field.Name + ", "

		// tag
		res += "tag : " + field.Tag.Get("json") + ", "

		// value
		res += "value : "

		fieldType := field.Type
		pos := field.pos
		valLen := field.dataLen
		raw := data[pos : pos+valLen]

		switch fieldType {
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
		res += "}"
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

// struct bytes:
/*
   data len code    (1 byte)
   data len         (variable-length bytes)
   data 		    (variable-length bytes)
*/
/*
   struct field bytes:
   field type  (1 byte)
   name len    (1 byte)
   name        (variable-length bytes)
   tag flag    (1 byte)
   tag len     (1 byte)(possible)
   tag         (variable-length bytes)(possible)
   field value (variable-length bytes)
*/

func EncodeStructure(value interface{}) (int, []byte) {
	if nil == value {
		return 0, MakeEmptyBinary()
	}
	rv := reflect.Indirect(reflect.ValueOf(value))
	rvKind := rv.Kind()
	if rvKind != reflect.Struct {
		fmt.Println("value is not structure")
		return 0, nil
	}
	rt := rv.Type()

	fieldNum := rv.NumField()
	if 0 == fieldNum {
		return 0, MakeEmptyBinary()
	}

	// 数据体BUFFER
	bodyBuff := utils.MBuffer{}
	fieldBuff := utils.MBuffer{}

	for i := 0; i < fieldNum; i++ {
		// field依然遵循TLV原则

		field := rt.Field(i)
		val := rv.Field(i)

		valKind := val.Kind()
		valType, ok := wholeReflectTypeMap[valKind]
		if !ok {
			fmt.Println("unsupported value type:%v", valKind)
			return 0, nil
		}

		// 重置fieldBuff
		fieldBuff.Reset()

		/*
		   name len                (variable-length bytes)
		   name                    (variable-length bytes)
		*/
		fieldName := field.Name
		nameLen := len(fieldName)
		if nameLen > 0x100 {
			fmt.Println("name len too long, name:%v", fieldName)
			return 0, nil
		}
		// write name len
		_, _ = fieldBuff.Write(utils.Int8ToByte(int8(nameLen)))
		// write name
		_, _ = fieldBuff.Write(utils.StringToBytes(fieldName))
		/*
		   fieldTag len                 (1 byte)(possible)
		   fieldTag                     (variable-length bytes)(possible)
		*/
		fieldTag := field.Tag
		tagLen := len(fieldTag)
		if tagLen > 0 {
			if tagLen > 0x100 {
				fmt.Println("tag len too long, tagLen:%v", fieldTag)
				return 0, nil
			}
			// write tag flag
			_, _ = fieldBuff.Write([]byte{byte(1)})

			// write tag len
			_, _ = fieldBuff.Write(utils.Int8ToByte(int8(tagLen)))
			// write tag
			_, _ = fieldBuff.Write(utils.StringToBytes(string(fieldTag)))
		} else {
			// write tag flag
			_, _ = fieldBuff.Write([]byte{byte(0)})
		}

		// write value
		switch valKind {
		case reflect.Int8:
			_, _ = fieldBuff.Write(utils.Int8ToByte(int8(val.Int())))

		case reflect.Uint8:
			_, _ = fieldBuff.Write(utils.UInt8ToByte(uint8(val.Uint())))

		case reflect.Int16:
			_, _ = fieldBuff.Write(utils.Int16ToByte(int16(val.Int())))

		case reflect.Uint16:
			_, _ = fieldBuff.Write(utils.UInt16ToByte(uint16(val.Uint())))

		case reflect.Int32:
			_, _ = fieldBuff.Write(utils.Int32ToByte(int32(val.Int())))

		case reflect.Uint32:
			_, _ = fieldBuff.Write(utils.UInt32ToByte(uint32(val.Uint())))

		case reflect.Int:
			fallthrough
		case reflect.Int64:
			_, _ = fieldBuff.Write(utils.Int64ToByte(val.Int()))

		case reflect.Uint:
			fallthrough
		case reflect.Uint64:
			_, _ = fieldBuff.Write(utils.UInt64ToByte(val.Uint()))

		case reflect.Float32:
			_, _ = fieldBuff.Write(utils.Float32ToByte(float32(val.Float())))

		case reflect.Float64:
			_, _ = fieldBuff.Write(utils.Float64ToByte(val.Float()))

		case reflect.String:
			_, raw := utils.PutString(val.String())
			_, _ = fieldBuff.Write(raw)

		case reflect.Array:
			fallthrough
		case reflect.Slice:
			_, raw := EncodeSlice(val.Interface())
			_, _ = fieldBuff.Write(raw)

		case reflect.Map:
			_, raw := EncodeMap(val.Interface())
			_, _ = fieldBuff.Write(raw)

		case reflect.Struct:
			_, raw := EncodeStructure(val.Interface())
			_, _ = fieldBuff.Write(raw)

		}

		/*
		   write one field
		*/
		// write field data type
		_, _ = bodyBuff.Write(utils.Int8ToByte(valType))

		// write field len
		fieldBytes := fieldBuff.Bytes()
		fieldLen := len(fieldBytes)
		switch {
		case fieldLen < 0x100:
			_, _ = bodyBuff.Write(utils.UInt8ToByte(mpBin8))
			_, _ = bodyBuff.Write(utils.Int8ToByte(int8(fieldLen)))

		case fieldLen < 0x10000:
			_, _ = bodyBuff.Write(utils.UInt8ToByte(mpBin16))
			_, _ = bodyBuff.Write(utils.Int16ToByte(int16(fieldLen)))

		case fieldLen < 0x10000000:
			_, _ = bodyBuff.Write(utils.UInt8ToByte(mpBin32))
			_, _ = bodyBuff.Write(utils.Int32ToByte(int32(fieldLen)))
		}
		// write field value
		_, _ = bodyBuff.Write(fieldBytes)
	}

	// write head and body
	bodyBytes := bodyBuff.Bytes()
	bodyLen := len(bodyBytes)
	if 0 == bodyLen {
		fmt.Println("struct body is empty")
		return 0, nil
	}
	var rawData []byte = nil
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
	}
	if 0 == headLen {
		fmt.Println("make head failed, bodyLen:%v", bodyLen)
	}

	copy(rawData[headLen:], bodyBuff.Bytes())

	return headLen, rawData
}

func DecodeStructure(src []byte) *OPStructure {
	// 判断空值标记
	if IsEmptyBinary(src) {
		return nil
	}

	srcLen := len(src)
	offset := 0

	// len code
	bodyLenCode := uint8(src[0])
	offset++

	// 略过数据长度向前移动游标
	switch bodyLenCode {
	case mpBin8:
		offset++
	case mpBin16:
		offset += 2
	case mpBin32:
		offset += 4
	}
	// 取数据
	data := make([]byte, srcLen)
	copy(data, src)

	opStructure := &OPStructure{}
	opStructure.SetData(data)

	dataStart := offset
	fields := decodeOPFields(data, dataStart)
	opStructure.fields = fields
	return opStructure
}

func decodeOPFields(src []byte, dataStart int) map[string]*OPField {
	total := len(src)
	if 0 == total {
		return nil
	}

	res := make(map[string]*OPField, 1024)

	offset := dataStart
	for offset != total {
		// decode field data type
		fieldType := int8(src[offset])
		offset++

		// get value len
		fixedLen, isFixed := fixedTypeLenMap[fieldType]

		// decode field len code
		fieldLenCode := uint8(src[offset])
		offset++

		// 解field内容时无需解field长度，直接将offset按code向前移动
		switch fieldLenCode {
		case mpBin8:
			offset++
		case mpBin16:
			offset += 2
		case mpBin32:
			offset += 4
		}

		// decode name
		nameLen := int(src[offset])
		offset++
		fieldName := utils.BytesToString(src[offset : offset+nameLen])
		offset += nameLen

		// decode tag
		tagFlag := int8(src[offset])
		offset++
		fieldTag := ""
		if 1 == tagFlag {
			tagLen := int(src[offset])
			offset++
			fieldTag = utils.BytesToString(src[offset : offset+tagLen])
			offset += tagLen
		}

		// decode value pos
		// pos and len, pos:high 32bit, len:low 32bit
		// 记录位置
		dataPos := offset

		// 计算长度
		valLen := 0
		if isFixed {
			// value是固定长度的类型,offset直接跳过固定长度到下一个pair,posLen记录的只是位置，不记录长度，并且位置记录在低32位
			offset += int(fixedLen)
			valLen = int(fixedLen)
		} else {
			// value是变长类型,需要根据valType计算数据的长度,posLen记录的是位置+长度
			headLen := 0

			lenCode := src[offset]
			offset++
			headLen++

			dataLen := int(0)
			switch lenCode {
			case mpBin8:
				dataLen = int(src[offset])
				offset++
				headLen++
			case mpBin16:
				dataLen = int(src[offset])<<8 | int(src[offset+1])
				offset += 2
				headLen += 2
			case mpBin32:
				dataLen = int(src[offset])<<24 | int(src[offset+1])<<16 | int(src[offset+2])<<8 | int(src[offset+3])
				offset += 4
				headLen += 4
			}
			// 将 dataLen 合并到 posLen 中
			valLen = headLen + dataLen
			// skip to next field
			offset += dataLen
		}

		// generate OPField
		field := &OPField{
			Name:    fieldName,
			Tag:     reflect.StructTag(fieldTag),
			Type:    fieldType,
			pos:     dataPos,
			dataLen: valLen,
		}
		res[fieldName] = field
	}
	return res
}
