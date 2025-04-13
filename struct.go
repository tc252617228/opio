package opio

import (
	"fmt" // GetDouble, GetFloat 等方法中使用了 fmt.Errorf
	"reflect"
	"strconv"

	// time 包不再需要，移除

	"opio/internal/utils"
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
	// Handle error from DecodeSlice, ignore for getter simplicity
	opSlice, _ := DecodeSlice(valBytes)
	return opSlice
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
	// Handle error from DecodeMap, ignore for getter simplicity
	opMap, _ := DecodeMap(valBytes)
	return opMap
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
	// Handle error from DecodeStructure, ignore for getter simplicity
	opStruct, _ := DecodeStructure(valBytes)
	return opStruct
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
		// Handle error from DecodeSlice, ignore for getter simplicity
		opSlice, _ := DecodeSlice(valBytes)
		return opSlice

	case VtMap:
		// Handle error from DecodeMap, ignore for getter simplicity
		opMap, _ := DecodeMap(valBytes)
		return opMap

	case VtStructure:
		// Handle error from DecodeStructure, ignore for getter simplicity
		opStruct, _ := DecodeStructure(valBytes)
		return opStruct
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
			// Handle error from DecodeSlice, ignore for Range simplicity
			opSlice, _ := DecodeSlice(raw)
			if !f(dataKey, opSlice) {
				breakLoop = true
				break
			}

		case VtMap:
			// Handle error from DecodeMap, ignore for Range simplicity
			opMap, _ := DecodeMap(raw)
			if !f(dataKey, opMap) {
				breakLoop = true
				break
			}

		case VtStructure:
			// Handle error from DecodeStructure, ignore for Range simplicity
			opStruct, _ := DecodeStructure(raw)
			if !f(dataKey, opStruct) {
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
			// Handle error from DecodeSlice, ignore for String simplicity
			opSlice, _ := DecodeSlice(raw)
			if opSlice != nil {
				res += opSlice.String(prettify)
			} else {
				res += "nil" // Or indicate error
			}

		case VtMap:
			// Handle error from DecodeMap, ignore for String simplicity
			opMap, _ := DecodeMap(raw)
			if opMap != nil {
				res += opMap.String(prettify)
			} else {
				res += "nil" // Or indicate error
			}

		case VtStructure:
			// Handle error from DecodeStructure, ignore for String simplicity
			opStruct, _ := DecodeStructure(raw)
			if opStruct != nil {
				res += opStruct.String(prettify)
			} else {
				res += "nil" // Or indicate error
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

// EncodeStructure encodes a Go struct into opio structure format bytes.
// Returns headLen, rawData, error.
func EncodeStructure(value interface{}) (int, []byte, error) {
	if nil == value {
		return 0, MakeEmptyBinary(), nil
	}
	rv := reflect.Indirect(reflect.ValueOf(value))
	rvKind := rv.Kind()
	if rvKind != reflect.Struct {
		return 0, nil, fmt.Errorf("EncodeStructure: value is not a struct, type is %v", rvKind)
	}
	rt := rv.Type()

	fieldNum := rv.NumField()
	if 0 == fieldNum {
		return 0, MakeEmptyBinary(), nil
	}

	// Body buffer
	bodyBuff := utils.MBuffer{}
	fieldBuff := utils.MBuffer{}

	var raw []byte
	var err error
	var headLen int // To receive headLen from Encode* calls

	for i := 0; i < fieldNum; i++ {
		// field follows TLV principle

		field := rt.Field(i)
		val := rv.Field(i)

		valKind := val.Kind()
		valType, ok := wholeReflectTypeMap[valKind]
		if !ok {
			return 0, nil, fmt.Errorf("EncodeStructure: unsupported field type: %v for field %s", valKind, field.Name)
		}

		// Reset fieldBuff
		fieldBuff.Reset()

		/*
		   name len                (variable-length bytes)
		   name                    (variable-length bytes)
		*/
		fieldName := field.Name
		nameLen := len(fieldName)
		if nameLen > 0xFF { // Max length for uint8
			return 0, nil, fmt.Errorf("EncodeStructure: field name too long (max 255): %s", fieldName)
		}
		// Write field name length (1 byte)
		_, _ = fieldBuff.Write([]byte{byte(nameLen)})
		// Write field name
		_, _ = fieldBuff.Write(utils.StringToBytes(fieldName))
		/*
		   字段标签长度                 (1 byte)(可能存在)
		   字段标签                     (variable-length bytes)(可能存在)
		*/
		fieldTag := field.Tag
		tagLen := len(fieldTag)
		if tagLen > 0 {
			if tagLen > 0xFF { // Max length for uint8
				return 0, nil, fmt.Errorf("EncodeStructure: field tag too long (max 255) for field %s: %s", fieldName, fieldTag)
			}
			// Write tag flag (1 = present)
			_, _ = fieldBuff.Write([]byte{1})
			// Write tag length (1 byte)
			_, _ = fieldBuff.Write([]byte{byte(tagLen)})
			// Write tag
			_, _ = fieldBuff.Write(utils.StringToBytes(string(fieldTag)))
		} else {
			// Write tag flag (0 = absent)
			_, _ = fieldBuff.Write([]byte{0})
		}

		// Write value
		raw = nil // Reset raw
		err = nil // Reset err
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
			_, raw = utils.PutString(val.String())
			_, _ = fieldBuff.Write(raw)

		case reflect.Array:
			fallthrough
		case reflect.Slice:
			headLen, raw, err = EncodeSlice(val.Interface()) // Returns error
			if err != nil {
				return 0, nil, fmt.Errorf("EncodeStructure: encoding slice field %s failed: %w", fieldName, err)
			}
			_, _ = fieldBuff.Write(raw)

		case reflect.Map:
			headLen, raw, err = EncodeMap(val.Interface()) // Returns error
			if err != nil {
				return 0, nil, fmt.Errorf("EncodeStructure: encoding map field %s failed: %w", fieldName, err)
			}
			_, _ = fieldBuff.Write(raw)

		case reflect.Struct:
			// Recursive call, handle error
			headLen, raw, err = EncodeStructure(val.Interface())
			if err != nil {
				return 0, nil, fmt.Errorf("EncodeStructure: encoding struct field %s failed: %w", fieldName, err)
			}
			_, _ = fieldBuff.Write(raw)
		}

		/*
		   写入一个字段
		*/
		// 写入字段数据类型
		_, _ = bodyBuff.Write(utils.Int8ToByte(valType))

		// 写入字段长度
		fieldBytes := fieldBuff.Bytes()
		fieldLen := len(fieldBytes)
		// Write field length encoding and length
		switch {
		case fieldLen < 0x100:
			_, _ = bodyBuff.Write([]byte{mpBin8, byte(fieldLen)})
		case fieldLen < 0x10000:
			_, _ = bodyBuff.Write([]byte{mpBin16, byte(fieldLen >> 8), byte(fieldLen)})
		case fieldLen < 0x10000000:
			_, _ = bodyBuff.Write([]byte{mpBin32, byte(fieldLen >> 24), byte(fieldLen >> 16), byte(fieldLen >> 8), byte(fieldLen)})
		default:
			// Handle potential error for extremely large fields if necessary
			return 0, nil, fmt.Errorf("EncodeStructure: field %s is too large (%d bytes)", fieldName, fieldLen)
		}
		// Write field value bytes
		_, _ = bodyBuff.Write(fieldBytes)
	}

	// 写入头部和主体
	bodyBytes := bodyBuff.Bytes()
	bodyLen := len(bodyBytes)
	if 0 == bodyLen {
		// This case should ideally not happen if fieldNum > 0, but handle defensively
		return 0, MakeEmptyBinary(), nil // Return empty binary for empty body
	}

	var rawData []byte = nil
	headLen = 0 // Reset headLen for the final structure encoding
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
	if 0 == headLen || rawData == nil {
		return 0, nil, fmt.Errorf("EncodeStructure: failed to create head, bodyLen: %v", bodyLen)
	}

	copy(rawData[headLen:], bodyBuff.Bytes())

	return headLen, rawData, nil
}

// DecodeStructure decodes opio structure format bytes into an OPStructure object.
// Returns *OPStructure, error.
func DecodeStructure(src []byte) (*OPStructure, error) {
	// Handle empty binary
	if IsEmptyBinary(src) {
		return nil, nil // Return nil object and nil error for empty binary
	}

	srcLen := len(src)
	var offset int // Re-declare offset

	// 长度编码 (bodyLenCode is no longer used)
	// bodyLenCode := uint8(src[0])
	// offset++

	// Decode head to get body length and head length
	bodyLen, headLen, err := DecodeHead(src)
	if err != nil {
		return nil, fmt.Errorf("DecodeStructure: failed to decode head: %w", err)
	}
	// Basic validation
	if headLen+bodyLen != srcLen {
		return nil, fmt.Errorf("DecodeStructure: inconsistent length information (headLen=%d, bodyLen=%d, srcLen=%d)", headLen, bodyLen, srcLen)
	}
	if bodyLen == 0 {
		// Empty struct body
		opStructure := &OPStructure{}
		opStructure.SetData(src) // Set original data even if body is empty
		opStructure.fields = make(map[string]*OPField)
		return opStructure, nil
	}

	offset = headLen // Start decoding from the beginning of the body

	// Copy source data
	data := make([]byte, srcLen)
	copy(data, src)

	opStructure := &OPStructure{}
	opStructure.SetData(data) // Set the copied data

	dataStart := offset
	// decodeOPFields might need error handling in the future
	fields, err := decodeOPFields(data, dataStart, srcLen) // Pass total length for bounds checking
	if err != nil {
		return nil, fmt.Errorf("DecodeStructure: failed to decode fields: %w", err)
	}
	opStructure.fields = fields
	return opStructure, nil
}

// decodeOPFields decodes the fields from the structure body.
// Returns map[string]*OPField, error.
func decodeOPFields(src []byte, dataStart int, srcTotalLen int) (map[string]*OPField, error) {
	if dataStart >= srcTotalLen && srcTotalLen > 0 { // Allow empty source if dataStart is 0
		// Nothing to decode if start offset is already at or beyond the end
		return make(map[string]*OPField), nil
	}
	if dataStart < 0 {
		return nil, fmt.Errorf("invalid dataStart offset: %d", dataStart)
	}

	res := make(map[string]*OPField, 16) // Start with a smaller capacity

	offset := dataStart
	for offset < srcTotalLen {
		// Decode field type
		if offset >= srcTotalLen {
			return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading field type", offset, srcTotalLen)
		}
		fieldType := int8(src[offset])
		offset++

		// Get fixed length if applicable
		fixedLen, isFixed := fixedTypeLenMap[fieldType]

		// Decode field length code
		if offset >= srcTotalLen {
			return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading field length code", offset, srcTotalLen)
		}
		fieldLenCode := uint8(src[offset])
		offset++

		// Decode field length based on code
		fieldDataLen := 0
		switch fieldLenCode {
		case mpBin8:
			if offset >= srcTotalLen {
				return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading field length (mpBin8)", offset, srcTotalLen)
			}
			fieldDataLen = int(src[offset])
			offset++
		case mpBin16:
			if offset+1 >= srcTotalLen {
				return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading field length (mpBin16)", offset, srcTotalLen)
			}
			fieldDataLen = int(src[offset])<<8 | int(src[offset+1])
			offset += 2
		case mpBin32:
			if offset+3 >= srcTotalLen {
				return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading field length (mpBin32)", offset, srcTotalLen)
			}
			fieldDataLen = int(src[offset])<<24 | int(src[offset+1])<<16 | int(src[offset+2])<<8 | int(src[offset+3])
			offset += 4
		default:
			return nil, fmt.Errorf("invalid field length code: %x at offset %d", fieldLenCode, offset-1)
		}

		// Check if the decoded field length makes sense
		fieldStartOffset := offset // Start of the actual field data (name, tag, value)
		if fieldStartOffset+fieldDataLen > srcTotalLen {
			return nil, fmt.Errorf("field data length (%d) exceeds source bounds (%d) at offset %d", fieldDataLen, srcTotalLen, fieldStartOffset)
		}

		// Decode field name
		if offset >= srcTotalLen {
			return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading name length", offset, srcTotalLen)
		}
		nameLen := int(src[offset])
		offset++
		if offset+nameLen > srcTotalLen {
			return nil, fmt.Errorf("offset (%d + %d) out of bounds (%d) when reading name", offset, nameLen, srcTotalLen)
		}
		fieldName := utils.BytesToString(src[offset : offset+nameLen])
		offset += nameLen

		// Decode field tag
		if offset >= srcTotalLen {
			return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading tag flag", offset, srcTotalLen)
		}
		tagFlag := int8(src[offset])
		offset++
		fieldTag := ""
		if tagFlag == 1 {
			if offset >= srcTotalLen {
				return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading tag length", offset, srcTotalLen)
			}
			tagLen := int(src[offset])
			offset++
			if offset+tagLen > srcTotalLen {
				return nil, fmt.Errorf("offset (%d + %d) out of bounds (%d) when reading tag", offset, tagLen, srcTotalLen)
			}
			fieldTag = utils.BytesToString(src[offset : offset+tagLen])
			offset += tagLen
		} else if tagFlag != 0 {
			return nil, fmt.Errorf("invalid tag flag: %d at offset %d", tagFlag, offset-1)
		}

		// Decode value position and length
		// dataPos is the start of the value's encoded representation within the field data
		dataPos := offset
		valLen := 0 // This is the length of the *encoded* value (including head for var-len types)

		if isFixed {
			valLen = int(fixedLen)
			if offset+valLen > fieldStartOffset+fieldDataLen { // Check against the field boundary
				return nil, fmt.Errorf("fixed value length (%d) exceeds field data bounds for field %s", valLen, fieldName)
			}
			offset += valLen
		} else {
			// For variable length types, the value itself is encoded (head + body)
			// We need to determine the total length of this encoded value.
			if offset >= fieldStartOffset+fieldDataLen {
				return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading variable value head for field %s", offset, fieldStartOffset+fieldDataLen, fieldName)
			}
			valHeadLen := 0
			valBodyLen := 0
			valLenCode := src[offset]
			offset++
			valHeadLen++

			switch valLenCode {
			case mpBin8:
				if offset >= fieldStartOffset+fieldDataLen {
					return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading variable value length (mpBin8) for field %s", offset, fieldStartOffset+fieldDataLen, fieldName)
				}
				valBodyLen = int(src[offset])
				offset++
				valHeadLen++
			case mpBin16:
				if offset+1 >= fieldStartOffset+fieldDataLen {
					return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading variable value length (mpBin16) for field %s", offset, fieldStartOffset+fieldDataLen, fieldName)
				}
				valBodyLen = int(src[offset])<<8 | int(src[offset+1])
				offset += 2
				valHeadLen += 2
			case mpBin32:
				if offset+3 >= fieldStartOffset+fieldDataLen {
					return nil, fmt.Errorf("offset (%d) out of bounds (%d) when reading variable value length (mpBin32) for field %s", offset, fieldStartOffset+fieldDataLen, fieldName)
				}
				valBodyLen = int(src[offset])<<24 | int(src[offset+1])<<16 | int(src[offset+2])<<8 | int(src[offset+3])
				offset += 4
				valHeadLen += 4
			default:
				// Check for simple types like Nil, True, False if applicable, otherwise error
				// Assuming only mpBin* are valid heads for var-len types here.
				return nil, fmt.Errorf("invalid variable value length code: %x for field %s at offset %d", valLenCode, fieldName, offset-1)
			}

			valLen = valHeadLen + valBodyLen
			if dataPos+valLen > fieldStartOffset+fieldDataLen { // Check against the field boundary
				return nil, fmt.Errorf("variable value length (%d) exceeds field data bounds for field %s", valLen, fieldName)
			}
			// Move offset past the value body
			offset += valBodyLen
		}

		// Ensure the final offset for this field matches the expected end based on fieldDataLen
		expectedFieldEndOffset := fieldStartOffset + fieldDataLen
		if offset != expectedFieldEndOffset {
			return nil, fmt.Errorf("field %s decoding finished at unexpected offset %d (expected %d)", fieldName, offset, expectedFieldEndOffset)
		}

		// Create OPField
		field := &OPField{
			Name:    fieldName,
			Tag:     reflect.StructTag(fieldTag),
			Type:    fieldType,
			pos:     dataPos, // Position within the original src byte slice where the value starts
			dataLen: valLen,  // Length of the encoded value (head+body for var-len)
		}
		if _, exists := res[fieldName]; exists {
			return nil, fmt.Errorf("duplicate field name detected: %s", fieldName)
		}
		res[fieldName] = field
	}

	// Final check: ensure we consumed the entire relevant part of the source buffer
	if offset != srcTotalLen {
		return nil, fmt.Errorf("decoding finished at offset %d, but expected end was %d", offset, srcTotalLen)
	}

	return res, nil
}

// DecodeHead is a helper to decode the common head format (len code + len)
// Returns bodyLen, headLen, error
func DecodeHead(src []byte) (bodyLen int, headLen int, err error) {
	srcLen := len(src)
	if srcLen < 1 {
		return 0, 0, fmt.Errorf("DecodeHead: source is empty")
	}

	lenCode := src[0]
	headLen = 1 // Start with the lenCode byte

	switch lenCode {
	case mpBin8:
		if srcLen < 2 {
			return 0, 0, fmt.Errorf("DecodeHead: insufficient data for mpBin8 length (need 2, got %d)", srcLen)
		}
		bodyLen = int(src[1])
		headLen += 1
	case mpBin16:
		if srcLen < 3 {
			return 0, 0, fmt.Errorf("DecodeHead: insufficient data for mpBin16 length (need 3, got %d)", srcLen)
		}
		bodyLen = int(src[1])<<8 | int(src[2])
		headLen += 2
	case mpBin32:
		if srcLen < 5 {
			return 0, 0, fmt.Errorf("DecodeHead: insufficient data for mpBin32 length (need 5, got %d)", srcLen)
		}
		bodyLen = int(src[1])<<24 | int(src[2])<<16 | int(src[3])<<8 | int(src[4])
		headLen += 4
	// Add cases for other potential single-byte codes if necessary (e.g., Nil, True, False)
	// case mpNil: bodyLen = 0; headLen = 1;
	default:
		// Check if it's a fixed-length type code or other valid single-byte code?
		// For now, assume only mpBin* are valid heads for data containers.
		return 0, 0, fmt.Errorf("DecodeHead: invalid length code: %x", lenCode)
	}

	// Validate calculated total length against source length
	if headLen+bodyLen > srcLen {
		// This check might be redundant if DecodeStructure already does it, but good for safety.
		// return 0, 0, fmt.Errorf("DecodeHead: calculated length (head=%d, body=%d) exceeds source length (%d)", headLen, bodyLen, srcLen)
	}

	return bodyLen, headLen, nil
}
