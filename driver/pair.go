package opio

import (
	"fmt"

	"github.com/tc252617228/opio/driver/internal/utils"
)

type OPPair interface {
	GetBool(key interface{}) bool

	GetInt8(key interface{}) int8

	GetInt16(key interface{}) int16

	GetInt32(key interface{}) int32

	GetInt64(key interface{}) int64

	GetFloat(key interface{}) float32

	GetDouble(key interface{}) float64

	GetString(key interface{}) string

	GetSlice(key interface{}) *OPSlice

	GetMap(key interface{}) *OPMap

	GetStructure(key interface{}) *OPStructure

	AllKeys() []interface{}
}

type opPairBase struct {
	src     []byte
	valType int8
}

// int-key
type opIntPair struct {
	opPairBase
	keyPos map[int64]int64 // first:real data key, second: pos+len
}

func newOPIntPair(src []byte, keyType, valType int8, dataStart int) *opIntPair {
	total := len(src)
	if 0 == total {
		fmt.Println("src is empty")
		//logs.Error("src is empty")
		return nil
	}
	if keyType != VtInt8 && keyType != VtInt16 && keyType != VtInt32 && keyType != VtInt64 {
		//logs.Error("unsupported integer key type:%v", keyType)
		fmt.Println("unsupported integer key type:%v", keyType)
		return nil
	}

	// get value len
	valFixedLen, isValFixed := fixedTypeLenMap[valType]

	offset := dataStart

	positions := make(map[int64]int64, 64*utils.KB)
	key := int64(0)
	for offset != total {

		// get key value
		switch keyType {
		case VtInt8:
			key = int64(src[offset])
			offset++

		case VtInt16:
			key = int64(utils.GetInt16(src[offset : offset+2]))
			offset += VtInt16Len

		case VtInt32:
			key = int64(utils.GetInt32(src[offset : offset+4]))
			offset += VtInt32Len

		case VtInt64:
			key = utils.GetInt64(src[offset : offset+8])
			offset += VtInt64Len
		}

		// pos and len, pos:high 32bit, len:low 32bit
		// 记录位置
		posLen := int64(offset)

		if isValFixed {
			// value是固定长度的类型,offset直接跳过固定长度到下一个pair,posLen记录的只是位置，不记录长度，并且位置记录在低32位
			offset += int(valFixedLen)
		} else {
			// value是变长类型,需要根据valType计算数据的长度,posLen记录的是位置+长度

			// 将位置信息移到高32位
			posLen = posLen << 32
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
			posLen = posLen | int64(headLen+dataLen)
			// skip to next pair
			offset += dataLen
		}
		positions[key] = posLen
	}

	if 0 == len(positions) {
		//logs.Error("data positions is empty")
		fmt.Println("data positions is empty")
		return nil
	}

	pair := &opIntPair{
		keyPos: positions,
	}
	pair.src = src
	pair.valType = valType

	return pair
}

func (pair *opIntPair) GetBool(key interface{}) bool {
	if pair.valType != VtBool {
		return false
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return false
	}

	keyData := parseInt(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return false
	}
	val := uint8(src[pos])
	return val > 0
}
func (pair *opIntPair) GetInt8(key interface{}) int8 {
	if pair.valType != VtInt8 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}

	keyData := parseInt(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return int8(src[pos])
}
func (pair *opIntPair) GetInt16(key interface{}) int16 {
	if pair.valType != VtInt16 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseInt(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt16(src[pos : pos+int64(VtInt16Len)])
}
func (pair *opIntPair) GetInt32(key interface{}) int32 {
	if pair.valType != VtInt32 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseInt(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt32(src[pos : pos+int64(VtInt32Len)])
}
func (pair *opIntPair) GetInt64(key interface{}) int64 {
	if pair.valType != VtInt64 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseInt(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt64(src[pos : pos+int64(VtInt64Len)])
}
func (pair *opIntPair) GetFloat(key interface{}) float32 {
	if pair.valType != VtFloat {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseInt(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetFloat32(src[pos : pos+int64(VtFloatLen)])
}
func (pair *opIntPair) GetDouble(key interface{}) float64 {
	if pair.valType != VtDouble {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseInt(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetFloat64(src[pos : pos+int64(VtDoubleLen)])
}
func (pair *opIntPair) GetString(key interface{}) string {
	if pair.valType != VtString {
		return ""
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return ""
	}
	keyData := parseInt(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return ""
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return utils.GetString(src[pos : pos+dataLen])
}
func (pair *opIntPair) GetSlice(key interface{}) *OPSlice {
	if pair.valType != VtSlice {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseInt(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeSlice(src[pos : pos+dataLen])
}
func (pair *opIntPair) GetMap(key interface{}) *OPMap {
	if pair.valType != VtMap {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseInt(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeMap(src[pos : pos+dataLen])
}

func (pair *opIntPair) GetStructure(key interface{}) *OPStructure {
	if pair.valType != VtStructure {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseInt(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeStructure(src[pos : pos+dataLen])
}

func (pair *opIntPair) AllKeys() []interface{} {
	if 0 == len(pair.keyPos) {
		return nil
	}
	res := make([]interface{}, len(pair.keyPos))
	i := 0
	for key := range pair.keyPos {
		res[i] = key
		i++
	}
	return res
}

//////////////////////////////////////////////////////////////////////////////////////////

// float-key
type opFloatPair struct {
	opPairBase
	keyPos map[float64]int64 // first:real data key, second: pos+len
}

func newOPFloatPair(src []byte, keyType, valType int8, dataStart int) *opFloatPair {
	total := len(src)
	if 0 == total {
		fmt.Println("src is empty")
		//logs.Error("src is empty")
		return nil
	}
	if keyType != VtFloat && keyType != VtDouble {
		fmt.Println("unsupported float key type:%v", keyType)
		//logs.Error("unsupported float key type:%v", keyType)
		return nil
	}

	// get value len
	valFixedLen, isValFixed := fixedTypeLenMap[valType]

	offset := dataStart

	positions := make(map[float64]int64, 64*utils.KB)
	key := float64(0)
	for offset != total {

		// get key value
		switch keyType {
		case VtFloat:
			key = float64(utils.GetFloat32(src[offset : offset+4]))
			offset += VtFloatLen

		case VtDouble:
			key = utils.GetFloat64(src[offset : offset+8])
			offset += VtDoubleLen
		}

		// pos and len, pos:high 32bit, len:low 32bit
		// 记录位置
		posLen := int64(offset)

		if isValFixed {
			// value是固定长度的类型,offset直接跳过固定长度到下一个pair,posLen记录的只是位置，不记录长度，并且位置记录在低32位
			offset += int(valFixedLen)
		} else {
			// value是变长类型,需要根据valType计算数据的长度,posLen记录的是位置+长度

			// 将位置信息移到高32位
			posLen = posLen << 32
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
			posLen = posLen | int64(headLen+dataLen)
			// skip to next pair
			offset += dataLen
		}
		positions[key] = posLen
	}

	if 0 == len(positions) {
		fmt.Println("data positions is empty")
		//logs.Error("data positions is empty")
		return nil
	}
	pair := &opFloatPair{
		keyPos: positions,
	}
	pair.src = src
	pair.valType = valType

	return pair
}

func (pair *opFloatPair) GetBool(key interface{}) bool {
	if pair.valType != VtBool {
		return false
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return false
	}

	keyData := parseFloat(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return false
	}
	val := uint8(src[pos])
	return val > 0
}
func (pair *opFloatPair) GetInt8(key interface{}) int8 {
	if pair.valType != VtInt8 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}

	keyData := parseFloat(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return int8(src[pos])
}
func (pair *opFloatPair) GetInt16(key interface{}) int16 {
	if pair.valType != VtInt16 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseFloat(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt16(src[pos : pos+int64(VtInt16Len)])
}
func (pair *opFloatPair) GetInt32(key interface{}) int32 {
	if pair.valType != VtInt32 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseFloat(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt32(src[pos : pos+int64(VtInt32Len)])
}
func (pair *opFloatPair) GetInt64(key interface{}) int64 {
	if pair.valType != VtInt64 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseFloat(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt64(src[pos : pos+int64(VtInt64Len)])
}
func (pair *opFloatPair) GetFloat(key interface{}) float32 {
	if pair.valType != VtFloat {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseFloat(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetFloat32(src[pos : pos+int64(VtFloatLen)])
}
func (pair *opFloatPair) GetDouble(key interface{}) float64 {
	if pair.valType != VtDouble {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseFloat(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetFloat64(src[pos : pos+int64(VtDoubleLen)])
}
func (pair *opFloatPair) GetString(key interface{}) string {
	if pair.valType != VtString {
		return ""
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return ""
	}
	keyData := parseFloat(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return ""
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return utils.GetString(src[pos : pos+dataLen])
}
func (pair *opFloatPair) GetSlice(key interface{}) *OPSlice {
	if pair.valType != VtSlice {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseFloat(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeSlice(src[pos : pos+dataLen])
}
func (pair *opFloatPair) GetMap(key interface{}) *OPMap {
	if pair.valType != VtMap {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseFloat(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeMap(src[pos : pos+dataLen])
}

func (pair *opFloatPair) GetStructure(key interface{}) *OPStructure {
	if pair.valType != VtStructure {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseFloat(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeStructure(src[pos : pos+dataLen])
}

func (pair *opFloatPair) AllKeys() []interface{} {
	if 0 == len(pair.keyPos) {
		return nil
	}
	res := make([]interface{}, len(pair.keyPos))
	i := 0
	for key := range pair.keyPos {
		res[i] = key
		i++
	}
	return res
}

//////////////////////////////////////////////////////////////////////////////////////////

// string-key
type opStringPair struct {
	opPairBase
	keyPos map[string]int64 // first:real data key, second: pos+len
}

func newOPStringPair(src []byte, keyType, valType int8, dataStart int) *opStringPair {
	total := len(src)
	if 0 == total {
		fmt.Println("src is empty")
		//logs.Error("src is empty")
		return nil
	}
	if keyType != VtString {
		fmt.Println("unsupported string key type:%v", keyType)
		//logs.Error("unsupported string key type:%v", keyType)
		return nil
	}

	// get value len
	valFixedLen, isValFixed := fixedTypeLenMap[valType]

	offset := dataStart

	positions := make(map[string]int64, 64*utils.KB)
	key := ""
	for offset != total {

		// get key value
		keyLenCode := uint8(src[offset])
		offset++
		keyLen := 0
		switch keyLenCode {
		case mpBin8:
			keyLen = int(src[offset])
			offset += 1
		case mpBin16:
			keyLen = int(src[offset])<<8 | int(src[offset+1])
			offset += 2
		case mpBin32:
			keyLen = int(src[offset])<<24 | int(src[offset+1])<<16 | int(src[offset+2])<<8 | int(src[offset+3])
			offset += 4
		}
		key = string(src[offset : offset+keyLen])
		offset += keyLen // skip to value

		// pos and len, pos:high 32bit, len:low 32bit
		// 记录位置
		posLen := int64(offset)

		if isValFixed {
			// value是固定长度的类型,offset直接跳过固定长度到下一个pair,posLen记录的只是位置，不记录长度，并且位置记录在低32位
			offset += int(valFixedLen)
		} else {
			// value是变长类型,需要根据valType计算数据的长度,posLen记录的是位置+长度

			// 将位置信息移到高32位
			posLen = posLen << 32
			headLen := 0

			lenCode := uint8(src[offset])
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
			posLen = posLen | int64(headLen+dataLen)
			// skip to next pair
			offset += dataLen
		}
		positions[key] = posLen
	}

	if 0 == len(positions) {
		fmt.Println("data positions is empty")
		//logs.Error("data positions is empty")
		return nil
	}
	pair := &opStringPair{
		keyPos: positions,
	}
	pair.src = src
	pair.valType = valType

	return pair
}

func (pair *opStringPair) GetBool(key interface{}) bool {
	if pair.valType != VtBool {
		return false
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return false
	}

	keyData := parseString(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return false
	}
	val := uint8(src[pos])
	return val > 0
}
func (pair *opStringPair) GetInt8(key interface{}) int8 {
	if pair.valType != VtInt8 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}

	keyData := parseString(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return int8(src[pos])
}
func (pair *opStringPair) GetInt16(key interface{}) int16 {
	if pair.valType != VtInt16 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseString(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt16(src[pos : pos+int64(VtInt16Len)])

}
func (pair *opStringPair) GetInt32(key interface{}) int32 {
	if pair.valType != VtInt32 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseString(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt32(src[pos : pos+int64(VtInt32Len)])
}
func (pair *opStringPair) GetInt64(key interface{}) int64 {
	if pair.valType != VtInt64 {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseString(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetInt64(src[pos : pos+int64(VtInt64Len)])
}
func (pair *opStringPair) GetFloat(key interface{}) float32 {
	if pair.valType != VtFloat {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseString(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetFloat32(src[pos : pos+int64(VtFloatLen)])
}
func (pair *opStringPair) GetDouble(key interface{}) float64 {
	if pair.valType != VtDouble {
		return 0
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return 0
	}
	keyData := parseString(key)
	pos, ok := keyPos[keyData]
	if !ok {
		return 0
	}
	return utils.GetFloat64(src[pos : pos+int64(VtDoubleLen)])
}
func (pair *opStringPair) GetString(key interface{}) string {
	if pair.valType != VtString {
		return ""
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return ""
	}
	keyData := parseString(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return ""
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return utils.GetString(src[pos : pos+dataLen])
}
func (pair *opStringPair) GetSlice(key interface{}) *OPSlice {
	if pair.valType != VtSlice {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseString(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeSlice(src[pos : pos+dataLen])
}
func (pair *opStringPair) GetMap(key interface{}) *OPMap {
	if pair.valType != VtMap {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseString(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeMap(src[pos : pos+dataLen])
}
func (pair *opStringPair) GetStructure(key interface{}) *OPStructure {
	if pair.valType != VtStructure {
		return nil
	}
	src := pair.src
	keyPos := pair.keyPos
	if 0 == len(src) || 0 == len(keyPos) {
		return nil
	}
	keyData := parseString(key)
	posLen, ok := keyPos[keyData]
	if !ok {
		return nil
	}
	pos := (posLen & 0x7FFFFFFF00000000) >> 32
	dataLen := posLen & 0xFFFFFFFF

	return DecodeStructure(src[pos : pos+dataLen])
}

func (pair *opStringPair) AllKeys() []interface{} {
	if 0 == len(pair.keyPos) {
		return nil
	}
	res := make([]interface{}, len(pair.keyPos))
	i := 0
	for key := range pair.keyPos {
		res[i] = key
		i++
	}
	return res
}

func parseInt(param interface{}) int64 {
	if nil == param {
		return 0
	}
	res := int64(0)
	switch x := param.(type) {
	case int8:
		res = int64(x)
	case int16:
		res = int64(x)
	case int32:
		res = int64(x)
	case int64:
		res = int64(x)
	case int:
		res = int64(x)
	default:
	}
	return res
}

func parseFloat(param interface{}) float64 {
	if nil == param {
		return 0
	}
	res := float64(0)
	switch x := param.(type) {
	case float32:
		res = float64(x)
	case float64:
		res = float64(x)
	default:
	}
	return res
}

func parseString(param interface{}) string {
	if nil == param {
		return ""
	}
	res := ""
	switch x := param.(type) {
	case string:
		res = string(x)
	default:
	}
	return res
}
