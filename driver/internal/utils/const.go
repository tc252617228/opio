package utils

const (
	KB = 1024
	MB = KB * 1024
	GB = MB * 1024
	TB = GB * 1024
)

// msgpack
const (
	mpPosFixNumMin = 0x00
	mpPosFixNumMax = 0x7f
	mpFixMapMin    = 0x80
	mpFixMapMax    = 0x8f
	mpFixArrayMin  = 0x90
	mpFixArrayMax  = 0x9f
	mpFixStrMin    = 0xa0
	mpFixStrMax    = 0xbf
	mpNil          = 0xc0
	mpNotUsed      = 0xc1
	mpFalse        = 0xc2
	mpTrue         = 0xc3
	mpFloat        = 0xca
	mpDouble       = 0xcb
	mpUint8        = 0xcc
	mpUint16       = 0xcd
	mpUint32       = 0xce
	mpUint64       = 0xcf
	mpInt8         = 0xd0
	mpInt16        = 0xd1
	mpInt32        = 0xd2
	mpInt64        = 0xd3
	// extensions below
	mpBin8         = 0xc4
	mpBin16        = 0xc5
	mpBin32        = 0xc6
	mpExt8         = 0xc7
	mpExt16        = 0xc8
	mpExt32        = 0xc9
	mpFixExt1      = 0xd4
	mpFixExt2      = 0xd5
	mpFixExt4      = 0xd6
	mpFixExt8      = 0xd7
	mpFixExt16     = 0xd8
	mpStr8         = 0xd9 // new
	mpStr16        = 0xda
	mpStr32        = 0xdb
	mpArray16      = 0xdc
	mpArray32      = 0xdd
	mpMap16        = 0xde
	mpMap32        = 0xdf
	mpNegFixNumMin = 0xe0
	mpNegFixNumMax = 0xff
)

const mask = 0xff

const (
	type_null_code  = 0
	type_bool_code  = 1
	type_int8_code  = 2
	type_int16_code = 3
	type_int32_code = 4
	type_int64_code = 5

	type_float_code  = 6
	type_double_code = 7

	type_datetime_code = 8
	type_string_code   = 9
	type_binary_code   = 10
)

const (
	ctString = 0
	ctBinary = 1
	ctArray  = 2
	ctMap    = 3
)

type mpContainerType struct {
	fixCutoff uint32
	bFixMin   uint8
	b8        uint8
	b16       uint8
	b32       uint8
}

var mpContainerTypes = [4]mpContainerType{
	{32, mpFixStrMin, mpStr8, mpStr16, mpStr32},
	{0, 0, mpBin8, mpBin16, mpBin32},
	{16, mpFixArrayMin, 0, mpArray16, mpArray32},
	{16, mpFixMapMin, 0, mpMap16, mpMap32}}

const maxSize int = 65536
const headSize int = 4
const (
	ZIP_MODEL_Uncompressed = 0
	ZIP_MODEL_Frame        = 1
	ZIP_MODEL_Block        = 2
)

// MagicNb(4 byte) + FLG(1 byte) + BD(1 byte) + HC(1 byte)
var lz4_head_code = []byte{4, 34, 77, 24, 100, 112, 185}
