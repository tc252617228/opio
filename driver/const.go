package opio

import (
	"reflect"
)

func init() {
	initWholeReflectTypeMap()

	initFixedType()

	initFixedTypeLenMap()

	initVariableType()

	initVarOPTypes()

	initOPMapKeyTypes()

	initOPMapKeyTypeMap()
}

const (
	max_buffer_size = 65536
	propCapacity    = 256

	MAXINT8  = 127
	MAXINT16 = 32767
	MAXINT32 = 2147483647
	MAXINT64 = 9223372036854775807
)

// Value type
const (
	ArrayMask = 16

	VtNull      = 0
	VtBool      = 1
	VtInt8      = 2
	VtInt16     = 3
	VtInt32     = 4
	VtInt64     = 5
	VtFloat     = 6
	VtFloat32   = 6
	VtDouble    = 7
	VtFloat64   = 7
	VtDateTime  = 8
	VtString    = 9
	VtBinary    = 10
	VtObject    = 11
	VtMap       = 12
	VtStructure = 13
	VtSlice     = 14 //VtSlice ==VtArray
	VtArray     = 14 //VtSlice ==VtArray

	BOOL_ARRAY     = 17
	INT8_ARRAY     = 18
	INT16_ARRAY    = 19
	INT32_ARRAY    = 20
	INT64_ARRAY    = 21
	FLOAT_ARRAY    = 22
	DOUBLE_ARRAY   = 23
	DATETIME_ARRAY = 24
	STRING_ARRAY   = 25
	BINARY_ARRAY   = 26
	OBJECT_ARRAY   = 27
	STRUCT         = 28
	VtRow          = 32

	VtRowArray = 48
)

// FIX TYPE LENGTH
const (
	VtBoolLen     = 1
	VtInt8Len     = 1
	VtInt16Len    = 2
	VtInt32Len    = 4
	VtInt64Len    = 8
	VtFloatLen    = 4
	VtFloat32Len  = 4
	VtDoubleLen   = 8
	VtFloat64Len  = 8
	VtDateTimeLen = 8
)

// Opers
const (
	OperEQ      = 0
	OperNE      = 1
	OperGT      = 2
	OperLT      = 3
	OperGE      = 4
	OperLE      = 5
	OperIn      = 6
	OperNotIn   = 7
	OperLike    = 8
	OperNotLike = 9
	OperReqexp  = 10
)

const (
	RelationAnd = 0
	RelationOr  = 1
)

const (
	// type
	TypeAX  int8 = 0
	TypeDX  int8 = 1
	TypeI2  int8 = 2
	TypeI4  int8 = 3
	TypeR8  int8 = 4
	TypeI8  int8 = 5
	TypeTX  int8 = 6
	TypeBN  int8 = 7
	TypeAny int8 = 15

	// packing
	MAGIC      int32 = 0x10203040
	cmdSelect  int32 = 110
	cmdUpdate  int32 = 120
	cmdInsert  int32 = 130
	cmdDelete  int32 = 140
	cmdReplace int32 = 150

	// url
	urlScheme       int32 = 0x20000000
	urlID           int32 = 0x21000000
	urlStatic       int32 = 0x22000000
	urlDynamic      int32 = 0x23000000
	urlChildID      int32 = 0x24000000
	urlChildStatic  int32 = 0x25000000
	urlChildDynamic int32 = 0x26000000
	urlAlarm        int32 = 0x2A000000
	urlChildAlarm   int32 = 0x2B000000
	urlArchive      int32 = 0x30000000
	urlCloudNodes   int32 = 0x40000000
	urlCloudNode    int32 = 0x41000000
	urlCloudDBs     int32 = 0x42000000
	urlCloudDB      int32 = 0x43000000
	urlCloudTime    int32 = 0x44000000
	urlEcho         int32 = 0x46000000

	// flag
	flagByName   int16 = 1      // 按对象名称请求, 否则按ID请求
	flagByID     int16 = 2      // 有ID索引，4.0协议
	flagFilter   int16 = 4      // 写历史启用过滤
	flagNoDS     int16 = 0x40   // 写数据不带状态
	flagNoTM     int16 = 0x80   // 写数据不带时间，带全局时间
	flagWall     int16 = 0x100  // 通过隔离器写实时/历史,返回1比特
	flagMMI      int16 = 0x200  // 操作指令：确认/切除报警
	flagSync     int16 = 0x400  // 同步复制操作
	flagCtrl     int16 = 0x800  // 控制指令：写数据到IO驱动，设定值。
	flagFeedback int16 = 0x1000 // 采集端：领取指令，执行控制，反馈结果
	flagCache    int16 = 0x2000 // 写历史缓存
)

// Prop Keys
const (
	PropReqId   = "Reqid"   //请求ID
	PropService = "Service" //服务名: openPlant
	PropTable   = "Table"   //表名：{Node, Point, Realtime, Archive, Stat, Alarm, AAlarm}
	PropAction  = "Action"  //指令: {Select, Insert, Update, Replace, Delete, ExecSQL}
	PropSubject = "Subject" //主题：SubjectPrefix.sub-Subject.sub-Subject...
	PropOption  = "Option"  //选项: {wall,cache,mmi,control,feedback,sync}, 可多选
	PropOrderBy = "OrderBy" //排序: {asc, desc}
	PropLimit   = "Limit"   //分页: [offset,] limit
	PropAsync   = "Async"   //异步请求: 0, 1
	PropColumns = "Columns" //表字段
	PropKey     = "Key"     //索引名称
	PropIndexes = "Indexes" //索引列表
	PropFilters = "Filters" //过滤条件
	PropError   = "Error"   //错误信息
	PropErrNo   = "Errno"   //错误码
	PropSQL     = "SQL"     //SQL

	PropToken     = "Token" //Token
	PropDB        = "db"    //database name
	PropTimestamp = "Time"  //时间戳
	PropSnapshot  = "Snapshot"
	PropSubscribe = "Subscribe" //动态订阅: 0, 1
)

const (
	LPropTaskId  = "TaskId"
	LPropCluster = "Cluster"
	LPropNasPath = "NasPath"
)

// Tables
const (
	TableNode     = "Node"
	TablePoint    = "Point"
	TableRealtime = "Realtime"
	TableArchive  = "Archive"
	TableStat     = "Stat"
	TableAlarm    = "Alarm"
	TableAAlarm   = "AAlarm"
)

// Actions
const (
	ActionCreate  = "Create"
	ActionSelect  = "Select"
	ActionInsert  = "Insert"
	ActionUpdate  = "Update"
	ActionReplace = "Replace"
	ActionDelete  = "Delete"
	ActionExecSQL = "ExecSQL"
	ActionCommit  = "Commit"
)

// Subjects
// Subject是 以 Subject 或 Subject.sub-Subject.sub-Subject... 形式发送的
const (
	SubjectNAS      = "NAS"
	SubjectCluster  = "Cluster"
	SubjectMetadata = "Metadata"
	SubjectTable    = "Table"
	SubjectToken    = "Token"
	SubjectKV       = "KV"
)

const (
	SubList             = "SubList"
	SubColumn           = "Column"
	SubColumnVersion    = "ColumnVersion"
	SubLogicIndexes     = "LogicIndexes"
	SubDateColumn       = "DataColumn"
	SubClassifyColumn   = "ClassifyColumn"
	SubTableOptions     = "TableOptions"
	SubTableConstraints = "TableConstraints"
	SubWrite            = "Write"
	SubRead             = "Read"
	SubClear            = "Clear"
)

const (
	TableColumns    = "TableColumns"
	LogicIndexUnits = "LogicIndexUnits"
	MatchDateColumn = "MatchDateColumn"
	ClassifyColumn  = "ClassifyColumn"
	TableOptions    = "TableOptions"
	Constraints     = "Constraints"
	Tables          = "Tables"
	ColumnVersions  = "ColumnVersions"
	Head            = "Head"
	DBCluster       = "Clusters"
	NASInfo         = "NasInfo"
)

const (
	ZIP_MODEL_Uncompressed = 0
	ZIP_MODEL_Frame        = 1
	ZIP_MODEL_Block        = 2
)

var (
	_uppercase = []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
		32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 47, 47,
		48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
		64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
		80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95,
		96, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
		80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 123, 124, 125, 126, 127,
		128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
		144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
		160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
		176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191,
		192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207,
		208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
		224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
		240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255,
	}
)

// get status const header
var (
	statusHeader = []byte{
		0x10, 0x20, 0x30, 0x40,
		0, 0, 0, 110,
		0x46, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0xA5, //will come back
		0x10, 0x20, 0x30, 0x40,
	}
)

// msgpack
const (
	mpBin8  = 0xc4
	mpBin16 = 0xc5
	mpBin32 = 0xc6
)

var (
	wholeReflectTypeMap map[reflect.Kind]int8
	fixedTypeMap        map[reflect.Kind]int8
	fixedTypeLenMap     map[int8]int8
	varOPTypes          []int8
	varTypeMap          map[reflect.Kind]int8
	opMapKeyTypes       []int8
	opMapKeyTypeMap     map[reflect.Kind]int8
)

func initWholeReflectTypeMap() {
	wholeReflectTypeMap = map[reflect.Kind]int8{
		reflect.Bool:    VtBool,
		reflect.Int8:    VtInt8,
		reflect.Uint8:   VtInt8,
		reflect.Int16:   VtInt16,
		reflect.Uint16:  VtInt16,
		reflect.Int32:   VtInt32,
		reflect.Uint32:  VtInt32,
		reflect.Int:     VtInt64,
		reflect.Uint:    VtInt64,
		reflect.Int64:   VtInt64,
		reflect.Uint64:  VtInt64,
		reflect.Float32: VtFloat,
		reflect.Float64: VtDouble,
		reflect.String:  VtString,
		reflect.Array:   VtSlice,
		reflect.Slice:   VtSlice,
		reflect.Map:     VtMap,
		reflect.Struct:  VtStructure,
	}
}

func initFixedType() {
	fixedTypeMap = map[reflect.Kind]int8{
		reflect.Bool:    VtBool,
		reflect.Int8:    VtInt8,
		reflect.Uint8:   VtInt8,
		reflect.Int16:   VtInt16,
		reflect.Uint16:  VtInt16,
		reflect.Int32:   VtInt32,
		reflect.Uint32:  VtInt32,
		reflect.Int:     VtInt64,
		reflect.Int64:   VtInt64,
		reflect.Uint:    VtInt64,
		reflect.Uint64:  VtInt64,
		reflect.Float32: VtFloat,
		reflect.Float64: VtDouble,
	}
}

func initFixedTypeLenMap() {
	fixedTypeLenMap = map[int8]int8{
		VtBool:     VtBoolLen,
		VtInt8:     VtInt8Len,
		VtInt16:    VtInt16Len,
		VtInt32:    VtInt32Len,
		VtInt64:    VtInt64Len,
		VtFloat:    VtFloatLen,
		VtDouble:   VtDoubleLen,
		VtDateTime: VtDateTimeLen,
	}
}

func initVariableType() {
	varTypeMap = map[reflect.Kind]int8{
		reflect.String: VtString,
		reflect.Array:  VtSlice,
		reflect.Slice:  VtSlice,
		reflect.Map:    VtMap,
		reflect.Struct: VtStructure,
	}
}

func initVarOPTypes() {
	varOPTypes = []int8{VtString, VtSlice, VtMap, VtStructure, VtObject}
}

func isVarOPType(dataType int8) bool {
	for _, typ := range varOPTypes {
		if dataType == typ {
			return true
		}
	}
	return false
}

func initOPMapKeyTypes() {
	opMapKeyTypes = []int8{VtInt8, VtInt16, VtInt32, VtInt64, VtFloat, VtDouble, VtString}
}

func isOPMapKeyType(dataType int8) bool {
	for _, typ := range opMapKeyTypes {
		if dataType == typ {
			return true
		}
	}
	return false
}

func initOPMapKeyTypeMap() {
	opMapKeyTypeMap = map[reflect.Kind]int8{
		reflect.Bool:    VtBool,
		reflect.Int8:    VtInt8,
		reflect.Uint8:   VtInt8,
		reflect.Int16:   VtInt16,
		reflect.Uint16:  VtInt16,
		reflect.Int32:   VtInt32,
		reflect.Uint32:  VtInt32,
		reflect.Int:     VtInt64,
		reflect.Int64:   VtInt64,
		reflect.Uint:    VtInt64,
		reflect.Uint64:  VtInt64,
		reflect.Float32: VtFloat,
		reflect.Float64: VtDouble,
		reflect.String:  VtString,
	}
}
