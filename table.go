package opio

import (
	"errors"
	"fmt"
	"math"
	"time"

	"opio/internal/utils"
)

// OPRow type
type OPRow struct {
	Data []byte
}

func (row *OPRow) GetRowData() []byte {
	return row.Data
}

const typeError = "row index :%v ,col index %v:Type Error!!,now type :%v"
const indexError = "col index:%v Error"

// Table type
// fixme Table不负责线程安全,客户端自己控制 !!!!
type Table struct {
	name     string //结构名称
	id       int32  //结构ID
	hash     int32  //结构的HashCode
	colCount uint32 //列数量

	fixedLength uint32 //定长字节
	bitLength   uint32 //bit 设置长度
	variableLen uint32 //变长个数

	Columns
	rows     []OPRow //行数据缓存
	rowCount uint    //行数
	cur      *OPRow  //行游标

	bufSize        int
	defaultBufSize int

	fixedBuf    []byte
	variableBuf [][]byte
	bitBuf      []byte

	defaultFixedBuf []byte
	defaultBitBuf   []byte

	errors []error
}

func (t *Table) GetColumns() []Column {
	return t.columns
}

func (t *Table) GetRow() []OPRow {
	return t.rows
}

// TypeID - Table type id
func (t *Table) TypeID() int32 {
	return t.id
}

// Name - Table name
func (t *Table) Name() string {
	return t.name
}

// Msgpack编码头部长度：(type + length)
func mpBinaryExtra(l int) uint16 {
	switch {
	case l < 256:
		return 2
	case l < 65536:
		return 3
	default:
		return 5
	}
}

func mpStringExtra(l int) uint16 {
	switch {
	case l < 32:
		return 1
	case l < 256:
		return 2
	case l < 65536:
		return 3
	default:
		return 5
	}
}

var uppercase = [256]byte{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
	16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
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
	240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}

// MakeHash for key
func MakeHash(key string) int32 {
	hash := 0
	b := []byte(key)
	l := len(b)
	i := 0
	for i < l && i < 4 {
		hash = (hash << 8) | int(uppercase[b[i]])
		i++
	}
	return int32(hash)
}

// AddColumns -
func (t *Table) AddColumns(cols []Column) {
	for _, col := range cols {
		t.AddColumnExtension(col.name, int(col.typ), int(col.length), col.ext)
	}
}

// AddColumn -
func (t *Table) AddColumn(name string, typ int, length int) bool {
	defer t.initBuf()
	return t.AddColumnEx(name, typ, length, 0, "", "", nil)
}

// AddColumn -
func (t *Table) AddColumnExtension(name string, typ int, length int, ext []byte) bool {
	defer t.initBuf()
	return t.AddColumnEx(name, typ, length, 0, "", "", ext)
}

// AddColumnEx -
func (t *Table) AddColumnEx(name string, typ int, length int, mask int, defval string, values string, ext []byte) bool {
	if mask != 0 {
		mask = 1
	}

	col := Column{name, uint8(typ), uint8(length), uint8(mask), defval, values, 0, 0, 0, 0, 0, 0, make([]byte, 0)}

	fixedString := uint32(0)

	switch col.typ {
	case VtBool:
		col.length = 1
	case VtInt8:
		col.length = 1
	case VtInt16:
		col.length = 2
	case VtInt32:
		col.length = 4
	case VtInt64:
		col.length = 8
	case VtFloat:
		col.length = 4
	case VtDouble, VtDateTime:
		col.length = 8
	case VtString:
		if length > 0 {
			fixedString = 1
		}
		col.length = uint8(length)
	case VtBinary:
		//fallthrough
		col.length = uint8(length)
	case VtSlice:
		fallthrough
	case VtMap:
		fallthrough
	case VtStructure:
		fallthrough
	case VtObject:
		col.length = 0
	case VtNull:
		col.length = 0
	default:
		col.length = 0
	}

	col.ext = append(col.ext, ext...)

	//若字段是变成字段，变长信息增加
	if col.length == 0 {
		col.cell = t.variableLen
		t.variableLen++
	}

	col.offset = t.fixedLength
	col.end = col.offset + uint32(col.length)
	col.index = int32(t.colCount)

	t.fixedLength = col.end + fixedString
	t.columns = append(t.columns, col)
	t.colCount++

	//固定长度字段的附加设置标记
	t.bitLength = (t.colCount + 7) >> 3

	// hash
	t.hash += int32((int(col.typ) << 24) + ((int(col.length) + (int(col.mask) << 7)) << 16) + int(col.hash)*int(t.colCount))
	return true
}

// NewTable make a new Table
func NewEmptyTable() *Table {
	return &Table{}
}

func NewTable(name string, capacity uint) *Table {
	t := &Table{}
	t.name = name
	t.id = MakeHash(name)
	t.rows = make([]OPRow, 0, capacity)
	t.errors = make([]error, 0)
	return t
}

// fixme 清空 table 表中的rows
func (t *Table) Clear() {
	if len(t.rows) > 0 {
		//reset rows
		t.rowCount = 0
		t.rows = t.rows[0:0]
		//reset buf
		copy(t.fixedBuf, t.defaultFixedBuf)
		copy(t.bitBuf, t.defaultBitBuf)
		t.bufSize = t.defaultBufSize
	}
}

// GetName - Table name
func (t *Table) GetName() string {
	return t.name
}

func (t *Table) setColumnBit(col uint32) {
	// col>>3 --- col/8
	// col&7 --- col%8
	//优化的位操作动作,快速设置其数据位
	t.bitBuf[uint32(col>>3)] |= 1 << byte(col&7)
}

// SetColumnBool -
func (t *Table) SetColumnBool(col uint32, value bool) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtBool {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	if value {
		utils.PutInt8(t.fixedBuf[c.offset:c.end], 1)
	}
	t.setColumnBit(col)
	return nil
}

// SetColumnInt8 -
// noinspection GoUnusedParameter
func (t *Table) SetColumnInt8(col uint32, value int8, mask int) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtInt8 {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	utils.PutInt8(t.fixedBuf[c.offset:c.end], value)
	t.setColumnBit(col)
	return nil

}

// SetColumnInt16 -
// noinspection GoUnusedParameter
func (t *Table) SetColumnInt16(col uint32, value int16, mask int) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtInt16 {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	utils.PutInt16(t.fixedBuf[c.offset:c.end], value)
	t.setColumnBit(col)
	return nil
}

// SetColumnInt32 -
// noinspection GoUnusedParameter
func (t *Table) SetColumnInt32(col uint32, value int32, mask int) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtInt32 {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	utils.PutInt32(t.fixedBuf[c.offset:c.end], value)
	t.setColumnBit(col)
	return nil
}

// SetColumnInt64 -
// noinspection GoUnusedParameter
func (t *Table) SetColumnInt64(col uint32, value int64, mask int64) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtInt64 {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	utils.PutInt64(t.fixedBuf[c.offset:c.end], value)
	t.setColumnBit(col)
	return nil

}

// SetColumnFloat -
func (t *Table) SetColumnFloat(col uint32, value float32) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtFloat {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	utils.PutFloat32(t.fixedBuf[c.offset:c.end], value)
	t.setColumnBit(col)
	return nil

}

// SetColumnDouble -
func (t *Table) SetColumnDouble(col uint32, value float64) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtDouble {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	utils.PutFloat64(t.fixedBuf[c.offset:c.end], value)
	t.setColumnBit(col)
	return nil
}

func (t *Table) SetColumnDateTime(col uint32, value time.Time) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtDateTime {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	msTime := float64(value.UnixNano()/1e6) / 1e3
	utils.PutFloat64(t.fixedBuf[c.offset:c.end], msTime)
	t.setColumnBit(col)
	return nil
}

// SetColumnString -
func (t *Table) SetColumnString(col uint32, value string) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtString {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}
	dataLen := len(value)

	headLen, bytes := utils.PutString(value)

	t.bufSize -= len(t.variableBuf[c.cell])

	t.variableBuf[c.cell] = bytes

	t.bufSize += dataLen + headLen

	t.setColumnBit(col)

	return nil
}

// SetColumnSlice -
func (t *Table) SetColumnSlice(col uint32, value interface{}) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtSlice {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}

	// 处理 EncodeSlice 返回的错误
	_, raw, err := EncodeSlice(value)
	if err != nil {
		// 将编码错误添加到表的错误列表中，并返回
		t.errors = append(t.errors, fmt.Errorf("SetColumnSlice: encoding failed for col %d: %w", col, err))
		return err
	}
	rawLen := len(raw)
	// 检查 EncodeSlice 是否返回了空二进制（这通常表示输入为 nil 或空）
	if IsEmptyBinary(raw) { // 假设 IsEmptyBinary 检查特定的空二进制表示
		// 如果输入有效但编码结果为空，这可能是一个错误，或者只是表示空 slice
		// 这里假设返回空二进制是有效的，并将其设置为空二进制
		raw = MakeEmptyBinary() // 确保使用标准的空二进制表示
		rawLen = len(raw)       // 通常是 2
	} else if 0 == rawLen {
		// 如果 rawLen 为 0 但不是标准的空二进制，则视为错误
		err = errors.New("put slice failed: EncodeSlice returned empty non-standard binary")
		t.errors = append(t.errors, err)
		return err
	}

	index, temp := utils.PutBinary(raw)

	t.bufSize -= len(t.variableBuf[c.cell])

	t.variableBuf[c.cell] = temp

	t.bufSize += rawLen + index

	t.setColumnBit(col)

	return nil
}

// SetColumnMap -
func (t *Table) SetColumnMap(col uint32, value interface{}) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtMap {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}

	// 处理 EncodeMap 返回的错误
	_, raw, err := EncodeMap(value)
	if err != nil {
		t.errors = append(t.errors, fmt.Errorf("SetColumnMap: encoding failed for col %d: %w", col, err))
		return err
	}
	rawLen := len(raw)
	if IsEmptyBinary(raw) {
		raw = MakeEmptyBinary()
		rawLen = len(raw)
	} else if 0 == rawLen {
		err = errors.New("put map failed: EncodeMap returned empty non-standard binary")
		t.errors = append(t.errors, err)
		return err
	}

	index, temp := utils.PutBinary(raw)

	t.bufSize -= len(t.variableBuf[c.cell])

	t.variableBuf[c.cell] = temp

	t.bufSize += rawLen + index

	t.setColumnBit(col)

	return nil
}

// SetColumnStructure -
func (t *Table) SetColumnStructure(col uint32, value interface{}) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtStructure {
		err := fmt.Errorf(typeError, t.rowCount, col, c.typ)
		t.errors = append(t.errors, err)
		return err
	}

	// 处理 EncodeStructure 返回的错误
	_, raw, err := EncodeStructure(value)
	if err != nil {
		t.errors = append(t.errors, fmt.Errorf("SetColumnStructure: encoding failed for col %d: %w", col, err))
		return err
	}
	rawLen := len(raw)
	if IsEmptyBinary(raw) {
		raw = MakeEmptyBinary()
		rawLen = len(raw)
	} else if 0 == rawLen {
		err = errors.New("put structure failed: EncodeStructure returned empty non-standard binary")
		t.errors = append(t.errors, err)
		return err
	}

	index, temp := utils.PutBinary(raw)

	t.bufSize -= len(t.variableBuf[c.cell])

	t.variableBuf[c.cell] = temp

	t.bufSize += rawLen + index

	t.setColumnBit(col)

	return nil
}

// SetColumnObject -
func (t *Table) SetColumnObject(col uint32, v interface{}) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ == VtObject {
		t.bufSize -= len(t.variableBuf[c.cell])
		switch value := v.(type) {
		case nil:
			t.variableBuf[c.cell] = []byte{mpBin8, 0}
			t.bufSize += 2
		case bool:
			if value {
				t.variableBuf[c.cell] = []byte{mpBin8, 2, VtBool, 1}
			} else {
				t.variableBuf[c.cell] = []byte{mpBin8, 2, VtBool, 0}
			}
			t.bufSize += 4
		case int8:
			t.variableBuf[c.cell] = []byte{mpBin8, 2, VtInt8, byte(value)}
			t.bufSize += 4
		case uint8:
			t.variableBuf[c.cell] = []byte{mpBin8, 2, VtInt8, byte(value)}
			t.bufSize += 4
		case int16:
			t.variableBuf[c.cell] = []byte{mpBin8, 3, VtInt16, byte(value >> 8), byte(value)}
			t.bufSize += 5
		case uint16:
			t.variableBuf[c.cell] = []byte{mpBin8, 3, VtInt16, byte(value >> 8), byte(value)}
			t.bufSize += 5

		case int32:
			t.variableBuf[c.cell] = []byte{mpBin8, 5, VtInt32, byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
			t.bufSize += 7
		case uint32:
			t.variableBuf[c.cell] = []byte{mpBin8, 5, VtInt32, byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
			t.bufSize += 7

		case int64:
			t.variableBuf[c.cell] = []byte{mpBin8, 9, VtInt64, byte(value >> 56), byte(value >> 48), byte(value >> 40), byte(value >> 32), byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
			t.bufSize += 11
		case uint64:
			t.variableBuf[c.cell] = []byte{mpBin8, 9, VtInt64, byte(value >> 56), byte(value >> 48), byte(value >> 40), byte(value >> 32), byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
			t.bufSize += 11

		case int:
			_value := int64(value)
			t.variableBuf[c.cell] = []byte{mpBin8, 9, VtInt64, byte(_value >> 56), byte(_value >> 48), byte(_value >> 40), byte(_value >> 32), byte(_value >> 24), byte(_value >> 16), byte(_value >> 8), byte(_value)}
			t.bufSize += 11
		case uint:
			_value := int64(value)
			t.variableBuf[c.cell] = []byte{mpBin8, 9, VtInt64, byte(_value >> 56), byte(_value >> 48), byte(_value >> 40), byte(_value >> 32), byte(_value >> 24), byte(_value >> 16), byte(_value >> 8), byte(_value)}
			t.bufSize += 11

		case float32:
			_value := math.Float32bits(value)
			t.variableBuf[c.cell] = []byte{mpBin8, 5, VtFloat, byte(_value >> 24), byte(_value >> 16), byte(_value >> 8), byte(_value)}
			t.bufSize += 7

		case float64:
			_value := math.Float64bits(value)
			t.variableBuf[c.cell] = []byte{mpBin8, 9, VtDouble, byte(_value >> 56), byte(_value >> 48), byte(_value >> 40), byte(_value >> 32), byte(_value >> 24), byte(_value >> 16), byte(_value >> 8), byte(_value)}
			t.bufSize += 11
		case time.Time:
			_value := math.Float64bits(float64(value.UnixNano()/1e6) / 1e3)
			t.variableBuf[c.cell] = []byte{mpBin8, 9, VtDouble, byte(_value >> 56), byte(_value >> 48), byte(_value >> 40), byte(_value >> 32), byte(_value >> 24), byte(_value >> 16), byte(_value >> 8), byte(_value)}
			t.bufSize += 11
		case string:
			//fixme !!!长度包含了类型系统  VtString VtBinary 初始化长度+1  字节结构-1
			v_len := len(value) + 1
			index := 3
			var temp []byte
			if v_len <= 0xff {
				temp = make([]byte, v_len+index)
				temp[0] = mpBin8
				temp[1] = byte(v_len)
				temp[2] = VtString
			} else if v_len <= 0xffff {
				index = 4
				temp = make([]byte, v_len+index)
				temp[0] = mpBin16
				temp[1] = byte(v_len >> 8)
				temp[2] = byte(v_len)
				temp[3] = VtString
			} else if v_len <= 0x7fffffff {
				index = 6
				temp = make([]byte, v_len+index)
				temp[0] = mpBin32
				temp[1] = byte(v_len >> 24)
				temp[2] = byte(v_len >> 16)
				temp[3] = byte(v_len >> 8)
				temp[4] = byte(v_len)
				temp[5] = VtString
			}
			copy(temp[index:], value)

			t.variableBuf[c.cell] = temp

			//fixme !!!长度包含了类型系统  VtString VtBinary 初始化长度+1  字节结构-1
			t.bufSize += v_len + index - 1

		case []byte:

			//fixme !!!长度包含了类型系统  VtString VtBinary 初始化长度+1  字节结构-1
			v_len := len(value) + 1
			index := 3
			var temp []byte
			if v_len <= 0xff {
				temp = make([]byte, v_len+index)
				temp[0] = mpBin8
				temp[1] = byte(v_len)
				temp[2] = VtBinary
			} else if v_len <= 0xffff {
				index = 4
				temp = make([]byte, v_len+index)
				temp[0] = mpBin16
				temp[1] = byte(v_len >> 8)
				temp[2] = byte(v_len)
				temp[3] = VtBinary
			} else if v_len <= 0x7fffffff {
				index = 6
				temp = make([]byte, v_len+index)
				temp[0] = mpBin32
				temp[1] = byte(v_len >> 24)
				temp[2] = byte(v_len >> 16)
				temp[3] = byte(v_len >> 8)
				temp[4] = byte(v_len)
				temp[5] = VtBinary
			}
			copy(temp[index:], value)
			t.variableBuf[c.cell] = temp

			//fixme !!!长度包含了类型系统  VtString VtBinary 初始化长度+1  字节结构-1
			t.bufSize += v_len + index - 1
		default:
			// 移除调试打印，直接返回错误
			return errors.New(fmt.Sprintf("Data Type error,Type %T", value))
		}
		t.setColumnBit(col)
	} else {
		e := errors.New(fmt.Sprintf(typeError, t.rowCount, col, c.typ))
		t.errors = append(t.errors, e)
		return e
	}
	return nil
}

// SetColumnBinary -
func (t *Table) SetColumnBinary(col uint32, value []byte) error {
	if t.colCount <= col {
		err := fmt.Errorf(indexError, col)
		t.errors = append(t.errors, err)
		return err
	}
	c := t.columns[col]
	if c.typ != VtBinary && c.typ != VtSlice && c.typ != VtMap && c.typ != VtStructure {
		e := errors.New(fmt.Sprintf(typeError, t.rowCount, col, c.typ))
		t.errors = append(t.errors, e)
		return e
	}

	t.bufSize -= len(t.variableBuf[c.cell])
	vLen := len(value)

	index, temp := utils.PutBinary(value)
	t.variableBuf[c.cell] = temp

	t.bufSize += vLen + index

	t.setColumnBit(col)

	return nil
}

func (t *Table) AppendRaw(value []byte) {
	t.rows = append(t.rows, OPRow{value})
	t.rowCount++
}

func (t *Table) GetErrors() (errors []error) {
	// 拷贝一份异常内容提供外部使用！
	// 切忌 只能拷贝，！！！不能提供对外操作指针！！！
	temp := make([]error, len(t.errors))
	copy(temp, t.errors)
	return temp
}

// SetColumnEmpty -
func (t *Table) SetColumnEmpty(col uint32) error {
	if t.colCount <= col {
		e := errors.New(fmt.Sprintf(indexError, col))
		t.errors = append(t.errors, e)
		return e
	}
	c := t.columns[col]
	if VtObject == c.typ ||
		VtBinary == c.typ ||
		VtString == c.typ ||
		VtSlice == c.typ ||
		VtMap == c.typ ||
		VtStructure == c.typ {
		t.bufSize -= len(t.variableBuf[c.cell])
		t.variableBuf[c.cell] = MakeEmptyBinary()
		t.bufSize += 2
		t.setColumnBit(col)
	} else {
		e := errors.New(fmt.Sprintf(typeError, t.rowCount, col, c.typ))
		t.errors = append(t.errors, e)
		return e
	}
	return nil
}

// SetColumnValue -
func (t *Table) SetColumnValue(col uint32, value interface{}) (err error) {
	switch v := value.(type) {
	case nil:
		return t.SetColumnEmpty(col)
	case bool:
		return t.SetColumnBool(col, v)
	case int8:
		return t.SetColumnInt8(col, v, 0)
	case uint8:
		return t.SetColumnInt8(col, int8(v), 0)
	case int16:
		return t.SetColumnInt16(col, v, 0)
	case uint16:
		return t.SetColumnInt16(col, int16(v), 0)
	case int32:
		return t.SetColumnInt32(col, v, 0)
	case uint32:
		return t.SetColumnInt32(col, int32(v), 0)
	case int64:
		return t.SetColumnInt64(col, v, 0)
	case uint64:
		return t.SetColumnInt64(col, int64(v), 0)
	case float32:
		return t.SetColumnFloat(col, v)
	case float64:
		return t.SetColumnDouble(col, v)
	case string:
		return t.SetColumnString(col, v)
	case []byte:
		return t.SetColumnBinary(col, v)
	default:
		return fmt.Errorf("unknown value type, type:%T, val:%v", v, v)
	}
}

func (t *Table) initBuf() {
	//设置默认缓冲参数
	t.defaultBufSize = int(t.fixedLength + t.bitLength)
	t.bufSize = t.defaultBufSize

	//构建变长缓冲
	t.variableBuf = make([][]byte, t.variableLen)

	//构建缓冲区
	t.fixedBuf = make([]byte, t.fixedLength)
	t.bitBuf = make([]byte, t.bitLength)

	//构建缓冲区默认值
	t.defaultFixedBuf = make([]byte, t.fixedLength)
	t.defaultBitBuf = make([]byte, t.bitLength)

}

// 每次完成数据操作后，添加缓存 -
func (t *Table) BindRow() {
	if len(t.errors) == 0 {
		_r := OPRow{Data: make([]byte, t.bufSize)}
		//拷贝定长数据
		copy(_r.Data, t.fixedBuf)

		//拷贝位参数设定
		copy(_r.Data[t.fixedLength:], t.bitBuf)

		index := t.fixedLength + t.bitLength
		//拷贝变长数据
		for i := uint32(0); i < t.variableLen; i++ {
			copy(_r.Data[index:], t.variableBuf[i])
			index += uint32(len(t.variableBuf[i]))
			t.variableBuf[i] = nil
		}
		//reset buf
		copy(t.fixedBuf, t.defaultFixedBuf)
		copy(t.bitBuf, t.defaultBitBuf)
		t.bufSize = t.defaultBufSize

		// append RowData
		t.rows = append(t.rows, _r)
		t.rowCount++
	} else {
		t.rowCount++
	}

}

// RowCount -
func (t *Table) RowCount() uint {
	return t.rowCount
}

func (t *Table) SetRows(rows *[]OPRow) {
	t.rowCount = uint(len(*rows))
	t.rows = append(t.rows, *rows...)
}
