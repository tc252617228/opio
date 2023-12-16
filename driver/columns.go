package opio

import (
	"fmt"

	"github.com/tc252617228/opio/driver/internal/utils"
)

const (
	cName   = "Name"
	cType   = "Type"
	cLength = "Length"
	cExt    = "Ext"
)

// Column type
type Column struct {
	name   string
	typ    uint8
	length uint8
	mask   uint8
	defVal string
	values string // enum values
	index  int32  // column index
	cell   uint32 // cell index
	offset uint32

	end uint32

	hash int32
	fkey int32 // foreign key for fast index

	ext []byte // Extension
}

func (col *Column) GetName() string {
	return col.name
}

func (col *Column) GetType() uint8 {
	return col.typ
}

func (col *Column) GetLength() uint8 {
	return col.length
}

func (col *Column) GetMask() uint8 {
	return col.mask
}

func (col *Column) GetIndex() int32 {
	return col.index
}

func (col *Column) SetCell(c uint32) {
	col.cell = c
}

func (col *Column) GetCell() uint32 {
	return col.cell
}

func (col *Column) GetOffset() uint32 {
	return col.offset
}

func (col *Column) GetExt() []byte {
	return col.ext
}

//func (col *Column) SetExt(b []byte) {
//	if col.ext != nil {
//		col.ext = append(col.ext, b...)
//	} else {
//		col.ext = make([]byte, 0, len(b))
//		col.ext = append(col.ext, b...)
//	}
//}

// // read -
func (col *Column) read(io *utils.Buffer) (err error) {
	size, err := io.DecodeMapStart()
	if err != nil {
		return err
	}
	for i := uint32(0); i < size; i++ {
		key, err := io.DecodeString()
		switch key {
		case cName:
			col.name, err = io.DecodeString()
		case cType:
			col.typ, err = io.DecodeUint8()
		case cLength:
			col.length, err = io.DecodeUint8()
		case cExt:
			col.ext, err = io.DecodeBytes()
		default:
			_, _ = io.DecodeValue()
		}
		if err != nil {
			//logs.Error("err", err)
			fmt.Println("err", err)
			break

		}
	}
	return err
}

// write -
func (col *Column) write(io *utils.Buffer) error {
	var n = uint32(3)
	var isHaveExt bool

	if col.ext != nil && len(col.ext) > 0 {
		isHaveExt = true
		n = 4
	}
	err := io.EncodeMapStart(n)
	if err == nil {
		_ = io.EncodeString(cName)
		_ = io.EncodeString(col.name)
		_ = io.EncodeString(cType)
		_ = io.EncodeUint8(col.typ)
		_ = io.EncodeString(cLength)
		err = io.EncodeUint8(col.length)
		if isHaveExt {
			_ = io.EncodeString(cExt)
			err = io.EncodeBytes(col.ext)
		}
	}
	return err
}

//////////////////////////////////////////////////////////////////////////////////////////

type Columns struct {
	columns []Column
}

// read -
func (this *Columns) read(io *utils.Buffer) (err error) {
	size, err := io.DecodeArrayStart()
	if err != nil {
		return err
	}

	for i := uint32(0); i < size; i++ {
		col := Column{}
		col.ext = make([]byte, 0)
		err := col.read(io)
		if err != nil {
			return err
		} else {
			this.columns = append(this.columns, col)
		}
	}
	return nil
}

// write -
func (this *Columns) write(io *utils.Buffer) error {
	colCount := uint32(len(this.columns))
	err := io.EncodeArrayStart(colCount)
	for i := 0; i < int(colCount); i++ {
		err = this.columns[i].write(io)
		if err != nil {
			break
		}
	}
	return nil
}
