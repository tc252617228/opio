package opio

import (
	"errors"
	"fmt"
	"time"

	"opio/internal/utils"
)

type OPDataSet struct {
	io        *utils.Buffer
	table     *Table
	colsCount uint32

	isFirst bool

	rowSize   uint32
	rowCursor uint32

	rowBuf    []byte
	rowVarBuf []byte

	closed bool
}

func (set *OPDataSet) Close() {
	if !set.closed {
		//快速读取IO缓存区，
		err := set.io.SkipAll()
		if err != nil {
			//logs.Error("err >>>>>>>", err)
			// fmt.Println("err >>>>>>>", err) // 注释掉打印
		}
		set.table = nil
		set.rowBuf = nil
		set.rowVarBuf = nil
	}
	set.closed = true
}

// ReadContent -
func (set *OPDataSet) Next() (bool, error) {
	//首次加载
	for {
		if set.isFirst || (set.rowSize > 0 && set.rowCursor == set.rowSize) {
			//读取一次数据长度
			size, err := set.io.DecodeArrayStart()
			if err != nil {
				return false, err
			} else if size == 0xffffffff { //eof
				return false, err
			} else if size == 0 {
				continue
			}
			set.rowSize += size
			set.rowCursor += 0
			set.rowBuf = nil
			set.isFirst = false
		}
		dataBuf, err := set.io.DecodeBytes()
		if err != nil {
			return false, err
		}
		set.rowBuf = dataBuf
		set.rowVarBuf = dataBuf[set.table.fixedLength+set.table.bitLength:]
		set.rowCursor++
		return true, nil
	}

}

func (set *OPDataSet) GetTableName() string {
	return set.table.Name()
}

func (set *OPDataSet) GetSize() uint32 {
	return set.rowSize
}

func (set *OPDataSet) GetColumns() []Column {
	return set.table.columns
}

func (set *OPDataSet) GetRow() []byte {
	return set.rowBuf
}

func (set *OPDataSet) GetValue(col uint32) (value interface{}, err error) {
	if col >= set.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	switch c.GetType() {
	case VtBool:
		value, err = set.GetBool(col)
	case VtInt8:
		value, err = set.GetInt8(col)
	case VtInt16:
		value, err = set.GetInt16(col)
	case VtInt32:
		value, err = set.GetInt32(col)
	case VtInt64:
		value, err = set.GetInt64(col)
	case VtFloat:
		value, err = set.GetFloat32(col)
	case VtDouble:
		value, err = set.GetFloat64(col)
	case VtDateTime:
		value, err = set.GetDateTime(col)
	case VtString:
		value, err = set.GetString(col)
	case VtBinary:
		value, err = set.GetBytes(col)
	case VtObject:
		value, err = set.GetObject(col)
	case VtSlice:
		raw, err := set.GetBytes(col)
		if nil == err {
			// Handle error from DecodeSlice, ignore for getter simplicity
			o, _ := DecodeSlice(raw)
			if o != nil {
				return o, nil
			} else {
				return nil, errors.New("decode slice failed")
			}
		} else {
			return nil, err
		}
	case VtMap:
		raw, err := set.GetBytes(col)
		if nil == err {
			// Handle error from DecodeMap, ignore for getter simplicity
			o, _ := DecodeMap(raw)
			if o != nil {
				return o, nil
			} else {
				return nil, errors.New("decode map failed")
			}
		} else {
			return nil, err
		}
	case VtStructure:
		raw, err := set.GetBytes(col)
		if nil == err {
			// Handle error from DecodeStructure, ignore for getter simplicity
			o, _ := DecodeStructure(raw)
			if o != nil {
				return o, nil
			} else {
				return nil, errors.New("decode map failed")
			}
		} else {
			return nil, err
		}

	}
	return value, err
}

// for dll by PB
func (set *OPDataSet) GetValueExt(col uint32) (value interface{}, isObj bool, err error) {
	if col >= set.colsCount {
		return nil, false, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	switch c.GetType() {
	case VtBool:
		value, err = set.GetBool(col)
	case VtInt8:
		value, err = set.GetInt8(col)
	case VtInt16:
		value, err = set.GetInt16(col)
	case VtInt32:
		value, err = set.GetInt32(col)
	case VtInt64:
		value, err = set.GetInt64(col)
	case VtFloat:
		value, err = set.GetFloat32(col)
	case VtDouble:
		value, err = set.GetFloat64(col)
	case VtDateTime:
		value, err = set.GetDateTime(col)
	case VtString:
		value, err = set.GetString(col)
	case VtBinary:
		value, err = set.GetBytes(col)
	case VtObject:
		value, isObj, err = set.GetObjectExt(col)
	case VtSlice:
		raw, err := set.GetBytes(col)
		if nil == err {
			// Handle error from DecodeSlice, ignore for getter simplicity
			o, _ := DecodeSlice(raw)
			if o != nil {
				return o, false, nil
			} else {
				return nil, false, errors.New("decode slice failed")
			}
		} else {
			return nil, false, err
		}
	case VtMap:
		raw, err := set.GetBytes(col)
		if nil == err {
			// Handle error from DecodeMap, ignore for getter simplicity
			o, _ := DecodeMap(raw)
			if o != nil {
				return o, false, nil
			} else {
				return nil, false, errors.New("decode map failed")
			}
		} else {
			return nil, false, err
		}
	case VtStructure:
		raw, err := set.GetBytes(col)
		if nil == err {
			// Handle error from DecodeStructure, ignore for getter simplicity
			o, _ := DecodeStructure(raw)
			if o != nil {
				return o, false, nil
			} else {
				return nil, false, errors.New("decode map failed")
			}
		} else {
			return nil, false, err
		}
	}
	return value, isObj, err
}

func (set *OPDataSet) GetObjectExt(col uint32) (interface{}, bool, error) {
	if col >= set.colsCount {
		return nil, false, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ == VtObject {
		endIndex := 0
		vLen := 0
		//计算所需获取数据的偏差
		for i := uint32(0); i <= c.cell; i++ {
			vType := set.rowVarBuf[endIndex]
			endIndex += 1
			if vType == mpBin8 {
				vLen = int(utils.GetUInt8(set.rowVarBuf[endIndex : endIndex+1]))
				endIndex += 1
			} else if vType == mpBin16 {
				vLen = int(utils.GetUInt16(set.rowVarBuf[endIndex : endIndex+2]))
				endIndex += 2
			} else if vType == mpBin32 {
				vLen = int(utils.GetUInt32(set.rowVarBuf[endIndex : endIndex+4]))
				endIndex += 4
			}
			if vLen > 0 {
				endIndex += vLen
			}
		}
		if vLen > 0 {
			_v := set.rowVarBuf[endIndex-vLen : endIndex]
			var resV interface{}
			var isObj bool
			switch _v[0] {
			case VtNull:
				resV = nil
			case VtBool:
				resV = utils.GetBool(_v[1:])
			case VtInt8:
				resV = utils.GetInt8(_v[1:])
			case VtInt16:
				resV = utils.GetInt16(_v[1:])
			case VtInt32:
				resV = utils.GetInt32(_v[1:])
			case VtInt64:
				resV = utils.GetInt64(_v[1:])
			case VtFloat:
				resV = utils.GetFloat32(_v[1:])
			case VtDouble:
				resV = utils.GetFloat64(_v[1:])
			case VtDateTime:
				resV = utils.GetDateTime(_v[1:])
			case VtString:
				resV = string(_v[1:])
			case VtBinary:
				resV = _v[1:]
			case VtObject:
				isObj = true
				resV = _v[1:]
			}
			return resV, isObj, nil
		} else {
			return nil, false, nil
		}
	} else {
		return nil, false, fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
	}
}

func (set *OPDataSet) GetObject(col uint32) (interface{}, error) {
	if col >= set.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ == VtObject {
		endIndex := 0
		vLen := 0
		//计算所需获取数据的偏差
		for i := uint32(0); i <= c.cell; i++ {
			vType := set.rowVarBuf[endIndex]
			endIndex += 1
			if vType == mpBin8 {
				vLen = int(utils.GetUInt8(set.rowVarBuf[endIndex : endIndex+1]))
				endIndex += 1
			} else if vType == mpBin16 {
				vLen = int(utils.GetUInt16(set.rowVarBuf[endIndex : endIndex+2]))
				endIndex += 2
			} else if vType == mpBin32 {
				vLen = int(utils.GetUInt32(set.rowVarBuf[endIndex : endIndex+4]))
				endIndex += 4
			}
			if vLen > 0 {
				endIndex += vLen
			}
		}
		if vLen > 0 {
			_v := set.rowVarBuf[endIndex-vLen : endIndex]
			switch _v[0] {
			case VtNull:
				return nil, nil
			case VtBool:
				return utils.GetBool(_v[1:]), nil
			case VtInt8:
				return utils.GetInt8(_v[1:]), nil
			case VtInt16:
				return utils.GetInt16(_v[1:]), nil
			case VtInt32:
				return utils.GetInt32(_v[1:]), nil
			case VtInt64:
				return utils.GetInt64(_v[1:]), nil
			case VtFloat:
				return utils.GetFloat32(_v[1:]), nil
			case VtDouble:
				return utils.GetFloat64(_v[1:]), nil
			case VtDateTime:
				return utils.GetDateTime(_v[1:]), nil
			case VtString:
				return string(_v[1:]), nil
			case VtBinary:
				return _v[1:], nil
			case VtObject:
				return _v[1:], nil
			}
		} else {
			return nil, nil
		}
	} else {
		return nil, fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
	}
	return nil, nil
}

func (set *OPDataSet) GetBool(col uint32) (bool, error) {
	if col >= set.colsCount {
		return false, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtBool {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return false, err
		case VtBool:
			v, err := set.GetBool(col)
			return v, err
		case VtInt8:
			v, err := set.GetInt8(col)
			return v != 0, err
		case VtInt16:
			v, err := set.GetInt16(col)
			return v != 0, err
		case VtInt32:
			v, err := set.GetInt32(col)
			return v != 0, err
		case VtInt64:
			v, err := set.GetInt64(col)
			return v != 0, err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return v != 0, err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return v != 0, err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			if err == nil {
				return v.Unix() > 0, err
			} else {
				return false, err
			}
		case VtString:
			v, err := set.GetString(col)
			return len(v) > 0, err
		case VtBinary:
			v, err := set.GetBytes(col)
			return len(v) > 0, err
		case VtObject:
			v, err := set.GetObject(col)
			return v != nil, err
		case VtSlice:
			v, err := set.GetBytes(col)
			if nil == err {
				// Handle error from DecodeSlice, ignore for getter simplicity
				o, _ := DecodeSlice(v)
				return o != nil && !o.IsEmpty(), err
			} else {
				return false, err
			}
		case VtMap:
			v, err := set.GetBytes(col)
			if nil == err {
				// Handle error from DecodeMap, ignore for getter simplicity
				o, _ := DecodeMap(v)
				return o != nil && !o.IsEmpty(), err
			} else {
				return false, err
			}
		case VtStructure:
			v, err := set.GetBytes(col)
			if nil == err {
				// Handle error from DecodeStructure, ignore for getter simplicity
				o, _ := DecodeStructure(v)
				return o != nil && !o.IsEmpty(), err
			} else {
				return false, err
			}
		default:
			return false, err
		}
	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetBool(data), nil
}

func (set *OPDataSet) GetInt8(col uint32) (int8, error) {
	if col >= set.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtInt8 {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := set.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := set.GetInt8(col)
			return v, err
		case VtInt16:
			v, err := set.GetInt16(col)
			return int8(v), err
		case VtInt32:
			v, err := set.GetInt32(col)
			return int8(v), err
		case VtInt64:
			v, err := set.GetInt64(col)
			return int8(v), err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return int8(v), err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return int8(v), err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			if err == nil {
				return int8(v.Unix()), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := set.GetObject(col)
			if err != nil {
				return 0, err
			}
			switch nv := v.(type) {
			case bool:
				if nv {
					return 1, err
				} else {
					return 0, err
				}
			case int8:
				return nv, err
			case int16:
				return int8(nv), err
			case int32:
				return int8(nv), err
			case int64:
				return int8(nv), err
			case float32:
				return int8(nv), err
			case float64:
				return int8(nv), err
			case time.Time:
				return int8(nv.Unix()), err
			default:
				return 0, err
			}
		default:
			return 0, err
		}
	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt8(data), nil
}

func (set *OPDataSet) GetInt16(col uint32) (int16, error) {
	if col >= set.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtInt16 {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := set.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := set.GetInt8(col)
			return int16(v), err
		case VtInt16:
			v, err := set.GetInt16(col)
			return v, err
		case VtInt32:
			v, err := set.GetInt32(col)
			return int16(v), err
		case VtInt64:
			v, err := set.GetInt64(col)
			return int16(v), err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return int16(v), err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return int16(v), err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			if err == nil {
				return int16(v.Unix()), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := set.GetObject(col)
			if err != nil {
				return 0, err
			}
			switch nv := v.(type) {
			case bool:
				if nv {
					return 1, err
				} else {
					return 0, err
				}
			case int8:
				return int16(nv), err
			case int16:
				return nv, err
			case int32:
				return int16(nv), err
			case int64:
				return int16(nv), err
			case float32:
				return int16(nv), err
			case float64:
				return int16(nv), err
			case time.Time:
				return int16(nv.Unix()), err
			default:
				return 0, err
			}
		default:
			return 0, err
		}

	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt16(data), nil
}

func (set *OPDataSet) GetInt32(col uint32) (int32, error) {
	if col >= set.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtInt32 {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := set.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := set.GetInt8(col)
			return int32(v), err
		case VtInt16:
			v, err := set.GetInt16(col)
			return int32(v), err
		case VtInt32:
			v, err := set.GetInt32(col)
			return v, err
		case VtInt64:
			v, err := set.GetInt64(col)
			return int32(v), err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return int32(v), err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return int32(v), err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			if err == nil {
				return int32(v.Unix()), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := set.GetObject(col)
			if err != nil {
				return 0, err
			}
			switch nv := v.(type) {
			case bool:
				if nv {
					return 1, err
				} else {
					return 0, err
				}
			case int8:
				return int32(nv), err
			case int16:
				return int32(nv), err
			case int32:
				return nv, err
			case int64:
				return int32(nv), err
			case float32:
				return int32(nv), err
			case float64:
				return int32(nv), err
			case time.Time:
				return int32(nv.Unix()), err
			default:
				return 0, err
			}
		default:
			return 0, err
		}
	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt32(data), nil
}

func (set *OPDataSet) GetInt64(col uint32) (int64, error) {
	if col >= set.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtInt64 {

		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := set.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := set.GetInt8(col)
			return int64(v), err
		case VtInt16:
			v, err := set.GetInt16(col)
			return int64(v), err
		case VtInt32:
			v, err := set.GetInt32(col)
			return int64(v), err
		case VtInt64:
			v, err := set.GetInt64(col)
			return v, err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return int64(v), err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return int64(v), err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			if err == nil {
				return v.Unix(), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := set.GetObject(col)
			if err != nil {
				return 0, err
			}
			switch nv := v.(type) {
			case bool:
				if nv {
					return 1, err
				} else {
					return 0, err
				}
			case int8:
				return int64(nv), err
			case int16:
				return int64(nv), err
			case int32:
				return int64(nv), err
			case int64:
				return nv, err
			case float32:
				return int64(nv), err
			case float64:
				return int64(nv), err
			case time.Time:
				return nv.Unix(), err
			default:
				return 0, err
			}
		default:
			return 0, err
		}
	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt64(data), nil
}

func (set *OPDataSet) GetFloat32(col uint32) (float32, error) {
	if col >= set.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtFloat {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := set.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := set.GetInt8(col)
			return float32(v), err
		case VtInt16:
			v, err := set.GetInt16(col)
			return float32(v), err
		case VtInt32:
			v, err := set.GetInt32(col)
			return float32(v), err
		case VtInt64:
			v, err := set.GetInt64(col)
			return float32(v), err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return v, err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return float32(v), err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			if err == nil {
				return float32(utils.DateTimeToDouble(v)), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := set.GetObject(col)
			if err != nil {
				return 0, err
			}
			switch nv := v.(type) {
			case bool:
				if nv {
					return 1, err
				} else {
					return 0, err
				}
			case int8:
				return float32(nv), err
			case int16:
				return float32(nv), err
			case int32:
				return float32(nv), err
			case int64:
				return float32(nv), err
			case float32:
				return nv, err
			case float64:
				return float32(nv), err
			case time.Time:
				return float32(utils.DateTimeToDouble(nv)), err
			default:
				return 0, err
			}
		default:
			return 0, err
		}
	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetFloat32(data), nil
}

func (set *OPDataSet) GetFloat64(col uint32) (float64, error) {
	if col >= set.colsCount {
		return 0, errors.New("table subscript out of range")
	}

	c := set.table.columns[col]
	if c.typ != VtDouble {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := set.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := set.GetInt8(col)
			return float64(v), err
		case VtInt16:
			v, err := set.GetInt16(col)
			return float64(v), err
		case VtInt32:
			v, err := set.GetInt32(col)
			return float64(v), err
		case VtInt64:
			v, err := set.GetInt64(col)
			return float64(v), err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return float64(v), err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return v, err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			if err == nil {
				return float64(v.Unix()) + float64(v.Nanosecond())/1e9, err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := set.GetObject(col)
			if err != nil {
				return 0, err
			}
			switch nv := v.(type) {
			case bool:
				if nv {
					return 1, err
				} else {
					return 0, err
				}
			case int8:
				return float64(nv), err
			case int16:
				return float64(nv), err
			case int32:
				return float64(nv), err
			case int64:
				return float64(nv), err
			case float32:
				return float64(nv), err
			case float64:
				return nv, err
			case time.Time:
				return utils.DateTimeToDouble(nv), err
			default:
				return 0, err
			}
		default:
			return 0, err
		}
	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetFloat64(data), nil
}

func (set *OPDataSet) GetDateTime(col uint32) (time.Time, error) {
	if col >= set.colsCount {
		return time.Unix(0, 0), errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtDateTime {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return time.Unix(0, 0), err
		case VtBool:
			return time.Unix(0, 0), err
		case VtInt8:
			v, err := set.GetInt8(col)
			return time.Unix(int64(v), 0), err
		case VtInt16:
			v, err := set.GetInt16(col)
			return time.Unix(int64(v), 0), err
		case VtInt32:
			v, err := set.GetInt32(col)
			return time.Unix(int64(v), 0), err
		case VtInt64:
			v, err := set.GetInt64(col)
			return time.Unix(v, 0), err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return time.Unix(int64(v), 0), err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return utils.Float2DateTime(v), err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			return v, err
		default:
			return time.Unix(0, 0), err
		}
	}
	data := set.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetDateTime(data), nil
}

func (set *OPDataSet) GetString(col uint32) (string, error) {
	if col >= set.colsCount {
		return "", errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtString {

		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return "", err
		case VtBool:
			v, err := set.GetBool(col)
			if v {
				return "true", err
			} else {
				return "false", err
			}
		case VtInt8:
			v, err := set.GetInt8(col)
			return fmt.Sprintf("%v", v), err
		case VtInt16:
			v, err := set.GetInt16(col)
			return fmt.Sprintf("%v", v), err
		case VtInt32:
			v, err := set.GetInt32(col)
			return fmt.Sprintf("%v", v), err
		case VtInt64:
			v, err := set.GetInt64(col)
			return fmt.Sprintf("%v", v), err
		case VtFloat:
			v, err := set.GetFloat32(col)
			return fmt.Sprintf("%v", v), err
		case VtDouble:
			v, err := set.GetFloat64(col)
			return fmt.Sprintf("%v", v), err
		case VtDateTime:
			v, err := set.GetDateTime(col)
			return fmt.Sprintf("%v", v), err
		case VtString:
			v, err := set.GetString(col)
			return v, err
		case VtBinary:
			v, err := set.GetBytes(col)
			return fmt.Sprintf("%v", v), err
		case VtObject:
			v, err := set.GetObject(col)
			return fmt.Sprintf("%v", v), err
		case VtSlice:
			v, err := set.GetBytes(col)
			if nil == err {
				// Handle error from DecodeSlice, ignore for getter simplicity
				o, _ := DecodeSlice(v)
				if o != nil {
					return o.String(false), nil
				} else {
					return "", errors.New("decode slice failed")
				}
			} else {
				return "", err
			}
		case VtMap:
			v, err := set.GetBytes(col)
			if nil == err {
				// Handle error from DecodeMap, ignore for getter simplicity
				o, _ := DecodeMap(v)
				if o != nil {
					return o.String(false), nil
				} else {
					return "", errors.New("decode map failed")
				}
			} else {
				return "", err
			}
		case VtStructure:
			v, err := set.GetBytes(col)
			if nil == err {
				// Handle error from DecodeStructure, ignore for getter simplicity
				o, _ := DecodeStructure(v)
				if o != nil {
					return o.String(false), nil
				} else {
					return "", errors.New("decode structure failed")
				}
			} else {
				return "", err
			}
		default:
			return "", err
		}
	}

	if c.length == 0 {
		offset := 0
		valLen := 0
		lenCode := uint8(0)
		//计算所需获取数据的偏差
		for i := uint32(0); i <= c.cell; i++ {
			lenCode = set.rowVarBuf[offset]
			offset++

			switch lenCode {
			case mpBin8:
				valLen = int(utils.GetUInt8(set.rowVarBuf[offset : offset+1]))
				offset += 1

			case mpBin16:
				valLen = int(utils.GetUInt16(set.rowVarBuf[offset : offset+2]))
				offset += 2

			case mpBin32:
				valLen = int(utils.GetUInt32(set.rowVarBuf[offset : offset+4]))
				offset += 4
			}

			offset += valLen

		}
		return string(set.rowVarBuf[offset-valLen : offset]), nil
	} else {
		return string(set.rowBuf[c.offset:(c.offset + uint32(c.length))]), nil
	}
}

func (set *OPDataSet) GetCompoundBytes(col uint32) ([]byte, error) {
	if col >= set.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	colType := c.typ
	if colType != VtSlice && colType != VtMap && colType != VtStructure {
		return nil, fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, colType)
	}
	offset := 0
	valLen := 0
	lenCode := uint8(0)
	headLen := 0

	//计算所需获取数据的偏差
	for i := uint32(0); i <= c.cell; i++ {
		headLen = 0

		lenCode = set.rowVarBuf[offset]
		offset++
		headLen++

		switch lenCode {
		case mpBin8:
			valLen = int(utils.GetUInt8(set.rowVarBuf[offset : offset+1]))
			offset++
			headLen++

		case mpBin16:
			valLen = int(utils.GetUInt16(set.rowVarBuf[offset : offset+2]))
			offset += 2
			headLen += 2

		case mpBin32:
			valLen = int(utils.GetUInt32(set.rowVarBuf[offset : offset+4]))
			offset += 4
			headLen += 4
		}
		offset += valLen
	}
	return set.rowVarBuf[offset-valLen-headLen : offset], nil
}

func (set *OPDataSet) GetBytes(col uint32) ([]byte, error) {
	if col >= set.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := set.table.columns[col]
	if c.typ != VtBinary && c.typ != VtSlice && c.typ != VtMap && c.typ != VtStructure {
		//计算所需获取数据的偏差
		return nil, fmt.Errorf("get bytes column data type error, col %v index: %v type: %v",
			c.name, col, c.typ)
	}

	if c.length == 0 {
		offset := 0
		valLen := 0
		for i := uint32(0); i <= c.cell; i++ {
			lenCode := set.rowVarBuf[offset]
			offset++

			switch lenCode {
			case mpBin8:
				valLen = int(utils.GetUInt8(set.rowVarBuf[offset : offset+1]))
				offset++

			case mpBin16:
				valLen = int(utils.GetUInt16(set.rowVarBuf[offset : offset+2]))
				offset += 2

			case mpBin32:
				valLen = int(utils.GetUInt32(set.rowVarBuf[offset : offset+4]))
				offset += 4
			}
			if valLen > 0 {
				offset += valLen
			}
		}
		return set.rowVarBuf[offset-valLen : offset], nil
	} else {
		return set.rowBuf[c.offset:(c.offset + uint32(c.length))], nil
	}

}

func (set *OPDataSet) GetTable() (*Table, error) {
	return set.table, nil
}

func (set *OPDataSet) SetRow(row []byte) {
	if nil == set.rowBuf {
		set.rowBuf = row
		set.rowVarBuf = set.rowBuf[set.table.fixedLength+set.table.bitLength:]
	}
}

func (set *OPDataSet) ClearRow() {
	set.rowBuf = nil
	set.rowVarBuf = nil
}

func (set *OPDataSet) SetTable(table *Table) {
	if nil == set.table {
		set.table = table
		set.colsCount = uint32(len(table.GetColumns()))
	}
}
