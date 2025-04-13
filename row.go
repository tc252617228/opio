package opio

import (
	"errors"
	"fmt"
	"time"

	"opio/internal/utils"
)

type UnPackOPRow struct {
	table     *Table
	colsCount uint32
	rowBuf    []byte
	rowVarBuf []byte
}

func NewUnPackOPRow(t *Table) (r *UnPackOPRow) {
	return &UnPackOPRow{
		table:     t,
		colsCount: uint32(len(t.GetColumns())),
	}
}

func (r *UnPackOPRow) GetValue(col uint32) (value interface{}, err error) {
	if col >= r.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	switch c.GetType() {
	case VtBool:
		value, err = r.GetBool(col)
	case VtInt8:
		value, err = r.GetInt8(col)
	case VtInt16:
		value, err = r.GetInt16(col)
	case VtInt32:
		value, err = r.GetInt32(col)
	case VtInt64:
		value, err = r.GetInt64(col)
	case VtFloat:
		value, err = r.GetFloat32(col)
	case VtDouble:
		value, err = r.GetFloat64(col)
	case VtDateTime:
		value, err = r.GetDateTime(col)
	case VtString:
		value, err = r.GetString(col)
	case VtBinary:
		value, err = r.GetBytes(col)
	case VtObject:
		value, err = r.GetObject(col)
	case VtSlice:
		raw, err := r.GetBytes(col)
		if err != nil {
			return nil, err
		}
		// Handle error from DecodeSlice, ignore for getter simplicity
		value, _ = DecodeSlice(raw)
	case VtMap:
		raw, err := r.GetBytes(col)
		if err != nil {
			return nil, err
		}
		// Handle error from DecodeMap, ignore for getter simplicity
		value, _ = DecodeMap(raw)
	case VtStructure:
		raw, err := r.GetBytes(col)
		if err != nil {
			return nil, err
		}
		// Handle error from DecodeStructure, ignore for getter simplicity
		value, _ = DecodeStructure(raw)
	}
	return value, err
}

// for dll by PB
func (r *UnPackOPRow) GetValueExt(col uint32) (value interface{}, isObj bool, err error) {
	if col >= r.colsCount {
		return nil, false, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	switch c.GetType() {
	case VtBool:
		value, err = r.GetBool(col)
	case VtInt8:
		value, err = r.GetInt8(col)
	case VtInt16:
		value, err = r.GetInt16(col)
	case VtInt32:
		value, err = r.GetInt32(col)
	case VtInt64:
		value, err = r.GetInt64(col)
	case VtFloat:
		value, err = r.GetFloat32(col)
	case VtDouble:
		value, err = r.GetFloat64(col)
	case VtDateTime:
		value, err = r.GetDateTime(col)
	case VtString:
		value, err = r.GetString(col)
	case VtBinary:
		value, err = r.GetBytes(col)
	case VtObject:
		value, isObj, err = r.GetObjectExt(col)
	case VtSlice:
		raw, err := r.GetBytes(col)
		if err != nil {
			return nil, false, err
		}
		// Handle error from DecodeSlice, ignore for getter simplicity
		value, _ = DecodeSlice(raw)
	case VtMap:
		raw, err := r.GetBytes(col)
		if err != nil {
			return nil, false, err
		}
		// Handle error from DecodeMap, ignore for getter simplicity
		value, _ = DecodeMap(raw)
	case VtStructure:
		raw, err := r.GetBytes(col)
		if err != nil {
			return nil, false, err
		}
		// Handle error from DecodeStructure, ignore for getter simplicity
		value, _ = DecodeStructure(raw)
	}
	return value, isObj, err
}

func (r *UnPackOPRow) GetObjectExt(col uint32) (interface{}, bool, error) {
	if col >= r.colsCount {
		return nil, false, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ == VtObject {
		endIndex := 0
		vLen := 0
		//计算所需获取数据的偏差
		for i := uint32(0); i <= c.cell; i++ {
			vType := r.rowVarBuf[endIndex]
			endIndex += 1
			if vType == mpBin8 {
				vLen = int(utils.GetUInt8(r.rowVarBuf[endIndex : endIndex+1]))
				endIndex += 1
			} else if vType == mpBin16 {
				vLen = int(utils.GetUInt16(r.rowVarBuf[endIndex : endIndex+2]))
				endIndex += 2
			} else if vType == mpBin32 {
				vLen = int(utils.GetUInt32(r.rowVarBuf[endIndex : endIndex+4]))
				endIndex += 4
			}
			if vLen > 0 {
				endIndex += vLen
			}
		}
		if vLen > 0 {
			_v := r.rowVarBuf[endIndex-vLen : endIndex]
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

func (r *UnPackOPRow) GetObject(col uint32) (interface{}, error) {
	if col >= r.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ == VtObject {
		endIndex := 0
		vLen := 0
		//计算所需获取数据的偏差
		for i := uint32(0); i <= c.cell; i++ {
			vType := r.rowVarBuf[endIndex]
			endIndex += 1
			if vType == mpBin8 {
				vLen = int(utils.GetUInt8(r.rowVarBuf[endIndex : endIndex+1]))
				endIndex += 1
			} else if vType == mpBin16 {
				vLen = int(utils.GetUInt16(r.rowVarBuf[endIndex : endIndex+2]))
				endIndex += 2
			} else if vType == mpBin32 {
				vLen = int(utils.GetUInt32(r.rowVarBuf[endIndex : endIndex+4]))
				endIndex += 4
			}
			if vLen > 0 {
				endIndex += vLen
			}
		}
		if vLen > 0 {
			_v := r.rowVarBuf[endIndex-vLen : endIndex]
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

func (r *UnPackOPRow) GetBool(col uint32) (bool, error) {
	if col >= r.colsCount {
		return false, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtBool {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return false, err
		case VtBool:
			v, err := r.GetBool(col)
			return v, err
		case VtInt8:
			v, err := r.GetInt8(col)
			return v != 0, err
		case VtInt16:
			v, err := r.GetInt16(col)
			return v != 0, err
		case VtInt32:
			v, err := r.GetInt32(col)
			return v != 0, err
		case VtInt64:
			v, err := r.GetInt64(col)
			return v != 0, err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return v != 0, err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return v != 0, err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			if err == nil {
				return v.Unix() > 0, err
			} else {
				return false, err
			}
		case VtString:
			v, err := r.GetString(col)
			return len(v) > 0, err
		case VtBinary:
			v, err := r.GetBytes(col)
			return len(v) > 0, err
		case VtObject:
			v, err := r.GetObject(col)
			return v != nil, err
		case VtSlice:
			v, err := r.GetBytes(col)
			if nil == err {
				// Handle error from DecodeSlice, ignore for getter simplicity
				o, _ := DecodeSlice(v)
				return o != nil && !o.IsEmpty(), err
			} else {
				return false, err
			}
		case VtMap:
			v, err := r.GetBytes(col)
			if nil == err {
				// Handle error from DecodeMap, ignore for getter simplicity
				o, _ := DecodeMap(v)
				return o != nil && !o.IsEmpty(), err
			} else {
				return false, err
			}
		case VtStructure:
			v, err := r.GetBytes(col)
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
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetBool(data), nil
}

func (r *UnPackOPRow) GetInt8(col uint32) (int8, error) {
	if col >= r.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtInt8 {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := r.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := r.GetInt8(col)
			return v, err
		case VtInt16:
			v, err := r.GetInt16(col)
			return int8(v), err
		case VtInt32:
			v, err := r.GetInt32(col)
			return int8(v), err
		case VtInt64:
			v, err := r.GetInt64(col)
			return int8(v), err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return int8(v), err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return int8(v), err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			if err == nil {
				return int8(v.Unix()), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := r.GetObject(col)
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
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt8(data), nil
}

func (r *UnPackOPRow) GetInt16(col uint32) (int16, error) {
	if col >= r.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtInt16 {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := r.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := r.GetInt8(col)
			return int16(v), err
		case VtInt16:
			v, err := r.GetInt16(col)
			return v, err
		case VtInt32:
			v, err := r.GetInt32(col)
			return int16(v), err
		case VtInt64:
			v, err := r.GetInt64(col)
			return int16(v), err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return int16(v), err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return int16(v), err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			if err == nil {
				return int16(v.Unix()), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := r.GetObject(col)
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
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt16(data), nil
}

func (r *UnPackOPRow) GetInt32(col uint32) (int32, error) {
	if col >= r.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtInt32 {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := r.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := r.GetInt8(col)
			return int32(v), err
		case VtInt16:
			v, err := r.GetInt16(col)
			return int32(v), err
		case VtInt32:
			v, err := r.GetInt32(col)
			return v, err
		case VtInt64:
			v, err := r.GetInt64(col)
			return int32(v), err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return int32(v), err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return int32(v), err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			if err == nil {
				return int32(v.Unix()), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := r.GetObject(col)
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
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt32(data), nil
}

func (r *UnPackOPRow) GetInt64(col uint32) (int64, error) {
	if col >= r.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtInt64 {

		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := r.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := r.GetInt8(col)
			return int64(v), err
		case VtInt16:
			v, err := r.GetInt16(col)
			return int64(v), err
		case VtInt32:
			v, err := r.GetInt32(col)
			return int64(v), err
		case VtInt64:
			v, err := r.GetInt64(col)
			return v, err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return int64(v), err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return int64(v), err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			if err == nil {
				return v.Unix(), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := r.GetObject(col)
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
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetInt64(data), nil
}

func (r *UnPackOPRow) GetFloat32(col uint32) (float32, error) {
	if col >= r.colsCount {
		return 0, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtFloat {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := r.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := r.GetInt8(col)
			return float32(v), err
		case VtInt16:
			v, err := r.GetInt16(col)
			return float32(v), err
		case VtInt32:
			v, err := r.GetInt32(col)
			return float32(v), err
		case VtInt64:
			v, err := r.GetInt64(col)
			return float32(v), err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return v, err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return float32(v), err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			if err == nil {
				return float32(utils.DateTimeToDouble(v)), err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := r.GetObject(col)
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
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetFloat32(data), nil
}

func (r *UnPackOPRow) GetFloat64(col uint32) (float64, error) {
	if col >= r.colsCount {
		return 0, errors.New("table subscript out of range")
	}

	c := r.table.columns[col]
	if c.typ != VtDouble {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return 0, err
		case VtBool:
			v, err := r.GetBool(col)
			if v {
				return 1, err
			} else {
				return 0, err
			}
		case VtInt8:
			v, err := r.GetInt8(col)
			return float64(v), err
		case VtInt16:
			v, err := r.GetInt16(col)
			return float64(v), err
		case VtInt32:
			v, err := r.GetInt32(col)
			return float64(v), err
		case VtInt64:
			v, err := r.GetInt64(col)
			return float64(v), err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return float64(v), err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return v, err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			if err == nil {
				return float64(v.Unix()) + float64(v.Nanosecond())/1e9, err
			} else {
				return 0, err
			}
		case VtString:
			return 0, err
		case VtObject:
			v, err := r.GetObject(col)
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
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetFloat64(data), nil
}

func (r *UnPackOPRow) GetDateTime(col uint32) (time.Time, error) {
	if col >= r.colsCount {
		return time.Unix(0, 0), errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtDateTime {
		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return time.Unix(0, 0), err
		case VtBool:
			return time.Unix(0, 0), err
		case VtInt8:
			v, err := r.GetInt8(col)
			return time.Unix(int64(v), 0), err
		case VtInt16:
			v, err := r.GetInt16(col)
			return time.Unix(int64(v), 0), err
		case VtInt32:
			v, err := r.GetInt32(col)
			return time.Unix(int64(v), 0), err
		case VtInt64:
			v, err := r.GetInt64(col)
			return time.Unix(v, 0), err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return time.Unix(int64(v), 0), err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return utils.Float2DateTime(v), err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			return v, err
		default:
			return time.Unix(0, 0), err
		}
	}
	data := r.rowBuf[c.offset:(c.offset + uint32(c.length))]
	return utils.GetDateTime(data), nil
}

func (r *UnPackOPRow) GetString(col uint32) (string, error) {
	if col >= r.colsCount {
		return "", errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	if c.typ != VtString {

		err := fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
		switch c.typ {
		case VtNull:
			return "", err
		case VtBool:
			v, err := r.GetBool(col)
			if v {
				return "true", err
			} else {
				return "false", err
			}
		case VtInt8:
			v, err := r.GetInt8(col)
			return fmt.Sprintf("%v", v), err
		case VtInt16:
			v, err := r.GetInt16(col)
			return fmt.Sprintf("%v", v), err
		case VtInt32:
			v, err := r.GetInt32(col)
			return fmt.Sprintf("%v", v), err
		case VtInt64:
			v, err := r.GetInt64(col)
			return fmt.Sprintf("%v", v), err
		case VtFloat:
			v, err := r.GetFloat32(col)
			return fmt.Sprintf("%v", v), err
		case VtDouble:
			v, err := r.GetFloat64(col)
			return fmt.Sprintf("%v", v), err
		case VtDateTime:
			v, err := r.GetDateTime(col)
			return fmt.Sprintf("%v", v), err
		case VtString:
			v, err := r.GetString(col)
			return v, err
		case VtBinary:
			v, err := r.GetBytes(col)
			return fmt.Sprintf("%v", v), err
		case VtObject:
			v, err := r.GetObject(col)
			return fmt.Sprintf("%v", v), err
		case VtSlice:
			v, err := r.GetBytes(col)
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
			v, err := r.GetBytes(col)
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
			v, err := r.GetBytes(col)
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
			lenCode = r.rowVarBuf[offset]
			offset++

			switch lenCode {
			case mpBin8:
				valLen = int(utils.GetUInt8(r.rowVarBuf[offset : offset+1]))
				offset += 1

			case mpBin16:
				valLen = int(utils.GetUInt16(r.rowVarBuf[offset : offset+2]))
				offset += 2

			case mpBin32:
				valLen = int(utils.GetUInt32(r.rowVarBuf[offset : offset+4]))
				offset += 4
			}

			offset += valLen

		}
		return string(r.rowVarBuf[offset-valLen : offset]), nil
	} else {
		return string(r.rowBuf[c.offset:(c.offset + uint32(c.length))]), nil
	}
}

func (r *UnPackOPRow) GetCompoundBytes(col uint32) ([]byte, error) {
	if col >= r.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
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

		lenCode = r.rowVarBuf[offset]
		offset++
		headLen++

		switch lenCode {
		case mpBin8:
			valLen = int(utils.GetUInt8(r.rowVarBuf[offset : offset+1]))
			offset++
			headLen++

		case mpBin16:
			valLen = int(utils.GetUInt16(r.rowVarBuf[offset : offset+2]))
			offset += 2
			headLen += 2

		case mpBin32:
			valLen = int(utils.GetUInt32(r.rowVarBuf[offset : offset+4]))
			offset += 4
			headLen += 4
		}
		offset += valLen
	}
	return r.rowVarBuf[offset-valLen-headLen : offset], nil
}

func (r *UnPackOPRow) GetBytes(col uint32) ([]byte, error) {
	if col >= r.colsCount {
		return nil, errors.New("table subscript out of range")
	}
	c := r.table.columns[col]
	colType := c.typ
	if colType != VtBinary && colType != VtSlice && colType != VtMap && colType != VtStructure {
		//计算所需获取数据的偏差
		return nil, fmt.Errorf("column data type error, col %v index: %v type: %v", c.name, col, c.typ)
	}

	if c.length == 0 {
		offset := 0
		valLen := 0
		for i := uint32(0); i <= c.cell; i++ {
			lenCode := r.rowVarBuf[offset]
			offset++

			switch lenCode {
			case mpBin8:
				valLen = int(utils.GetUInt8(r.rowVarBuf[offset : offset+1]))
				offset++

			case mpBin16:
				valLen = int(utils.GetUInt16(r.rowVarBuf[offset : offset+2]))
				offset += 2

			case mpBin32:
				valLen = int(utils.GetUInt32(r.rowVarBuf[offset : offset+4]))
				offset += 4
			}
			if valLen > 0 {
				offset += valLen
			}
		}
		return r.rowVarBuf[offset-valLen : offset], nil
	} else {
		return r.rowBuf[c.offset:(c.offset + uint32(c.length))], nil
	}

}

func (r *UnPackOPRow) GetTable() (*Table, error) {
	return r.table, nil
}

func (r *UnPackOPRow) SetRow(row []byte) {
	if nil == r.rowBuf {
		r.rowBuf = row
		r.rowVarBuf = r.rowBuf[r.table.fixedLength+r.table.bitLength:]
	}
}

func (r *UnPackOPRow) SetTable(table *Table) {
	if nil == r.table {
		r.table = table
		r.colsCount = uint32(len(table.GetColumns()))
	}
}
