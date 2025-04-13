package opio

import (
	"errors"
	"fmt"

	"opio/internal/utils"
)

type Indexs struct {
	index_type uint8
	key        string
	key_str    []string
	key_i32    []int32
	key_i64    []int64
	utils.ReaderWriter
}

// read -
func (this *Indexs) read(io *utils.Buffer) (err error) {
	_, v_type, err := io.DecodeExtendStart()

	if err != nil {
		return err
	}

	data_size, err := io.DecodeArrayStart()
	if err != nil {
		return err
	}

	switch v_type {
	case INT32_ARRAY:
		this.key_i32 = make([]int32, data_size)
		for i := uint32(0); i < data_size; i++ {
			v, err := io.DecodeInt32()
			if err != nil {
				return err
			}
			this.key_i32[i] = int32(v)
		}
	case INT64_ARRAY:
		this.key_i64 = make([]int64, data_size)
		for i := uint32(0); i < data_size; i++ {
			v, err := io.DecodeInt64()
			if err != nil {
				return err
			}
			this.key_i64[i] = v
		}
	case STRING_ARRAY:
		this.key_str = make([]string, data_size)
		for i := uint32(0); i < data_size; i++ {
			v, err := io.DecodeString()
			if err != nil {
				return err
			}
			this.key_str[i] = v
		}
	default:
		err = errors.New(fmt.Sprintf("DecodeIndexs error value type=%d", v_type))
	}
	return err
}

// write -
func (this *Indexs) write(io *utils.Buffer) (err error) {
	switch this.index_type {
	case STRING_ARRAY:
		key_str_len := 0
		for _, v := range this.key_str {
			_temp := len(v)
			key_str_len += int(mpStringExtra(_temp)) + _temp
		}
		err := io.EncodeExtendLen(uint32(key_str_len), this.index_type)
		if err != nil {
			return err
		}

		err = io.EncodeArrayStart(uint32(len(this.key_str)))
		if err != nil {
			return err
		}
		for _, v := range this.key_str {
			err = io.EncodeString(v)
		}

	case INT32_ARRAY:
		size := uint32(len(this.key_i32) * 5)
		err := io.EncodeExtendLen(size, this.index_type)
		if err != nil {
			return err
		}

		err = io.EncodeArrayStart(uint32(len(this.key_i32)))
		if err != nil {
			return err
		}
		for _, v := range this.key_i32 {
			err = io.EncodeInt32(v)
		}
	case INT64_ARRAY:
		size := uint32(len(this.key_i64) * 9)
		err := io.EncodeExtendLen(size, this.index_type)
		if err != nil {
			return err
		}

		err = io.EncodeArrayStart(uint32(len(this.key_i64)))
		if err != nil {
			return err
		}
		for _, v := range this.key_i64 {
			err = io.EncodeInt64(v)
		}
	default:
	}
	return err
}

func (this *Indexs) GetKeys() interface{} {
	if len(this.key_i32) > 0 {
		return this.key_i32
	} else if len(this.key_i64) > 0 {
		return this.key_i64
	} else if len(this.key_str) > 0 {
		return this.key_str
	} else {
		return nil
	}
}
