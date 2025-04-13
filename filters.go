package opio

import (
	"errors"

	"opio/internal/utils"
)

type Filters struct {
	filters []Filter
	utils.ReaderWriter
}

// read -
func (fs *Filters) read(io *utils.Buffer) error {
	size, err := io.DecodeArrayStart()
	if err != nil {
		return err
	}
	data := make([]Filter, size)
	for i := uint32(0); i < size; i++ {
		err := data[i].read(io)
		if err != nil {
			return err
		}
	}
	fs.filters = data
	return nil
}

func (fs *Filters) GetFilters() *[]Filter {
	return &fs.filters
}

// write -
func (fs *Filters) write(io *utils.Buffer) error {

	size := uint32(len(fs.filters))
	err := io.EncodeArrayStart(size)
	if err != nil {
		return err
	}

	for i := uint32(0); i < size; i++ {
		err := fs.filters[i].write(io)
		if err != nil {
			return err
		}
	}
	return nil
}

// Filter 定义了查询、更新或删除操作的过滤条件。
type Filter struct {
	Left     string // 左操作数（通常是列名）
	Operator uint8  // 操作符 (见 opio.Oper* 常量)
	Right    string // 右操作数（值）
	Relation uint8  // 与下一个过滤条件的关系 (见 opio.Relation* 常量)
}

// NewFilter 创建一个新的 Filter 实例。
// noinspection ALL
func NewFilter(left string, oper uint8, right string, relation uint8) *Filter {
	return &Filter{Left: left, Operator: oper, Right: right, Relation: relation}
}

// read -
// noinspection ALL
func (f *Filter) read(io *utils.Buffer) (err error) {

	size, err := io.DecodeMapStart()
	if err != nil {
		return err
	}
	if size != 4 {
		return errors.New("data error")
	}
	//left
	left, err := io.DecodeString()
	if err != nil {
		return err
	} else if left != "L" {
		return errors.New("key error")
	}

	f.Left, err = io.DecodeString()
	if err != nil {
		return err
	}

	//oper
	oper, err := io.DecodeString()
	if err != nil {
		return err
	} else if oper != "O" {
		return errors.New("key error")
	}

	f.Operator, err = io.DecodeUint8()
	if err != nil {
		return err
	}

	//right
	right, err := io.DecodeString()
	if err != nil {
		return err
	} else if right != "R" {
		return errors.New("key error")
	}

	f.Right, err = io.DecodeString()
	if err != nil {
		return err
	}

	//relation
	relation, err := io.DecodeString()
	if err != nil {
		return err
	} else if relation != "Or" {
		return errors.New("key error")
	}
	f.Relation, err = io.DecodeUint8()
	if err != nil {
		return err
	}

	return err
}

// write -
// noinspection ALL
func (f *Filter) write(io *utils.Buffer) (err error) {

	io.EncodeMapStart(4)

	err = io.EncodeString("L")
	if err != nil {
		return err
	}
	err = io.EncodeString(f.Left)
	if err != nil {
		return err
	}

	err = io.EncodeString("O")
	if err != nil {
		return err
	}
	err = io.EncodeUint8(f.Operator)
	if err != nil {
		return err
	}

	err = io.EncodeString("R")
	if err != nil {
		return err
	}
	err = io.EncodeString(f.Right)
	if err != nil {
		return err
	}

	err = io.EncodeString("Or")
	if err != nil {
		return err
	}
	err = io.EncodeUint8(f.Relation)
	if err != nil {
		return err
	}
	return nil
}
