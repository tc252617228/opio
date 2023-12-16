package opio

import (
	"errors"

	"github.com/tc252617228/opio/driver/internal/utils"
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

// Filter -
type Filter struct {
	left  string
	oper  uint8
	right string

	relation uint8
}

// noinspection ALL
func NewFilter(left string, oper byte, right string, relation byte) *Filter {
	return &Filter{left, oper, right, relation}
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

	f.left, err = io.DecodeString()
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

	f.oper, err = io.DecodeUint8()
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

	f.right, err = io.DecodeString()
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
	f.relation, err = io.DecodeUint8()
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
	err = io.EncodeString(f.left)
	if err != nil {
		return err
	}

	err = io.EncodeString("O")
	if err != nil {
		return err
	}
	err = io.EncodeUint8(f.oper)
	if err != nil {
		return err
	}

	err = io.EncodeString("R")
	if err != nil {
		return err
	}
	err = io.EncodeString(f.right)
	if err != nil {
		return err
	}

	err = io.EncodeString("Or")
	if err != nil {
		return err
	}
	err = io.EncodeUint8(f.relation)
	if err != nil {
		return err
	}
	return nil
}
