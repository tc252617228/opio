package opio

import "fmt"

// Response -
type Response struct {
	Request
}

func NewResponse(request *Request) *Response {
	if nil == request {
		return nil
	}
	res := &Response{}
	res.buff = request.buff
	res.props = make(map[string]interface{}, propCapacity)
	res.table = NewEmptyTable()
	res.SetErrNo(0)
	return res
}

// SetError -
func (resp *Response) SetError(err string) {
	resp.Lock()
	defer resp.Unlock()
	resp.props[PropError] = err
}

// SetErrNo -
func (resp *Response) SetErrNo(err int32) {
	resp.Lock()
	defer resp.Unlock()
	resp.props[PropErrNo] = err
}

func (resp *Response) GetError() string {
	resp.Lock()
	defer resp.Unlock()
	if 0 == len(resp.props) {
		return ""
	}
	val, ok := resp.props[PropError]
	if ok {
		return val.(string)
	}
	return ""
}

func (resp *Response) GetErrNo() int32 {
	resp.Lock()
	defer resp.Unlock()
	if 0 == len(resp.props) {
		return -1
	}
	val, ok := resp.props[PropErrNo]
	if ok {
		switch val.(type) {
		case int8:
			return int32(val.(int8))
		case uint8:
			return int32(val.(uint8))
		case int16:
			return int32(val.(int16))
		case uint16:
			return int32(val.(uint16))
		case int32:
			return val.(int32)
		case uint32:
			return int32(val.(uint32))
		case int64:
			return int32(val.(int64))
		case uint64:
			return int32(val.(uint64))
		case int:
			return val.(int32)
		}
	}
	return -1
}

// encodeAndWriteToBuffer encodes the response properties and table into the buffer.
// 注意：这个方法是根据 Request.write 的行为推断的，可能需要根据实际逻辑调整。
func (resp *Response) encodeAndWriteToBuffer() error {
	resp.Lock()
	defer resp.Unlock()

	// 1. 编码属性 (props map)
	// 使用顶层 EncodeMap 函数
	_, propsData, err := EncodeMap(resp.props)
	if err != nil {
		return fmt.Errorf("response write: failed to encode props map: %w", err)
	}
	// 将编码后的属性数据写入缓冲区
	err = resp.buff.PutBytes(propsData) // 使用 PutBytes
	if err != nil {
		return fmt.Errorf("response write: failed to write encoded props to buffer: %w", err)
	}

	// 2. 编码表格 (table)
	// Table 没有 Encode 方法，直接写入每行的原始数据
	if resp.table != nil && resp.table.rowCount > 0 {
		// 写入行数 (假设需要，这里用 int32)
		// 注意：协议可能需要不同的方式表示表格数据，这里只是一个可能的实现
		// err = resp.buff.PutInt32(int32(resp.table.rowCount)) // 示例：写入行数
		// if err != nil {
		// 	return fmt.Errorf("response write: failed to write row count: %w", err)
		// }

		// 迭代写入每行数据
		for i, row := range resp.table.rows {
			if row.Data == nil {
				// 处理空行数据？根据协议决定是跳过、写入空标记还是报错
				// 暂时跳过空行
				continue
			}
			err = resp.buff.PutBytes(row.Data) // 使用 PutBytes
			if err != nil {
				return fmt.Errorf("response write: failed to write row %d data to buffer: %w", i, err)
			}
		}
	} else {
		// 如果没有表格数据，可能需要写入一个空标记，例如 nil 或空 map/slice
		// 暂时不写入任何内容表示空表格
		// err = resp.buff.EncodeNil() // 示例
		// if err != nil {
		// 	return fmt.Errorf("response write: failed to write nil for empty table: %w", err)
		// }
	}

	return nil // 所有写入成功
}

// Write -
func (resp *Response) Write(use_mode_in bool) error {
	if use_mode_in {
		resp.buff.UseOptionIn()
	}
	return resp.encodeAndWriteToBuffer() // 调用重命名后的方法
}

// WriteAndFlush -
func (resp *Response) WriteAndFlush() error {
	resp.buff.UseOptionIn()
	err := resp.encodeAndWriteToBuffer() // 调用重命名后的方法
	if err != nil {
		return err
	}

	_ = resp.buff.EncodeNil()
	err = resp.buff.Flush(true)
	return err
}

func (resp *Response) Echo() (bool, error) {
	if resp != nil {
		_, _ = resp.buff.ReadEcho()
	}
	return true, nil
}
