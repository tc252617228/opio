package opio

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

// write -
func (resp *Response) Write(use_mode_in bool) error {
	if use_mode_in {
		resp.buff.UseOptionIn()
	}
	return resp.write()
}

// write -
func (resp *Response) WriteAndFlush() error {
	resp.buff.UseOptionIn()
	err := resp.write()
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
