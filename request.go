package opio

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"

	"github.com/tc252617228/opio/internal/utils"
)

// Request -
type Request struct {
	buff    *utils.Buffer
	props   map[string]interface{} //属性集合
	table   *Table
	dataSet *OPDataSet

	sync.Mutex
}

func (req *Request) Reset() {
	req.Lock()
	defer req.Unlock()
	// remake map
	req.props = make(map[string]interface{}, propCapacity)
	if req.table != nil {
		//快速释放已开辟的 row 缓冲区
		req.table.Clear()
	}
	req.table = NewEmptyTable()
	req.dataSet = nil
}

// SetID -
func (req *Request) SetID(id int64) {
	req.Lock()
	defer req.Unlock()
	req.props[PropReqId] = id
}

func (req *Request) GetId() int64 {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return 0
	}
	val, ok := req.props[PropReqId]
	if ok {
		switch v := val.(type) {
		case string:
			resV, e := strconv.ParseInt(val.(string), 10, 64)
			if e != nil {
				return 0
			}
			return resV
		case int64:
			return v
		}
	}
	return 0
}

// SetService -
func (req *Request) SetService(name string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropService] = name
}

func (req *Request) GetService() string {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return ""
	}
	val, ok := req.props[PropService]
	if ok {
		return val.(string)
	}
	return ""
}

// SetAction -
func (req *Request) SetAction(action string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropAction] = action
}

// GetAction -
func (req *Request) GetAction() string {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return ""
	}
	val, ok := req.props[PropAction]
	if ok {
		return val.(string)
	}
	return ""
}

// SetSubject -
func (req *Request) SetSubject(subject string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropSubject] = subject
}

// GetSubject -
func (req *Request) GetSubject() string {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return ""
	}
	val, ok := req.props[PropSubject]
	if ok {
		return val.(string)
	}
	return ""
}

// SetTableName -
func (req *Request) SetTableName(table string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropTable] = table
}

// GetTableName -
func (req *Request) GetTableName() string {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return ""
	}
	val, ok := req.props[PropTable]
	if ok {
		return val.(string)
	}
	return ""
}

// SetTable -
func (req *Request) SetTable(data *Table) error {
	req.Lock()
	defer req.Unlock()
	if len(data.errors) > 0 {
		return errors.New(" Unhandled exceptions in table ")
	}
	req.props[PropTable] = data.Name()
	req.props[PropColumns] = data
	req.table = data
	return nil
}

// SetToken -
func (req *Request) SetToken(token string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropToken] = token
}

// GetToken -
func (req *Request) GetToken() string {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return ""
	}
	val, ok := req.props[PropToken]
	if ok {
		return val.(string)
	}
	return ""
}

// SetDB -
func (req *Request) SetDB(db string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropDB] = db
}

// GetDB -
func (req *Request) GetDB() string {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return ""
	}
	val, ok := req.props[PropDB]
	if ok {
		return val.(string)
	}
	return ""
}

func (req *Request) SetTimestamp(t float64) {
	req.Lock()
	defer req.Unlock()
	req.props[PropTimestamp] = t
}

func (req *Request) GetTimestamp() float64 {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return -1
	}
	val, ok := req.props[PropTimestamp]
	if ok {
		return val.(float64)
	}
	return -1
}

// SetOption -
func (req *Request) SetOption(option string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropOption] = option
}

// SetOrderBy -
func (req *Request) SetOrderBy(order string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropOrderBy] = order
}

// SetLimit -
func (req *Request) SetLimit(limit string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropLimit] = limit
}

// SetAsync -
func (req *Request) SetAsync(async int32) {
	req.Lock()
	defer req.Unlock()
	req.props[PropAsync] = async
}

// SetIndexesInt32 -
func (req *Request) SetIndexesInt32(name string, indices []int32) {
	req.Lock()
	defer req.Unlock()
	i := Indexs{}
	i.index_type = INT32_ARRAY
	i.key = name
	i.key_i32 = indices
	req.props[PropIndexes] = i
}

// SetIndexesInt64 -
func (req *Request) SetIndexesInt64(name string, indices []int64) {
	req.Lock()
	defer req.Unlock()
	i := Indexs{}
	i.index_type = INT64_ARRAY
	i.key = name
	i.key_i64 = indices
	req.props[PropIndexes] = i
}

// SetIndexesString -
func (req *Request) SetIndexesString(name string, indices []string) {
	req.Lock()
	defer req.Unlock()
	i := Indexs{}
	i.index_type = STRING_ARRAY
	i.key = name
	i.key_str = indices
	req.props[PropIndexes] = i
}

// SetFilters -
func (req *Request) SetFilters(filters []Filter) {
	req.Lock()
	defer req.Unlock()
	f := Filters{}
	f.filters = make([]Filter, 0, len(filters))
	f.filters = append(f.filters, filters...)
	req.props[PropFilters] = f
}

// SetSQL -
func (req *Request) SetSQL(SQL string) {
	req.Lock()
	defer req.Unlock()
	req.props[PropSQL] = SQL
}

// GetSQL -
func (req *Request) GetSQL() string {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return ""
	}
	val, ok := req.props[PropSQL]
	if ok {
		return val.(string)
	}
	return ""
}

// Set -
func (req *Request) Set(k string, v interface{}) {
	req.Lock()
	defer req.Unlock()
	if nil != req.props {
		req.props[k] = v
	}
}

// Get -
func (req *Request) Get(k string) interface{} {
	req.Lock()
	defer req.Unlock()
	if 0 == len(req.props) {
		return nil
	}
	val, ok := req.props[k]
	if ok {
		return val
	}
	return nil
}

func (req *Request) GetProp() map[string]interface{} {
	req.Lock()
	defer req.Unlock()
	return req.props
}

// ReadContent -
func (req *Request) GetDataSet() (table *OPDataSet) {
	return req.dataSet
}

func (req *Request) write() error {
	req.Lock()
	defer req.Unlock()
	var err error
	size := len(req.props)
	_ = req.buff.EncodeMapStart(uint32(size))
	io := req.buff

	if v, ok := req.props[PropTable]; ok {
		err = io.EncodeString(PropTable)
		if err != nil {
			fmt.Println("key:", PropTable, "value: ", v, "err:", err)
			//logs.Warn("key:", PropTable, "value: ", v, "err:", err)
			fmt.Println(string(debug.Stack()))
			//logs.Warn(string(debug.Stack()))
			return err
		}
		err = req.buff.EncodeValue(v)
		if err != nil {
			fmt.Println("key:", PropTable, "value: ", v, "err:", err)
			//logs.Warn("key:", PropTable, "value: ", v, "err:", err)
			fmt.Println(string(debug.Stack()))
			//logs.Warn(string(debug.Stack()))
			return err
		}
	}

	for key, v := range req.props {
		if key == PropTable {
			continue
		}
		err = io.EncodeString(key)
		if err == nil {
			switch key {
			case PropColumns:
				cols := v.(*Table)
				err = cols.write(io)
			case PropIndexes:
				i := v.(Indexs)
				err = i.write(io)
			case PropFilters:
				f := v.(Filters)
				err = f.write(io)
			default:
				err = req.buff.EncodeValue(v)
			}
		}
		if err != nil {
			fmt.Println(string(debug.Stack()))
			//logs.Error(string(debug.Stack()))
			fmt.Println("key:", key, "value: ", v, "err:", err)
			//logs.Error("key:", key, "value: ", v, "err:", err)
			break
		}
	}

	return err
}

// write -
func (req *Request) Write() error {
	return req.write()
}

// WriteContent -
func (req *Request) WriteContent(data *Table) error {
	err := req.buff.EncodeArrayStart(uint32(data.rowCount))
	if err != nil {
		fmt.Println("WriteContent EncodeArrayStart", err)
		//logs.Error(" WriteContent EncodeArrayStart", err)
	}

	for _, v := range data.rows {
		size := uint32(len(v.Data))
		err := req.buff.EncodeExtendLen(size, VtRow)
		if err != nil {
			fmt.Println("WriteContent EncodeExtendLen", err)
			//logs.Error(" WriteContent EncodeExtendLen", err)
			return err
		}
		err = req.buff.PutBytes(v.Data)
		if err != nil {
			fmt.Println("WriteContent PutBytes", err)
			//logs.Error("WriteContent PutBytes ", err)
			return err
		}
	}
	return nil
}

// Flush -
func (req *Request) Flush() {
	_ = req.buff.EncodeNil()
	_ = req.buff.Flush(true)
}

// WriteAndFlush -
func (req *Request) WriteAndFlush() error {
	err := req.write()
	if err != nil {
		return err
	}
	err = req.buff.EncodeNil()
	if err != nil {
		return err
	}
	err = req.buff.Flush(true)
	return err
}

func (req *Request) Echo() {
	_, _ = req.buff.ReadEcho()
}

func (req *Request) aliveDetectFilter(buf []byte) (bool, error) {
	if buf[20] == 0x40 && buf[19] == 0x30 && buf[18] == 0x20 && buf[17] == 0x10 {
		req.buff.GetBytes(buf) //心跳buf中内容全读出来
		return true, req.buff.DirectWrite([]byte{buf[16]})
	}
	return false, nil
}

// read -
func (req *Request) Read() error {
	req.Lock()
	defer req.Unlock()

	defer func() {
		// TODO IO 异常处理
		req.dataSet = &OPDataSet{}
		req.dataSet.io = req.buff
		req.dataSet.table = req.table
		req.dataSet.colsCount = req.table.colCount
		req.dataSet.isFirst = true
	}()

	var io = req.buff

	var err error

	// 判断是否是连接测试请求
	// 可能会有连续心跳情况，故这里要用死循环
	for {
		buf := make([]byte, 21)
		isHeartHead, err := io.PeekN(buf)
		if err != nil {
			return err
		}
		if !isHeartHead {
			break
		}
		isDetectReq, err := req.aliveDetectFilter(buf)
		if err != nil {
			return err
		}
		if !isDetectReq {
			break
		}
	}

	size, err := io.DecodeMapStart()
	if err != nil {
		return err
	}
	for i := uint32(0); i < size; i++ {
		key, err := io.DecodeString()
		if err == nil {
			switch key {
			case PropTable:
				value, e2 := io.DecodeString()
				if e2 != nil {
					return e2
				}
				req.props[key] = value
				// set table name
				req.table.name = value

			case PropColumns:
				cols := Columns{}
				err = cols.read(io)
				if err != nil {
					return err
				}
				for _, v := range cols.columns {
					req.table.AddColumnExtension(v.name, int(v.typ), int(v.length), v.ext)
				}
				req.props[key] = cols

			case PropIndexes:
				indexs := Indexs{}
				err = indexs.read(io)
				if err != nil {
					return err
				}
				req.props[key] = indexs

			case PropFilters:
				f := Filters{}
				err := f.read(io)
				if err != nil {
					return err
				}
				req.props[key] = f

			default:
				value, e2 := io.DecodeValue()
				if e2 != nil {
					return e2
				}
				req.props[key] = value
			}
		}
		if err != nil {
			break
		}
	}
	return nil
}

// read opapi v4
func (req *Request) ReadV4() error {
	//defer func() {
	//	// TODO IO 异常处理
	//	req.dataSet = &OPDataSet{}
	//	req.dataSet.io = req.buff
	//	req.dataSet.table = req.table
	//	req.dataSet.colsCount = req.table.colCount
	//	req.dataSet.isFirst = true
	//}()
	//
	//var io = req.buff
	//var err error

	return nil
}

// GetResponse -
func (req *Request) GetResponse() (res *Response, err error) {
	res = req.MakeResponse()
	_ = res.Read()
	return res, nil
}

func (req *Request) MakeResponse() *Response {
	req.Lock()
	defer req.Unlock()

	res := &Response{}
	res.buff = req.buff
	res.table = &Table{}
	res.props = make(map[string]interface{}, propCapacity)
	return res
}
