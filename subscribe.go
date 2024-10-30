package opio

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

type ConInfo struct {
	host    string
	port    int32
	timeout int32
	user    string
	pass    string
	tbName  string
}

type Subscribe struct {
	conn        *IOConnect
	conf        *ConInfo
	isClose     bool
	snapshot    bool
	initialized bool
	keyName     string
	subType     int
	keys        map[interface{}]struct{}
	keysUpdated bool
	Request
}

func (op *IOConnect) NewSubscribe(tableName string) (*Subscribe, error) {
	nop, err := op.copyConn()
	if err != nil {
		return nil, err
	}
	sub := &Subscribe{}

	sub.conf = &ConInfo{
		host:    op.host,
		port:    op.port,
		timeout: op.timeout,
		user:    op.user,
		pass:    op.pass,
		tbName:  tableName,
	}

	sub.conn = nop
	sub.buff = nop.io
	sub.props = make(map[string]interface{}, propCapacity)
	sub.keys = make(map[interface{}]struct{})
	table := NewTable(tableName, 0)
	table.AddColumn("*", VtNull, 0)
	_ = sub.SetTable(table)
	sub.table = table
	return sub, nil
}

// noinspection GoUnusedExportedFunction
func MakeSubscribe(conInfo *ConInfo) (*Subscribe, error) {
	nop, err := Init(conInfo.host, int(conInfo.port), int(conInfo.timeout), conInfo.user, conInfo.pass)
	if err != nil {
		return nil, err
	}
	sub := &Subscribe{}
	sub.conf = conInfo

	sub.conn = nop
	sub.buff = nop.io
	sub.props = make(map[string]interface{}, propCapacity)
	table := NewTable(conInfo.tbName, 0)
	table.AddColumn("*", VtNull, 0)
	_ = sub.SetTable(table)
	sub.table = table
	return sub, nil
}

func (sub *Subscribe) SetSnapshot(v bool) error {
	if !sub.initialized {
		sub.snapshot = v
		return nil
	}
	return errors.New("Subscribe Runing!!")
}

func (sub *Subscribe) Close() {
	if !sub.isClose && sub.conn != nil {
		sub.isClose = true
		_ = sub.conn.Close()
		sub.conf = nil
		sub.conn = nil
	}
}

// 与C API保持一致 by PB
func (sub *Subscribe) InitSubscribe(key interface{}, keyName string, callback func(res *Response)) error {
	if !sub.initialized {
		sub.initialized = true
		sub.SetID(int64(rand.Int()))
		sub.SetService("openplant")
		sub.SetAction(ActionSelect)
		sub.Set("Async", 1)
		sub.Set("Snapshot", sub.snapshot)
		switch v := key.(type) {
		case []int32:
			sub.subType = 1
			sub.SetIndexesInt32(keyName, v)
			for _, v_ := range v {
				sub.keys[v_] = struct{}{}
			}
		case []int64:
			sub.subType = 2
			sub.SetIndexesInt64(keyName, v)
			for _, v_ := range v {
				sub.keys[v_] = struct{}{}
			}
		case []string:
			sub.subType = 3
			sub.SetIndexesString(keyName, v)
			for _, v_ := range v {
				sub.keys[v_] = struct{}{}
			}
		}
		sub.keyName = keyName
		if err := sub.makeSubReq(); err != nil {
			return err
		}
		res := &Response{}
		res.buff = sub.buff
		go func() {
			for !sub.isClose {
				res.Reset()
				e := res.Read()
				if e == nil {
					callback(res)
				} else {
					res.SetError(e.Error())
					res.SetErrNo(-97)
					callback(res)
					for !sub.isClose {
						if con, err := sub.conn.copyConn(); err != nil {
							time.Sleep(time.Second * 20)
						} else {
							sub.conn = con
							sub.buff = con.io
							res.buff = sub.buff
							res.SetErrNo(-90)
							callback(res)

							sub.makeSubReq()
							break
						}
					}
				}
			}
		}()
	}
	return nil
}

func (sub *Subscribe) makeSubReq() error {

	//keys被更新过需要重新setIndexes
	if sub.initialized && sub.keysUpdated {
		switch sub.subType {
		case 1:
			keyArr := make([]int32, 0, len(sub.keys))
			for key := range sub.keys {
				keyArr = append(keyArr, key.(int32))
			}
			sub.SetIndexesInt32(sub.keyName, keyArr)
		case 2:
			keyArr := make([]int64, 0, len(sub.keys))
			for key := range sub.keys {
				keyArr = append(keyArr, key.(int64))
			}
			sub.SetIndexesInt64(sub.keyName, keyArr)
		case 3:
			keyArr := make([]string, 0, len(sub.keys))
			for key := range sub.keys {
				keyArr = append(keyArr, key.(string))
			}
			sub.SetIndexesString(sub.keyName, keyArr)
		default:
		}
	}
	err := sub.Write()
	if err != nil {
		return err
	}
	sub.Flush()
	return nil
}

// add by PB
func (sub *Subscribe) change(key interface{}, changeType int) error {
	if sub.initialized && !sub.isClose {
		sub.keysUpdated = true
		sub.SetID(int64(rand.Int()))
		sub.Set("Subscribe", changeType)
		switch sub.subType {
		case 1:
			switch v := key.(type) {
			case []int32:
				sub.SetIndexesInt32(sub.keyName, v)
				for _, v_ := range v {
					if changeType == 1 {
						sub.keys[v_] = struct{}{}
					} else {
						delete(sub.keys, v_)
					}
				}
			default:
				return errors.New("key must int32[] ")
			}
		case 2:
			switch v := key.(type) {
			case []int64:
				sub.SetIndexesInt64(sub.keyName, v)
				for _, v_ := range v {
					if changeType == 1 {
						sub.keys[v_] = struct{}{}
					} else {
						delete(sub.keys, v_)
					}
				}
			default:
				return errors.New("key must int64[] ")
			}
		case 3:
			switch v := key.(type) {
			case []string:
				sub.SetIndexesString(sub.keyName, v)
				for _, v_ := range v {
					if changeType == 1 {
						sub.keys[v_] = struct{}{}
					} else {
						delete(sub.keys, v_)
					}
				}
			default:
				return errors.New("key must string[] ")
			}
		}
		_ = sub.Write()
		sub.Flush()
	} else {
		fmt.Println("not conn")
	}
	return nil
}

func (sub *Subscribe) Subscribe(key interface{}) error {
	return sub.change(key, 1)
}

func (sub *Subscribe) UnSubscribe(key interface{}) error {
	return sub.change(key, 0)
}

func (sub *Subscribe) GetConInfo() *ConInfo {
	return sub.conf
}
