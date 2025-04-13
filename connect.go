package opio

import (
	"errors"
	"fmt"
	"net"
	"time"

	"opio/internal/utils"
)

// IOConnect -
type IOConnect struct {
	conn    net.Conn
	host    string
	port    int32
	timeout int32
	user    string
	pass    string
	version int32
	info    string
	random  []byte
	client  string // client address
	errno   int32
	io      *utils.Buffer
}

func (op *IOConnect) GetAddress() string {
	return op.conn.RemoteAddr().String()
}

func (op *IOConnect) Info() string {
	return op.info
}

// Init - 创建新连接
func Init(host string, port int, timeout int, user string, pass string) (*IOConnect, error) {
	op := &IOConnect{nil, host, int32(port), int32(timeout), user, pass, 0, "", nil, "", 0, nil}
	// 使用 net.JoinHostPort 兼容 IPv6
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	op.conn = conn
	op.io = utils.NewBuffer(op.conn, max_buffer_size)
	err = op.loginExt()
	if err != nil {
		return nil, err
	}
	return op, nil
}

func (op *IOConnect) copyConn() (*IOConnect, error) {
	return Init(op.host, int(op.port), int(op.timeout), op.user, op.pass)
}

func (op *IOConnect) Copy() (*IOConnect, error) {
	return op.copyConn()
}

// noinspection GoUnusedExportedFunction
func InitConn(ip string, port int, timeOut int) (*IOConnect, error) {
	op := &IOConnect{nil, ip, int32(port), int32(timeOut), "", "", 0, "", nil, "", 0, nil}
	// 使用 net.JoinHostPort 兼容 IPv6
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	op.conn = conn
	op.io = utils.NewBuffer(op.conn, max_buffer_size)
	return op, nil
}

// noinspection GoUnusedExportedFunction
func InitConnTCP(conn net.Conn) *IOConnect {
	op := &IOConnect{}
	op.conn = conn
	op.io = utils.NewBuffer(op.conn, max_buffer_size)
	return op
}

func MakeUUID(key string) int64 {
	var a uint32
	var b uint32
	for i, v := range key {
		c := uint32(_uppercase[v])
		if (i & 1) == 0 {
			a ^= ((a << 7) ^ c) ^ (a >> 3)
		} else {
			a ^= ^(((a << 11) ^ c) ^ (a >> 5))
		}
		b = b*131 + c
	}
	return ((int64(a) & 0x7FFFFFFF) << 32) | int64(b)
}

func (op *IOConnect) login() error {
	defer op.io.Reset()
	head := []byte{0, 0, 0, 0}
	i, err := op.conn.Read(head)
	if err != nil {
		return err
	} else if i == 1 {
		_, err = op.conn.Write([]byte{255})
		if err == nil {
			temp := []byte{0}
			_, err = op.conn.Read(temp)
			if err == nil && temp[0] == 255 {

			}
		}
		return errors.New("login -- wall model error ")

	} else if i == 4 {
		var err error
		buf := make([]byte, 100)
		_, err = op.conn.Read(buf)
		if err != nil {
			return err
		}

		// server: SERVER_VERSION(60) + session(4) + SCRAMBLE(20) + 12 + ver(4)
		n := 60
		for buf[n-1] == 0 && n > 0 {
			n--
		}
		op.info = string(buf[:n])
		op.random = make([]byte, 20)
		copy(op.random, buf[64:84])
		op.version = utils.GetInt32(buf[96:])

		// client: client_info(40) + session(4) + user(16) + passwd(2+20) + (18)
		utils.Memset(buf, 0)
		copy(buf[44:60], []byte(op.user))
		if len(op.pass) > 0 {
			reply := utils.Scramle(op.random, []byte(op.pass))
			utils.PutInt16(buf[60:], int16(len(reply)))
			copy(buf[62:82], reply)
		}

		// write
		_ = op.io.PutBytes(buf)
		err = op.io.Flush(true)
		if err != nil {
			return err
		}

		//  magic, peer, error, magic
		err = op.io.GetBytes(buf[:16])
		if err != nil {
			return err
		}
		op.client = fmt.Sprintf("%d.%d.%d.%d", uint8(buf[4]), uint8(buf[5]), uint8(buf[6]), uint8(buf[7]))
		ret := utils.GetInt32(buf[8:])
		if ret != 0 {
			e := fmt.Sprintf("login %s:%d error %d", op.host, op.port, ret)
			return errors.New(e)
		}
		return nil
	} else {
		return errors.New("read header error")
	}

}

func (op *IOConnect) loginExt() error {

	defer op.io.Reset()

	var err error
	buf := make([]byte, 100)
	err = op.io.GetBytes(buf)
	if err != nil {
		return err
	}
	// server: SERVER_VERSION(60) + session(4) + SCRAMBLE(20) + 12 + ver(4)
	n := 60
	for buf[n-1] == 0 && n > 0 {
		n--
	}
	op.info = string(buf[:n])
	op.random = make([]byte, 20)
	copy(op.random, buf[64:84])
	op.version = utils.GetInt32(buf[96:])

	// client: client_info(40) + session(4) + user(16) + passwd(2+20) + (18)
	utils.Memset(buf, 0)
	copy(buf[44:60], []byte(op.user))
	if len(op.pass) > 0 {
		reply := utils.Scramle(op.random, []byte(op.pass))
		utils.PutInt16(buf[60:], int16(len(reply)))
		copy(buf[62:82], reply)
	}

	// write
	_ = op.io.PutBytes(buf)
	err = op.io.Flush(true)
	if err != nil {
		return err
	}

	//  magic, peer, error, magic
	err = op.io.GetBytes(buf[:16])
	if err != nil {
		return err
	}
	op.client = fmt.Sprintf("%d.%d.%d.%d", buf[4], buf[5], buf[6], buf[7])
	ret := utils.GetInt32(buf[8:])
	if ret != 0 {
		e := fmt.Sprintf("login %s:%d error %d", op.host, op.port, ret)
		return errors.New(e)
	}

	return nil
}

// Alive -
func (op *IOConnect) Alive() bool {
	var err error
	b := []byte{
		0x10, 0x20, 0x30, 0x40,
		0, 0, 0, 110,
		0x46, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0xA5, //will come back
		0x10, 0x20, 0x30, 0x40,
	}
	if err = op.conn.SetDeadline(time.Now().Add(time.Millisecond * 10)); err != nil {
		//logs.Warn(" SetDeadline  Err", err)
		return false
	}
	if _, err = op.conn.Write(b); err != nil {
		//logs.Warn(" Write Alive message Err", err)
		return false
	}
	r := make([]byte, 1)
	if _, err = op.conn.Read(r); err != nil {
		//logs.Warn("Read Alive message Err", err)
		return false
	}
	//back to normal deadline time
	if err := op.conn.SetDeadline(time.Time{}); err != nil {
		//logs.Warn("SetDeadline Err", err)
		return false
	}
	return true
}

// Version -
func (op *IOConnect) Version() string {
	return fmt.Sprintf("%d.%d.%d", uint8(op.version>>16), uint8(op.version>>8), uint8(op.version))
}

// VersionNumber -
func (op *IOConnect) VersionNumber() int32 {
	return op.version
}

// Close -
func (op *IOConnect) Close() (err error) {
	if op.conn != nil {
		op.io.Clear()
		op.io = nil
		err = op.conn.Close()
		op.conn = nil
	}
	return err
}

// Reconnect - 重新连接
func (op *IOConnect) Reconnect() error {
	_ = op.conn.Close()
	// 使用 net.JoinHostPort 兼容 IPv6
	addr := net.JoinHostPort(op.host, fmt.Sprintf("%d", op.port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	op.conn = conn
	return nil
}

// NewRequest -
func (op *IOConnect) NewRequest(m map[string]interface{}) *Request {
	r := &Request{
		buff:  op.io,
		props: make(map[string]interface{}, propCapacity),
	}
	r.Reset()

	for k, v := range m {
		r.Set(k, v)
	}

	return r
}

// GetEcho -
func (op *IOConnect) GetEcho() (int8, error) {
	e, err := op.io.ReadEcho()
	return e, err
}

// SetCompressModel -
func (op *IOConnect) SetCompressModel(model byte) (err error) {
	//TODO 数据网络传输压缩模型
	_ = op.io.SetCompressModel(model)
	return nil
}

func (op *IOConnect) SkipAll() (err error) {
	return op.io.SkipAll()
}

func (op *IOConnect) Clear() {
	op.io.Clear()
}

func (op *IOConnect) Peek() (byte, error) {
	return op.io.Peek()
}
