package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/pierrec/lz4"
)

// Reader -
type Reader interface {
	read(io *Buffer) error
}

// Writer -
type Writer interface {
	write(io *Buffer) error
}

// ReaderWriter -
type ReaderWriter interface {
	Reader
	Writer
}

// Buffer -
type Buffer struct {
	io     io.ReadWriteCloser
	size   int
	Out    []byte
	In     []byte
	inOff  int
	inEnd  int
	outOff int

	mode byte

	mode_in byte

	//lz 流模型缓存区
	zip_compress_out_buf *bytes.Buffer

	zip_uncompress_in_buf  *bytes.Buffer
	zip_uncompress_out_buf *bytes.Buffer

	//双缓冲的lz4 缓冲区
	pack_buf   [][]byte
	unpack_buf [][]byte

	//数据打包解包计数器
	pack_index   int32
	unpack_index int32

	//解压缩流模型
	zr *lz4.Reader

	inEof bool

	//lz4 head Codes
	lz4_head_code []byte
	// hash 校验核
	hash XXHZero

	//判断是否是心跳头
	isHeartHead bool
}

// NewBuffer -
func NewBuffer(io io.ReadWriteCloser, size int) *Buffer {
	if size > maxSize {
		size = maxSize
	}
	buf := &Buffer{}
	buf.io = io
	buf.size = size
	buf.Out = make([]byte, size)
	buf.outOff = headSize

	buf.mode = ZIP_MODEL_Uncompressed

	buf.lz4_head_code = lz4_head_code
	buf.hash = XXHZero{}

	//压缩缓冲区
	buf.zip_compress_out_buf = new(bytes.Buffer)

	//解压缩缓冲区
	buf.zip_uncompress_in_buf = new(bytes.Buffer)
	buf.zip_uncompress_out_buf = new(bytes.Buffer)
	buf.zr = lz4.NewReader(buf.zip_uncompress_in_buf)

	buf.pack_buf = make([][]byte, 2)
	buf.pack_buf[0] = make([]byte, maxSize)
	buf.pack_buf[1] = make([]byte, maxSize)

	buf.unpack_buf = make([][]byte, 2)
	buf.unpack_buf[0] = make([]byte, maxSize)
	buf.unpack_buf[1] = make([]byte, maxSize)

	return buf
}

func (b *Buffer) SetCompressModel(model byte) error {
	switch model {
	case ZIP_MODEL_Uncompressed, ZIP_MODEL_Frame, ZIP_MODEL_Block:
		b.mode = byte(model)
		return nil
	default:
		return errors.New("CompressModel error !! CompressModel:1 is compress_frame,2 is compress_Block")
	}
}

// Reset -
func (b *Buffer) Reset() {
	b.inOff = 0
	b.inEnd = 0
	b.outOff = headSize
}

// Clear -
func (b *Buffer) Clear() {
	b.inOff = 0
	b.inEnd = 0
	b.outOff = headSize
	b.Out = make([]byte, b.size)
	b.In = make([]byte, b.size)

	b.hash.Reset()
	b.zip_compress_out_buf.Reset()
	b.zip_uncompress_in_buf.Reset()
	b.zip_uncompress_out_buf.Reset()

}

// PutBool -
func (b *Buffer) PutBool(v bool) error {
	if b.outOff >= len(b.Out) {
		err := b.Flush(false)
		if err != nil {
			return err
		}
	}
	var i byte
	if v == true {
		i = 1
	}
	b.Out[b.outOff] = i
	b.outOff++
	return nil
}

// PutInt8 -
func (b *Buffer) PutInt8(v int8) error {
	if b.outOff >= len(b.Out) {
		err := b.Flush(false)
		if err != nil {
			return err
		}
	}
	b.Out[b.outOff] = byte(v)
	b.outOff++
	return nil
}

// PutUint8 -
func (b *Buffer) PutUint8(v uint8) error {
	if b.outOff >= len(b.Out) {
		err := b.Flush(false)
		if err != nil {
			return err
		}
	}
	b.Out[b.outOff] = v
	b.outOff++
	return nil
}

// PutInt16 -
func (b *Buffer) PutInt16(v int16) error {
	i := b.outOff
	if i+2 > len(b.Out) {
		err := b.Flush(false)
		i = b.outOff
		if err != nil {
			return err
		}
	}
	b.Out[i] = byte(v >> 8)
	b.Out[i+1] = byte(v)
	b.outOff += 2
	return nil
}

// PutUint16 -
func (b *Buffer) PutUint16(v uint16) error {
	i := b.outOff
	if i+2 > len(b.Out) {
		err := b.Flush(false)
		i = b.outOff
		if err != nil {
			return err
		}
	}
	b.Out[i] = byte(v >> 8)
	b.Out[i+1] = byte(v)
	b.outOff += 2
	return nil
}

// PutInt32 -
func (b *Buffer) PutInt32(v int32) error {
	i := b.outOff
	if i+4 > len(b.Out) {
		err := b.Flush(false)
		i = b.outOff
		if err != nil {
			return err
		}
	}
	b.Out[i] = byte(v >> 24)
	b.Out[i+1] = byte(v >> 16)
	b.Out[i+2] = byte(v >> 8)
	b.Out[i+3] = byte(v)
	b.outOff += 4
	return nil
}

// PutInt64 -
func (b *Buffer) PutInt64(v int64) error {
	i := b.outOff
	if i+8 > len(b.Out) {
		err := b.Flush(false)
		i = b.outOff
		if err != nil {
			return err
		}
	}
	b.Out[i] = byte(v >> 56)
	b.Out[i+1] = byte(v >> 48)
	b.Out[i+2] = byte(v >> 40)
	b.Out[i+3] = byte(v >> 32)
	b.Out[i+4] = byte(v >> 24)
	b.Out[i+5] = byte(v >> 16)
	b.Out[i+6] = byte(v >> 8)
	b.Out[i+7] = byte(v)
	b.outOff += 8
	return nil
}

// PutFloat32 -
func (b *Buffer) PutFloat32(f float32) error {
	i := b.outOff
	if i+4 > len(b.Out) {
		err := b.Flush(false)
		i = b.outOff
		if err != nil {
			return err
		}
	}
	v := math.Float32bits(f)
	b.Out[i] = byte(v >> 24)
	b.Out[i+1] = byte(v >> 16)
	b.Out[i+2] = byte(v >> 8)
	b.Out[i+3] = byte(v)
	b.outOff += 4
	return nil
}

// PutFloat64 -
func (b *Buffer) PutFloat64(f float64) error {
	i := b.outOff
	if i+8 > len(b.Out) {
		err := b.Flush(false)
		i = b.outOff
		if err != nil {
			return err
		}
	}
	v := math.Float64bits(f)
	b.Out[i] = byte(v >> 56)
	b.Out[i+1] = byte(v >> 48)
	b.Out[i+2] = byte(v >> 40)
	b.Out[i+3] = byte(v >> 32)
	b.Out[i+4] = byte(v >> 24)
	b.Out[i+5] = byte(v >> 16)
	b.Out[i+6] = byte(v >> 8)
	b.Out[i+7] = byte(v)
	b.outOff += 8
	return nil
}

// PutBytes -
func (b *Buffer) PutBytes(buf []byte) error {
	var n int
	src := 0
	for src < len(buf) {
		n = copy(b.Out[b.outOff:], buf[src:])
		src += n
		b.outOff += n
		if b.outOff == len(b.Out) {
			err := b.Flush(false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Flush -
func (b *Buffer) Flush(eof bool) error {
	n := b.outOff - headSize
	if n == 0 && !eof {
		return nil
	}
	e := 0
	if eof {
		e = 1
	}

	if b.mode != ZIP_MODEL_Uncompressed {
		size := 0
		var temp []byte
		tempMode := b.mode

		switch tempMode {
		case ZIP_MODEL_Frame:
			temp, size, tempMode = b.compress_frame(b.Out[headSize:b.outOff])
		case ZIP_MODEL_Block:
			temp, size, tempMode = b.compress_block(b.Out[headSize:b.outOff])
		}

		//计算压缩数据包
		b.Out[0] = byte(e)
		b.Out[1] = tempMode
		//重新设置发送长度
		b.Out[2] = byte(size >> 8)
		b.Out[3] = byte(size)

		err := b.writeFull(b.Out[:headSize])
		if err != nil {
			return err
		}
		err = b.writeFull(temp)
		if err != nil {
			return err
		}
	} else {
		b.Out[0] = byte(e)
		b.Out[1] = 0
		b.Out[2] = byte(n >> 8)
		b.Out[3] = byte(n)
		err := b.writeFull(b.Out[:b.outOff])
		if err != nil {
			return err
		}
	}
	b.outOff = headSize
	return nil
}

func (b *Buffer) compress_block(data []byte) ([]byte, int, byte) {
	b.pack_index++
	buf := b.pack_buf[b.pack_index&1]
	n, err := lz4.CompressBlock(data, buf, nil)
	if err != nil || n == 0 || n >= len(data) {
		return data, len(data), ZIP_MODEL_Uncompressed
	}
	return buf[:n], n, ZIP_MODEL_Block
}

func (b *Buffer) uncompress_block(data []byte) ([]byte, int, error) {
	b.unpack_index++
	buf := b.unpack_buf[b.unpack_index&1]
	n, err := lz4.UncompressBlock(data, buf)
	if err != nil {
		return data, len(data), err
	}
	return buf[:n], n, nil
}

func (b *Buffer) compress_frame(data []byte) ([]byte, int, byte) {
	b.zip_compress_out_buf.Reset()
	b.hash.Reset()

	defer func() {
		b.zip_compress_out_buf.Reset()
		b.hash.Reset()
	}()
	b.unpack_index++
	buf := b.unpack_buf[b.unpack_index&1]

	n, err := lz4.CompressBlock(data, buf, nil)
	if err == nil && n > 0 {
		_, _ = b.hash.Write(data)
		h32 := b.hash.Sum32()
		//写头
		b.zip_compress_out_buf.Write(b.lz4_head_code)
		//写数据长度
		b.zip_compress_out_buf.Write([]byte{byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)})
		//写数据体
		b.zip_compress_out_buf.Write(buf[:n])
		//写 校验核
		b.zip_compress_out_buf.Write([]byte{0, 0, 0, 0, byte(h32), byte(h32 >> 8), byte(h32 >> 16), byte(h32 >> 24)})
		return b.zip_compress_out_buf.Bytes(), b.zip_compress_out_buf.Len(), ZIP_MODEL_Frame
	} else {
		return data, len(data), ZIP_MODEL_Uncompressed
	}
}

func (b *Buffer) uncompress_frame(data []byte) ([]byte, int, error) {
	defer func() {
		b.zip_uncompress_in_buf.Reset()
		b.zip_uncompress_out_buf.Reset()
	}()
	b.zip_uncompress_in_buf.Write(data)
	_, err := io.Copy(b.zip_uncompress_out_buf, b.zr)
	if err != nil {
		return data, len(data), err
	}
	return b.zip_uncompress_out_buf.Bytes(), b.zip_uncompress_out_buf.Len(), nil
}

func (b *Buffer) UseOptionIn() {
	b.mode = b.mode_in
}

// ReadFull -
func (b *Buffer) readFull(data []byte) error {
	ioBufer := b.io
	var err error
	var num int
	off := 0
	for off < len(data) {
		num, err = ioBufer.Read(data[off:])
		if err != nil {
			return err
		}
		off += num
	}
	return nil
}

// WriteFull -
func (b *Buffer) writeFull(data []byte) error {
	ioBuffer := b.io
	var err error
	var num int
	off := 0
	for off < len(data) {
		num, err = ioBuffer.Write(data[off:])
		if err != nil {
			return err
		}
		off += num
	}
	return nil
}

// 直接裸写tcp,心跳回包时用
func (b *Buffer) DirectWrite(data []byte) error {
	_, err := b.io.Write(data)
	return err
}

func (b *Buffer) ReadEcho() (int8, error) {
	echo := []byte{0}
	err := b.readFull(echo)
	if err != nil {
		return 0, err
	}
	return int8(echo[0]), nil
}

func (b *Buffer) SkipAll() (err error) {
	for !b.inEof {
		err = b.Read()
		if err != nil {
			return err
		}
	}
	defer func() {
		b.inOff = 0
		b.inEnd = 0
		b.inEof = true
	}()
	return err
}

func (b *Buffer) Peek() (byte, error) {
	if b.inOff < b.inEnd {
		v := b.In[b.inOff]
		return byte(v), nil
	}
	err := b.Read()
	if err != nil {
		return 0, err
	}
	//  inOff ~~~~
	v := b.In[b.inOff]
	return byte(v), nil
}

func (b *Buffer) PeekN(buf []byte) (bool, error) {
	off := 0
	for off < len(buf) {
		if b.inOff == b.inEnd {
			err := b.Read()
			if !b.isHeartHead || err != nil {
				return b.isHeartHead, err
			}
		}
		n := copy(buf[off:], b.In[b.inOff:])
		off += n
		b.inOff += n
	}
	b.inOff -= off
	return b.isHeartHead, nil
}

// read -
func (b *Buffer) Read() error {
	head := []byte{0, 0, 0, 0}
	err := b.readFull(head)
	if err != nil {
		return err
	}
	//判断是否是心跳探测请求
	b.isHeartHead = false
	if head[0] == 0x10 && head[1] == 0x20 && head[2] == 0x30 && head[3] == 0x40 {
		//重置head
		head[0] = 1  //eof
		head[1] = 0  //uncompress
		head[2] = 0  //len >> 8
		head[3] = 21 //len
		b.isHeartHead = true
	}

	if head[0] == 1 {
		b.inEof = true
	} else {
		b.inEof = false
	}

	n := (int(head[2]) << 8) | int(head[3])
	if len(b.In) < n {
		b.In = make([]byte, n)
	}

	err = b.readFull(b.In[:n])
	if err != nil {
		return err
	}

	//TODO 模式處理是否提供自動適配客戶端 代碼待定
	zip := head[1] & 3 //zip 模型仅提供了前两位进行设置压缩算法
	b.mode_in = zip

	if zip != ZIP_MODEL_Uncompressed {
		switch zip {
		case ZIP_MODEL_Frame:
			b.In, n, err = b.uncompress_frame(b.In[:n])
			if err != nil {
				return err
			}
		case ZIP_MODEL_Block:
			//fixme 新增压缩算法，解决压缩速度问题
			b.In, n, err = b.uncompress_block(b.In[:n])
			if err != nil {
				return err
			}
		}
	}
	b.inOff = 0
	b.inEnd = n
	return nil
}

// GetBytes -
func (b *Buffer) GetBytes(buf []byte) error {
	off := 0
	for off < len(buf) {
		if b.inOff == b.inEnd {
			err := b.Read()
			if err != nil {
				return err
			}
		}
		n := copy(buf[off:], b.In[b.inOff:])
		off += n
		b.inOff += n
	}
	return nil
}

// GetInt8 -
func (b *Buffer) GetInt8() (int8, error) {
	if b.inOff < b.inEnd {
		v := b.In[b.inOff]
		b.inOff++
		return int8(v), nil
	}
	var buf [1]byte
	err := b.GetBytes(buf[0:])
	if err != nil {
		return 0, err
	}
	return int8(buf[0]), nil
}

// GetUint8 -
func (b *Buffer) GetUint8() (uint8, error) {
	if b.inOff < b.inEnd {
		v := b.In[b.inOff]
		b.inOff++
		return v, nil
	}
	var buf [1]byte
	err := b.GetBytes(buf[0:])
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

// GetInt16 -
func (b *Buffer) GetInt16() (int16, error) {
	i := b.inOff
	if i+2 <= b.inEnd {
		b.inOff += 2
		return (int16(b.In[i]) << 8) | int16(b.In[i+1]), nil
	}
	var buf [2]byte
	err := b.GetBytes(buf[0:])
	if err != nil {
		return 0, err
	}
	return (int16(buf[0]) << 8) | int16(buf[1]), nil
}

// GetUint16 -
func (b *Buffer) GetUint16() (uint16, error) {
	i := b.inOff
	if i+2 <= b.inEnd {
		b.inOff += 2
		return (uint16(b.In[i]) << 8) | uint16(b.In[i+1]), nil
	}
	var buf [2]byte
	err := b.GetBytes(buf[0:])
	if err != nil {
		return 0, err
	}
	return (uint16(buf[0]) << 8) | uint16(buf[1]), nil
}

// GetInt32 -
func (b *Buffer) GetInt32() (int32, error) {
	if b.inOff+4 <= b.inEnd {
		a := b.In[b.inOff:]
		b.inOff += 4
		return (int32(a[0]) << 24) | (int32(a[1]) << 16) | (int32(a[2]) << 8) | int32(a[3]), nil
	}
	var a [4]byte
	err := b.GetBytes(a[0:])
	if err != nil {
		return 0, err
	}
	return (int32(a[0]) << 24) | (int32(a[1]) << 16) | (int32(a[2]) << 8) | int32(a[3]), nil
}

// GetUint32 -
func (b *Buffer) GetUint32() (uint32, error) {
	if b.inOff+4 <= b.inEnd {
		a := b.In[b.inOff:]
		b.inOff += 4
		return (uint32(a[0]) << 24) | (uint32(a[1]) << 16) | (uint32(a[2]) << 8) | uint32(a[3]), nil
	}
	var a [4]byte
	err := b.GetBytes(a[0:])
	if err != nil {
		return 0, err
	}
	return (uint32(a[0]) << 24) | (uint32(a[1]) << 16) | (uint32(a[2]) << 8) | uint32(a[3]), nil
}

// GetInt64 -
func (b *Buffer) GetInt64() (int64, error) {
	if b.inOff+8 <= b.inEnd {
		a := b.In[b.inOff:]
		b.inOff += 8
		return (int64(a[0]) << 56) | (int64(a[1]) << 48) | (int64(a[2]) << 40) | (int64(a[3]) << 32) | (int64(a[4]) << 24) | (int64(a[5]) << 16) | (int64(a[6]) << 8) | int64(a[7]), nil
	}
	var a [8]byte
	err := b.GetBytes(a[0:])
	if err != nil {
		return 0, err
	}
	return (int64(a[0]) << 56) | (int64(a[1]) << 48) | (int64(a[2]) << 40) | (int64(a[3]) << 32) | (int64(a[4]) << 24) | (int64(a[5]) << 16) | (int64(a[6]) << 8) | int64(a[7]), nil
}

// GetUint64 -
func (b *Buffer) GetUint64() (uint64, error) {
	if b.inOff+8 <= b.inEnd {
		a := b.In[b.inOff:]
		b.inOff += 8
		return (uint64(a[0]) << 56) | (uint64(a[1]) << 48) | (uint64(a[2]) << 40) | (uint64(a[3]) << 32) | (uint64(a[4]) << 24) | (uint64(a[5]) << 16) | (uint64(a[6]) << 8) | uint64(a[7]), nil
	}
	var a [8]byte
	err := b.GetBytes(a[0:])
	if err != nil {
		return 0, err
	}
	return (uint64(a[0]) << 56) | (uint64(a[1]) << 48) | (uint64(a[2]) << 40) | (uint64(a[3]) << 32) | (uint64(a[4]) << 24) | (uint64(a[5]) << 16) | (uint64(a[6]) << 8) | uint64(a[7]), nil
}

// GetFloat32 -
func (b *Buffer) GetFloat32() (float32, error) {
	if b.inOff+4 <= b.inEnd {
		a := b.In[b.inOff:]
		b.inOff += 4
		return math.Float32frombits((uint32(a[0]) << 24) | (uint32(a[1]) << 16) | (uint32(a[2]) << 8) | uint32(a[3])), nil
	}
	var a [4]byte
	err := b.GetBytes(a[0:])
	if err != nil {
		return 0, err
	}
	return math.Float32frombits((uint32(a[0]) << 24) | (uint32(a[1]) << 16) | (uint32(a[2]) << 8) | uint32(a[3])), nil
}

// GetFloat64 -
func (b *Buffer) GetFloat64() (float64, error) {
	if b.inOff+8 <= b.inEnd {
		a := b.In[b.inOff:]
		b.inOff += 8
		return math.Float64frombits((uint64(a[0]) << 56) | (uint64(a[1]) << 48) | (uint64(a[2]) << 40) | (uint64(a[3]) << 32) | (uint64(a[4]) << 24) | (uint64(a[5]) << 16) | (uint64(a[6]) << 8) | uint64(a[7])), nil
	}
	var a [8]byte
	err := b.GetBytes(a[0:])
	if err != nil {
		return 0, err
	}
	return math.Float64frombits((uint64(a[0]) << 56) | (uint64(a[1]) << 48) | (uint64(a[2]) << 40) | (uint64(a[3]) << 32) | (uint64(a[4]) << 24) | (uint64(a[5]) << 16) | (uint64(a[6]) << 8) | uint64(a[7])), nil
}

//// Msgpack

// encodeContainerLen -
func (b *Buffer) encodeContainerLen(cindex int, l uint32) error {
	var err error
	var tmp [8]byte
	ct := &mpContainerTypes[cindex]
	if ct.bFixMin > 0 && l < ct.fixCutoff {
		err = b.PutUint8(ct.bFixMin | byte(l))
	} else if ct.b8 > 0 && l < 256 {
		err = b.PutUint16((uint16(ct.b8) << 8) | uint16(l))
	} else if l < 65536 {
		tmp[0] = ct.b16
		tmp[1] = byte(l >> 8)
		tmp[2] = byte(l)
		err = b.PutBytes(tmp[:3])
	} else {
		tmp[0] = ct.b32
		tmp[1] = byte(l >> 24)
		tmp[2] = byte(l >> 16)
		tmp[3] = byte(l >> 8)
		tmp[4] = byte(l)
		err = b.PutBytes(tmp[:5])
	}
	return err
}

// EncodeNil -
func (b *Buffer) EncodeNil() error {
	return b.PutUint8(mpNil)
}

// EncodeBool -
func (b *Buffer) EncodeBool(v bool) error {
	var i uint8 = mpFalse
	if v == true {
		i = mpTrue
	}
	return b.PutUint8(i)
}

// EncodeInt8 - [-128, 127]
func (b *Buffer) EncodeInt8(i int8) error {
	var err error
	if i >= -32 {
		err = b.PutInt8(i)
	} else {
		err = b.PutUint16((uint16(mpInt8) << 8) | uint16(byte(i)))
	}
	return err
}

// EncodeUint8 - [0, 255]
func (b *Buffer) EncodeUint8(i uint8) error {
	var err error
	if i <= 127 {
		err = b.PutInt8(int8(i))
	} else {
		err = b.PutUint16((uint16(mpInt8) << 8) | uint16(i))
	}
	return err
}

// EncodeInt16 - [-32768, 32767]
func (b *Buffer) EncodeInt16(i int16) error {
	var err error
	var tmp [4]byte
	if i >= -32 {
		if i <= 127 { // [-32, 127]
			err = b.PutInt8(int8(i))
		} else if i <= 255 { // [128, 255]
			err = b.PutUint16((uint16(mpUint8) << 8) | uint16(i))
		} else {
			tmp[0] = mpUint16
			tmp[1] = byte(i >> 8)
			tmp[2] = byte(i)
			err = b.PutBytes(tmp[:3])
		}
	} else if i >= -128 { // [-128, -31]
		err = b.PutUint16((uint16(mpInt8) << 8) | uint16(byte(i)))
	} else {
		tmp[0] = mpInt16
		tmp[1] = byte(i >> 8)
		tmp[2] = byte(i)
		err = b.PutBytes(tmp[:3])
	}
	return err
}

// EncodeUint16 - [0, 65535]
func (b *Buffer) EncodeUint16(i uint16) error {
	var err error
	var tmp [4]byte
	switch {
	case i <= 127:
		err = b.PutInt8(int8(i))
	case i <= 255:
		err = b.PutUint16((uint16(mpUint8) << 8) | uint16(i))
	default:
		tmp[0] = mpUint16
		tmp[1] = byte(i >> 8)
		tmp[2] = byte(i)
		err = b.PutBytes(tmp[:3])
	}
	return err
}

// EncodeInt32 -
func (b *Buffer) EncodeInt32(i int32) error {
	var err error
	var tmp [8]byte
	tmp[0] = mpInt32
	tmp[1] = byte(i >> 24)
	tmp[2] = byte(i >> 16)
	tmp[3] = byte(i >> 8)
	tmp[4] = byte(i)
	err = b.PutBytes(tmp[:5])
	return err
}

// EncodeUint32 -
func (b *Buffer) EncodeUint32(i uint32) error {
	var err error
	var tmp [8]byte
	switch {
	case i <= 127:
		err = b.PutInt8(int8(i))
	case i <= 255:
		err = b.PutUint16((uint16(mpUint8) << 8) | uint16(i))
	case i <= 65535:
		tmp[0] = mpUint16
		tmp[1] = byte(i >> 8)
		tmp[2] = byte(i)
		err = b.PutBytes(tmp[:3])
	default:
		tmp[0] = mpUint32
		tmp[1] = byte(i >> 24)
		tmp[2] = byte(i >> 16)
		tmp[3] = byte(i >> 8)
		tmp[4] = byte(i)
		err = b.PutBytes(tmp[:5])
	}
	return err
}

// EncodeInt -
func (b *Buffer) EncodeInt64(i int64) error {
	var err error
	err = b.PutUint8(mpInt64)
	if err == nil {
		err = b.PutInt64(i)
	}
	return err
}

// EncodeInt -
func (b *Buffer) EncodeInt(i int64) error {
	var err error
	var tmp [8]byte
	if i >= -32 {
		if i <= 127 {
			err = b.PutInt8(int8(i))
		} else if i <= 255 {
			err = b.PutUint16((uint16(mpUint8) << 8) | uint16(i))
		} else if i <= 65535 {
			tmp[0] = mpUint16
			tmp[1] = byte(i >> 8)
			tmp[2] = byte(i)
			err = b.PutBytes(tmp[:3])
		} else if i <= 4294967295 {
			tmp[0] = mpUint32
			tmp[1] = byte(i >> 24)
			tmp[2] = byte(i >> 16)
			tmp[3] = byte(i >> 8)
			tmp[4] = byte(i)
			err = b.PutBytes(tmp[:5])
		} else {
			err = b.PutUint8(mpUint64)
			if err == nil {
				err = b.PutInt64(i)
			}
		}
	} else if i >= -128 {
		err = b.PutUint16((uint16(mpInt8) << 8) | uint16(byte(i)))
	} else if i >= -32768 {
		tmp[0] = mpInt16
		tmp[1] = byte(i >> 8)
		tmp[2] = byte(i)
		err = b.PutBytes(tmp[:3])
	} else if i >= -2147483648 {
		tmp[0] = mpInt32
		tmp[1] = byte(i >> 24)
		tmp[2] = byte(i >> 16)
		tmp[3] = byte(i >> 8)
		tmp[4] = byte(i)
		err = b.PutBytes(tmp[:5])
	} else {
		err = b.PutUint8(mpInt64)
		if err == nil {
			err = b.PutInt64(i)
		}
	}
	return err
}

// EncodeUint64 -
func (b *Buffer) EncodeUint64(i uint64) error {
	var err error
	var tmp [8]byte
	switch {
	case i <= 127:
		err = b.PutInt8(int8(i))
	case i <= 255:
		err = b.PutUint16((uint16(mpUint8) << 8) | uint16(i))
	case i <= 65535:
		tmp[0] = mpUint16
		tmp[1] = byte(i >> 8)
		tmp[2] = byte(i)
		err = b.PutBytes(tmp[:3])
	case i <= 4294967295:
		tmp[0] = mpUint32
		tmp[1] = byte(i >> 24)
		tmp[2] = byte(i >> 16)
		tmp[3] = byte(i >> 8)
		tmp[4] = byte(i)
		err = b.PutBytes(tmp[:5])
	default:
		err = b.PutUint8(mpUint64)
		if err == nil {
			err = b.PutInt64(int64(i))
		}
	}
	return err
}

// EncodeFloat32 -
func (b *Buffer) EncodeFloat32(f float32) error {
	err := b.PutUint8(mpFloat)
	if err == nil {
		_ = b.PutFloat32(f)
	}
	return err
}

// EncodeFloat64 -
func (b *Buffer) EncodeFloat64(d float64) error {
	err := b.PutUint8(mpDouble)
	if err == nil {
		err = b.PutFloat64(d)
	}
	return err
}

// EncodeString -
func (b *Buffer) EncodeString(s string) error {
	err := b.encodeContainerLen(ctString, uint32(len(s)))
	if err == nil {
		err = b.PutBytes([]byte(s))
	}
	return err
}

// EncodeBytes -
func (b *Buffer) EncodeBytes(blob []byte) error {
	err := b.encodeContainerLen(ctBinary, uint32(len(blob)))
	if err == nil {
		err = b.PutBytes(blob)
	}
	return err
}

// DecodeUint8 -
func (b *Buffer) DecodeUint8() (value uint8, err error) {
	typ, err := b.GetUint8()
	if err != nil {
		return value, err
	}
	switch typ {
	case mpUint8, mpInt8:
		value, err = b.GetUint8()
	default:
		if typ <= 127 {
			value = typ
		} else {
			return value, errors.New(fmt.Sprintf(" DecodeUint8 error type=%d", typ))
		}
	}
	return value, err
}

// DecodeInt32 -
func (b *Buffer) DecodeInt32() (value int32, err error) {
	typ, err := b.GetUint8()
	if err != nil {
		return value, err
	}
	switch typ {
	case mpInt32:
		value, err = b.GetInt32()
	default:
		err = errors.New(fmt.Sprintf("DecodeInt error type=%d", typ))
	}
	return value, err
}

// DecodeInt64 -
func (b *Buffer) DecodeInt64() (value int64, err error) {
	typ, err := b.GetUint8()
	if err != nil {
		return value, err
	}
	switch typ {
	case mpInt64:
		value, err = b.GetInt64()
	default:
		err = errors.New(fmt.Sprintf("DecodeInt error type=%d", typ))
	}
	return value, err
}

// DecodeInt -
func (b *Buffer) DecodeInt() (int64, error) {
	typ, err := b.GetUint8()
	if err != nil {
		return 0, err
	}
	var i int64
	switch typ {
	case mpUint8:
		v, e := b.GetUint8()
		i = int64(v)
		err = e
	case mpUint16:
		v, e := b.GetUint16()
		i = int64(v)
		err = e
	case mpUint32:
		v, e := b.GetUint32()
		i = int64(v)
		err = e
	case mpUint64:
		v, e := b.GetInt64()
		i = int64(v)
		err = e
	case mpInt8:
		v, e := b.GetInt8()
		i = int64(v)
		err = e
	case mpInt16:
		v, e := b.GetInt16()
		i = int64(v)
		err = e
	case mpInt32:
		v, e := b.GetInt32()
		i = int64(v)
		err = e
	case mpInt64:
		v, e := b.GetInt64()
		i = v
		err = e
	default:
		if typ <= 127 || typ >= mpNegFixNumMin { // -32 ~ 127
			i = int64(int8(typ))
		} else {
			s := fmt.Sprintf("DecodeInt error type=%d", typ)
			err = errors.New(s)
		}
	}
	return i, err
}

// DecodeFloat64 -
func (b *Buffer) DecodeFloat64() (float64, error) {
	typ, err := b.GetUint8()
	if err != nil {
		return 0, err
	}
	var v float64
	switch typ {
	case mpFloat:
		f, e := b.GetFloat32()
		v = float64(f)
		err = e
	case mpDouble:
		v, err = b.GetFloat64()
	default:
		s := fmt.Sprintf("DecodeFloat error type=%d", typ)
		err = errors.New(s)
	}
	return v, err
}

// DecodeString -
func (b *Buffer) DecodeString() (string, error) {
	typ, err := b.GetUint8()
	if err != nil {
		return "", err
	}
	size := 0
	switch typ {
	case mpStr8:
		n, e := b.GetUint8()
		size = int(n)
		err = e
	case mpStr16:
		n, e := b.GetUint16()
		size = int(n)
		err = e
	case mpStr32:
		n, e := b.GetUint32()
		size = int(n)
		err = e
	case mpNil:
		size = 0
	default:
		if (typ & 0xe0) == mpFixStrMin {
			size = int(typ & 31)
		} else {
			s := fmt.Sprintf("DecodeString error type=%d", typ)
			err = errors.New(s)
		}
	}
	if err == nil && size > 0 {
		buf := make([]byte, size)
		err = b.GetBytes(buf)
		if err == nil {
			return string(buf), nil
		}
	}
	return "", err
}

// DecodeBytes -
func (b *Buffer) DecodeBytes() ([]byte, error) {
	typ, err := b.GetUint8()
	if err != nil {
		return nil, err
	}
	size := 0
	switch typ {
	case mpBin8:
		n, e := b.GetUint8()
		size = int(n)
		err = e
	case mpBin16:
		n, e := b.GetUint16()
		size = int(n)
		err = e
	case mpBin32:
		n, e := b.GetUint32()
		size = int(n)
		err = e
	case mpFixExt1, mpFixExt2, mpFixExt4, mpFixExt8, mpFixExt16, mpExt8, mpExt16, mpExt32:
		size, err := b.decodeExtendLen(typ)
		if err == nil {
			_, err = b.GetUint8()
		}
		if err == nil && size > 0 {
			blob := make([]byte, size)
			err = b.GetBytes(blob)
			return blob, err
		}
	case mpNil:
		size = 0
	default:
		s := fmt.Sprintf("DecodeBytes error type=%d", typ)
		err = errors.New(s)
	}
	if err == nil && size > 0 {
		buf := make([]byte, size)
		err = b.GetBytes(buf)
		if err == nil {
			return buf, nil
		}
	}
	return nil, err
}

// EncodeExtendLen -
func (b *Buffer) EncodeExtendLen(n uint32, xtag uint8) error {
	var err error
	var tmp [8]byte
	switch {
	case n < 256:
		tmp[0] = mpExt8
		tmp[1] = byte(n)
		tmp[2] = xtag
		err = b.PutBytes(tmp[:3])
	case n < 65536:
		tmp[0] = mpExt16
		tmp[1] = byte(n >> 8)
		tmp[2] = byte(n)
		tmp[3] = xtag
		err = b.PutBytes(tmp[:4])
	default:
		tmp[0] = mpExt32
		tmp[1] = byte(n >> 24)
		tmp[2] = byte(n >> 16)
		tmp[3] = byte(n >> 8)
		tmp[4] = byte(n)
		tmp[5] = xtag
		err = b.PutBytes(tmp[:6])
	}
	return err
}

// decodeExtendLen -
func (b *Buffer) decodeExtendLen(typ uint8) (uint32, error) {
	var err error
	var tmp [4]byte
	var size uint32
	switch typ {
	case mpFixExt1:
		size = 1
	case mpFixExt2:
		size = 2
	case mpFixExt4:
		size = 4
	case mpFixExt8:
		size = 8
	case mpFixExt16:
		size = 16
	case mpExt8:
		err = b.GetBytes(tmp[:1])
		size = uint32(tmp[0])
	case mpExt16:
		err = b.GetBytes(tmp[:2])
		size = (uint32(tmp[0]) << 8) + uint32(tmp[1])
	case mpExt32:
		err = b.GetBytes(tmp[:4])
		size = (uint32(tmp[0]) << 24) + (uint32(tmp[1]) << 16) + (uint32(tmp[2]) << 8) + uint32(tmp[3])
	default:
		s := fmt.Sprintf("DecodeExtend error type=%d", typ)
		err = errors.New(s)
	}
	return size, err
}

// DecodeExtendStart -
func (b *Buffer) DecodeExtendStart() (uint32, uint8, error) {
	var size uint32
	var xtag uint8
	typ, err := b.GetUint8()
	if err != nil {
		return 0, 0, err
	}
	size, err = b.decodeExtendLen(typ)
	if err == nil {
		xtag, err = b.GetUint8()
	}
	return size, xtag, err
}

// DecodeExtend -
func (b *Buffer) DecodeExtend() ([]byte, uint8, error) {
	var size uint32
	var xtag uint8
	var blob []byte
	typ, err := b.GetUint8()
	if err != nil {
		return nil, 0, err
	}
	size, err = b.decodeExtendLen(typ)
	if err == nil {
		xtag, err = b.GetUint8()
	}
	if err == nil && size > 0 {
		blob = make([]byte, size)
		err = b.GetBytes(blob)
	}
	return blob, xtag, err
}

// EncodeMapStart -
func (b *Buffer) EncodeMapStart(n uint32) error {
	return b.encodeContainerLen(ctMap, n)
}

// EncodeArrayStart -
func (b *Buffer) EncodeArrayStart(n uint32) error {
	return b.encodeContainerLen(ctArray, n)
}

// DecodeMapStart -
func (b *Buffer) DecodeMapStart() (uint32, error) {
	typ, err := b.GetUint8()
	if err != nil {
		return 0, err
	}
	var u16 uint16
	var size uint32
	switch typ {
	case mpMap16:
		u16, err = b.GetUint16()
		size = uint32(u16)
	case mpMap32:
		size, err = b.GetUint32()
	case mpNil:
		size = 0xffffffff
	default:
		if (typ & 0xf0) == mpFixMapMin {
			size = uint32(typ & 15)
		} else {
			s := fmt.Sprintf("decodeMapStart error type=%d", typ)
			err = errors.New(s)
		}
	}
	return size, err
}

// DecodeArrayStart -
func (b *Buffer) DecodeArrayStart() (uint32, error) {
	typ, err := b.GetUint8()
	if err != nil {
		return 0, err
	}
	var u16 uint16
	var size uint32
	switch typ {
	case mpArray16:
		u16, err = b.GetUint16()
		size = uint32(u16)
	case mpArray32:
		size, err = b.GetUint32()
	case mpNil:
		size = 0xffffffff
	default:
		if (typ & 0xf0) == mpFixArrayMin {
			size = uint32(typ & 15)
		} else {
			s := fmt.Sprintf("decodeArrayStart error type=%d", typ)
			err = errors.New(s)
		}
	}
	return size, err
}

// EncodeValue -
func (b *Buffer) EncodeValue(value interface{}) error {
	var err error
	switch v := value.(type) {
	case nil:
		err = b.EncodeNil()
	case bool:
		err = b.EncodeBool(v)
	case int8:
		err = b.EncodeInt8(v)
	case uint8:
		err = b.EncodeUint8(v)
	case int16:
		err = b.EncodeInt16(v)
	case uint16:
		err = b.EncodeUint16(v)
	case int32:
		err = b.EncodeInt32(v)
	case uint32:
		err = b.EncodeUint32(v)
	case int64:
		err = b.EncodeInt64(v)
	case uint64:
		err = b.EncodeUint64(v)
	case int:
		err = b.EncodeInt64(int64(v))
	case uint:
		err = b.EncodeUint64(uint64(v))
	case float32:
		err = b.EncodeFloat32(v)
	case float64:
		err = b.EncodeFloat64(v)
	case string:
		err = b.EncodeString(v)
	case []byte:
		err = b.EncodeBytes(v)
	case Writer:
		err = v.write(b)
	default:
		s := fmt.Sprintf("EncodeValue error type %d", reflect.TypeOf(v))
		err = errors.New(s)
	}
	return err
}

// DecodeValue -
func (b *Buffer) DecodeValue() (interface{}, error) {
	var v interface{}
	var u8 uint8
	var u16 uint16
	var u32 uint32
	typ, err := b.GetUint8()
	if err != nil {
		return nil, err
	}
	switch typ {
	case mpNil, mpNotUsed:
		v = nil
	case mpFalse:
		v = false
	case mpTrue:
		v = true
	case mpUint8:
		v, err = b.GetUint8()
	case mpUint16:
		v, err = b.GetUint16()
	case mpUint32:
		v, err = b.GetUint32()
	case mpUint64:
		v, err = b.GetUint64()
	case mpInt8:
		v, err = b.GetInt8()
	case mpInt16:
		v, err = b.GetInt16()
	case mpInt32:
		v, err = b.GetInt32()
	case mpInt64:
		v, err = b.GetInt64()
	case mpFloat:
		v, err = b.GetFloat32()
	case mpDouble:
		v, err = b.GetFloat64()
	case mpStr8:
		u8, err = b.GetUint8()
		if err == nil && u8 > 0 {
			blob := make([]byte, u8)
			err = b.GetBytes(blob)
			v = string(blob)
		}
	case mpStr16:
		u16, err = b.GetUint16()
		if err == nil && u16 > 0 {
			blob := make([]byte, u16)
			err = b.GetBytes(blob)
			v = string(blob)
		}
	case mpStr32:
		u32, err = b.GetUint32()
		if err == nil && u32 > 0 {
			blob := make([]byte, u32)
			err = b.GetBytes(blob)
			v = string(blob)
		}
	case mpBin8:
		u8, err = b.GetUint8()
		if err == nil && u8 > 0 {
			blob := make([]byte, u8)
			err = b.GetBytes(blob)
			v = blob
		}
	case mpBin16:
		u16, err = b.GetUint16()
		if err == nil && u16 > 0 {
			blob := make([]byte, u16)
			err = b.GetBytes(blob)
			v = blob
		}
	case mpBin32:
		u32, err = b.GetUint32()
		if err == nil && u32 > 0 {
			blob := make([]byte, u32)
			err = b.GetBytes(blob)
			v = blob
		}
	case mpArray16:
		u16, err = b.GetUint16()
		if err == nil && u16 > 0 {
			v = make([]interface{}, u16)
		}
	case mpArray32:
		u32, err = b.GetUint32()
		if err == nil && u32 > 0 {
			v = make([]interface{}, u32)
		}
	case mpMap16:
		u16, err = b.GetUint16()
		if err == nil && u16 > 0 {
			v = make(map[string]interface{}, u16)
		}
	case mpMap32:
		u32, err = b.GetUint32()
		if err == nil && u32 > 0 {
			v = make(map[string]interface{}, u32)
		}
	case mpFixExt1, mpFixExt2, mpFixExt4, mpFixExt8, mpFixExt16, mpExt8, mpExt16, mpExt32:
		u32, err = b.decodeExtendLen(typ)
		if err == nil {
			_, err = b.GetUint8()
		}
		if err == nil && u32 > 0 {
			blob := make([]byte, u32)
			err = b.GetBytes(blob)
			v = blob
		}
	default:
		switch typ & 0xf0 {
		case mpFixMapMin: // 0x80
			v = make(map[string]interface{}, int(typ&15))
		case mpFixArrayMin: // 0x90
			v = make([]interface{}, int(typ&15))
		case mpFixStrMin, 0xb0: // 0xa0, 0xb0
			blob := make([]byte, int(typ&31))
			err = b.GetBytes(blob)
			if err == nil {
				v = string(blob)
			}
		default: // 0xc0-0xff, 0x0-0x7f
			v = int8(typ)
		}
	}
	return v, err
}
