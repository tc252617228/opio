package opio

import (
	"fmt"
	"time"

	"github.com/tc252617228/opio/internal/utils"
)

// Value -
type Value struct {
	RT int8
	ID int32
	TM int32
	DS int16
	AV float64
}

// Archive -
type Archive struct {
	ID    int32
	Type  int8
	Data  []Value
	Error int32
}

// Stat -
type Stat struct {
	ID    int32
	Type  int8
	Data  []StatVal
	Error int32
}

// StatVal -
type StatVal struct {
	Time    int32
	Status  int16
	Flow    float64 // 累积
	Max     float64 // 最大，DX: 跳变为1的次数
	Min     float64 // 最小，DX: 跳变为0的次数
	MaxTime int32   // 最大值时间，DX: 值为1持续时间
	MinTime int32   // 最小值时间，DX: 值为0持续时间
	Avg     float64 // 时间平均
	Mean    float64
	Sum     float64
}

// ArchiveQuery -
type ArchiveQuery struct {
	op       *IOConnect
	io       *utils.Buffer
	ids      []int32
	mode     int32
	begin    time.Time
	end      time.Time
	interval int32
}

// Mode
// noinspection GoUnusedConst
const (
	ModeRaw      int32 = 0  ///< 取原始值
	ModeSpan     int32 = 1  ///< 取等间距值
	ModePlot     int32 = 2  ///< 取PLOT值，每区间包括（起始值，最大值，最小值, 结束）
	ModeArch     int32 = 4  ///< 取归档值
	ModeFlow     int32 = 8  ///< 取累计值
	ModeMax      int32 = 9  ///< 取最大值
	ModeMin      int32 = 10 ///< 取最小值
	ModeAvg      int32 = 11 ///< 取平均值
	ModeMean     int32 = 12 ///< 取算术平均值
	ModeSum      int32 = 14 ///< 取原始值的累加和，
	ModeStat     int32 = 15 ///< 取所有统计值：累计/最大/最小/平均/方差
	ModeStatMask int32 = 8  ///< 取累计值
)

// read -
func (value *Value) Read(io *utils.Buffer) (err error) {
	value.RT, _ = io.GetInt8()
	if value.RT == -1 {
		_, err = io.GetInt32() // 无效值标记
	} else {
		value.TM, _ = io.GetInt32()
		value.DS, _ = io.GetInt16()
		switch value.RT & 15 {
		case TypeAX:
			f, e := io.GetFloat32()
			value.AV = float64(f)
			err = e
		case TypeDX:
			i, e := io.GetInt8()
			value.AV = float64(i)
			err = e
		case TypeI2:
			i, e := io.GetInt16()
			value.AV = float64(i)
			err = e
		case TypeI4:
			i, e := io.GetInt32()
			value.AV = float64(i)
			err = e
		case TypeR8:
			value.AV, err = io.GetFloat64()
		}
	}
	return err
}

// ReadAX -
func (value *Value) ReadAX(io *utils.Buffer) error {
	value.RT = TypeAX
	value.TM, _ = io.GetInt32()
	value.DS, _ = io.GetInt16()
	f, err := io.GetFloat32()
	value.AV = float64(f)
	return err
}

// ReadDX -
func (value *Value) ReadDX(io *utils.Buffer) error {
	value.RT = TypeDX
	value.TM, _ = io.GetInt32()
	value.DS, _ = io.GetInt16()
	i, err := io.GetInt8()
	value.AV = float64(i)
	return err
}

// ReadI2 -
func (value *Value) ReadI2(io *utils.Buffer) error {
	value.RT = TypeI2
	value.TM, _ = io.GetInt32()
	value.DS, _ = io.GetInt16()
	i, err := io.GetInt16()
	value.AV = float64(i)
	return err
}

// ReadI4 -
func (value *Value) ReadI4(io *utils.Buffer) error {
	value.RT = TypeI4
	value.TM, _ = io.GetInt32()
	value.DS, _ = io.GetInt16()
	i, err := io.GetInt32()
	value.AV = float64(i)
	return err
}

// ReadR8 -
func (value *Value) ReadR8(io *utils.Buffer) (err error) {
	value.RT = TypeR8
	value.TM, _ = io.GetInt32()
	value.DS, _ = io.GetInt16()
	value.AV, err = io.GetFloat64()
	return err
}

// write -
func (value *Value) Write(io *utils.Buffer) error {
	_ = io.PutInt32(value.ID)
	_ = io.PutInt32(value.TM)
	_ = io.PutInt16(value.DS)
	err := io.PutFloat64(value.AV)
	return err
}

// WriteAX -
func (value *Value) WriteAX(io *utils.Buffer) error {
	_ = io.PutInt32(value.TM)
	_ = io.PutInt16(value.DS)
	err := io.PutFloat32(float32(value.AV))
	return err
}

// WriteDX -
func (value *Value) WriteDX(io *utils.Buffer) error {
	_ = io.PutInt32(value.TM)
	_ = io.PutInt16(value.DS)
	err := io.PutInt8(int8(value.AV))
	return err
}

// WriteI2 -
func (value *Value) WriteI2(io *utils.Buffer) error {
	_ = io.PutInt32(value.TM)
	_ = io.PutInt16(value.DS)
	err := io.PutInt16(int16(value.AV))
	return err
}

// WriteI4 -
func (value *Value) WriteI4(io *utils.Buffer) error {
	_ = io.PutInt32(value.TM)
	_ = io.PutInt16(value.DS)
	err := io.PutInt32(int32(value.AV))
	return err
}

// WriteR8 -
func (value *Value) WriteR8(io *utils.Buffer) error {
	_ = io.PutInt32(value.TM)
	_ = io.PutInt16(value.DS)
	err := io.PutFloat64(value.AV)
	return err
}

// read -
func (a *Archive) Read(io *utils.Buffer) (err error) {
	var count int32
	a.Type, _ = io.GetInt8()
	if a.Type < 0 {
		a.Error, err = io.GetInt32()
	} else {
		count, err = io.GetInt32()
		if err != nil {
			return err
		}
		v := make([]Value, count)
		switch a.Type & 15 {
		case TypeAX:
			for i := int32(0); i < count && err == nil; i++ {
				err = v[i].ReadAX(io)
			}
		case TypeDX:
			for i := int32(0); i < count && err == nil; i++ {
				err = v[i].ReadDX(io)
			}
		case TypeI2:
			for i := int32(0); i < count && err == nil; i++ {
				err = v[i].ReadI2(io)
			}
		case TypeI4:
			for i := int32(0); i < count && err == nil; i++ {
				err = v[i].ReadI4(io)
			}
		case TypeR8:
			for i := int32(0); i < count && err == nil; i++ {
				err = v[i].ReadR8(io)
			}
		default:
			err = fmt.Errorf("读取归档数据时无效的类型 %d", a.Type)
		}
		a.Data = v
	}
	return err
}

// read -
func (v *StatVal) Read(io *utils.Buffer, mode int32) (err error) {
	switch mode {
	case ModeFlow:
		v.Time, _ = io.GetInt32()
		v.Status, _ = io.GetInt16()
		v.Flow, err = io.GetFloat64()
	case ModeMax:
		v.Time, _ = io.GetInt32()
		v.Status, _ = io.GetInt16()
		v.Max, err = io.GetFloat64()
	case ModeMin:
		v.Time, _ = io.GetInt32()
		v.Status, _ = io.GetInt16()
		v.Min, err = io.GetFloat64()
	case ModeAvg:
		v.Time, _ = io.GetInt32()
		v.Status, _ = io.GetInt16()
		v.Avg, err = io.GetFloat64()
	case ModeMean:
		v.Time, _ = io.GetInt32()
		v.Status, _ = io.GetInt16()
		v.Mean, err = io.GetFloat64()
	case ModeSum:
		v.Time, _ = io.GetInt32()
		v.Status, _ = io.GetInt16()
		v.Sum, err = io.GetFloat64()
	case ModeStat:
		b := make([]byte, 62)
		err = io.GetBytes(b)
		if err == nil {
			v.Time = utils.GetInt32(b)
			v.Status = utils.GetInt16(b[4:])
			v.Flow = utils.GetFloat64(b[6:])
			v.Max = utils.GetFloat64(b[14:])
			v.Min = utils.GetFloat64(b[22:])
			v.MaxTime = utils.GetInt32(b[30:])
			v.MinTime = utils.GetInt32(b[34:])
			v.Avg = utils.GetFloat64(b[38:])
			v.Mean = utils.GetFloat64(b[46:])
		}
	}
	return err
}

// read -
func (a *Stat) Read(io *utils.Buffer, mode int32) (err error) {
	var count int32
	a.Type, _ = io.GetInt8()
	if a.Type < 0 {
		a.Error, err = io.GetInt32()
	} else {
		count, err = io.GetInt32()
		if err != nil {
			return err
		}
		v := make([]StatVal, count)
		for i := int32(0); i < count && err == nil; i++ {
			err = v[i].Read(io, mode)
		}
		a.Data = v
	}
	return err
}

// write -
func (a *Archive) Write(io *utils.Buffer) (err error) {
	count := len(a.Data)
	_ = io.PutInt32(a.ID)
	_ = io.PutInt8(a.Type)
	_ = io.PutInt32(int32(count))
	switch a.Type {
	case TypeAX:
		for i := 0; i < count && err == nil; i++ {
			err = a.Data[i].WriteAX(io)
		}
	case TypeDX:
		for i := 0; i < count && err == nil; i++ {
			err = a.Data[i].WriteDX(io)
		}
	case TypeI2:
		for i := 0; i < count && err == nil; i++ {
			err = a.Data[i].WriteI2(io)
		}
	case TypeI4:
		for i := 0; i < count && err == nil; i++ {
			err = a.Data[i].WriteI4(io)
		}
	case TypeR8:
		for i := 0; i < count && err == nil; i++ {
			err = a.Data[i].WriteR8(io)
		}
	}
	return err
}

// ReadRealtime -
func (op *IOConnect) ReadRealtime(v []Value) (err error) {
	io := op.io
	count := len(v)
	_ = io.PutInt32(MAGIC)
	_ = io.PutInt32(cmdSelect)
	_ = io.PutInt32(urlDynamic)
	_ = io.PutInt16(0)
	_ = io.PutInt16(0)
	_ = io.PutInt32(int32(count))
	for i := 0; i < count && err == nil; i++ {
		err = io.PutInt32(v[i].ID)
	}
	if err == nil {
		_ = io.PutInt32(MAGIC)
		err = io.Flush(true)
	}
	if err != nil {
		fmt.Println(err)
		return err
	}
	var magic int32
	magic, err = io.GetInt32()
	if magic != MAGIC || err != nil {
		if err == nil {
			err = fmt.Errorf("读取实时数据错误，magic=%d", magic)
		}
		return err
	}
	_, _ = io.GetInt32()     // 标志
	size, _ := io.GetInt32() // 数量
	if size != int32(count) {
		err = fmt.Errorf("读取实时数据错误，数量=%d，期望=%d", size, count)
	}
	for i := 0; i < count && err == nil; i++ {
		err = v[i].Read(io)
	}
	magic, err = io.GetInt32()
	if magic != MAGIC && err == nil {
		err = fmt.Errorf("读取实时数据错误，magic=%d", magic)
	}
	return err
}

// WriteRealtime -
func (op *IOConnect) WriteRealtime(v []Value) (err error) {
	io := op.io
	count := len(v)
	_ = io.PutInt32(MAGIC)
	_ = io.PutInt32(cmdInsert)
	_ = io.PutInt32(urlDynamic)
	_ = io.PutInt16(0)
	_ = io.PutInt16(flagWall) // 0
	_ = io.PutInt32(int32(count))
	_ = io.PutInt8(TypeR8)
	for i := 0; i < count && err == nil; i++ {
		err = v[i].Write(io)
	}
	if err == nil {
		_ = io.PutInt32(MAGIC)
		err = io.Flush(true)
	}
	if err != nil {
		fmt.Println(err)
		return err
	}
	var echo int8
	echo, err = io.ReadEcho()
	if echo != 0 {
		err = fmt.Errorf("写入实时数据错误 %d", int32(echo))
	}
	return err
}

// WriteArchive -
func (op *IOConnect) WriteArchive(v []*Archive, cache bool) (err error) {
	io := op.io
	count := len(v)
	flag := flagWall
	if cache {
		flag |= flagCache
	}
	_ = io.PutInt32(MAGIC)
	_ = io.PutInt32(cmdInsert)
	_ = io.PutInt32(urlArchive)
	_ = io.PutInt16(0)
	_ = io.PutInt16(flag)
	_ = io.PutInt32(int32(count))
	for i := 0; i < count && err == nil; i++ {
		err = v[i].Write(io)
	}
	if err == nil {
		_ = io.PutInt32(MAGIC)
		err = io.Flush(true)
	}
	if err != nil {
		fmt.Println(err)
		return err
	}
	var echo int8
	echo, err = io.ReadEcho()
	if echo != 0 {
		err = fmt.Errorf("写入归档数据错误 %d", int32(echo))
	}
	return err
}

// NewArchiveQuery -
func NewArchiveQuery(op *IOConnect, ids []int32, mode int32, begin, end time.Time, interval int32) *ArchiveQuery {
	query := &ArchiveQuery{op, op.io, ids, mode, begin, end, interval}
	return query
}

// Begin -
func (q *ArchiveQuery) Begin() (err error) {
	io := q.io
	ids := q.ids
	mode := q.mode
	beg := q.begin.Unix()
	end := q.end.Unix()
	itv := q.interval
	count := len(q.ids)
	io.Reset()
	_ = io.PutInt32(MAGIC)
	_ = io.PutInt32(cmdSelect)
	_ = io.PutInt32(urlArchive)
	_ = io.PutInt32(0)
	_ = io.PutInt32(int32(count))
	for i := 0; i < count && err == nil; i++ {
		_ = io.PutInt32(ids[i])
		_ = io.PutInt32(mode)
		_ = io.PutInt32(0)
		_ = io.PutInt32(int32(beg))
		_ = io.PutInt32(int32(end))
		err = io.PutInt32(itv)
	}
	if err == nil {
		_ = io.PutInt32(MAGIC)
		err = io.Flush(true)
	}
	if err != nil {
		fmt.Println(err)
		return err
	}

	var magic, flag, rowCount int32
	magic, err = io.GetInt32()
	if err == nil {
		flag, err = io.GetInt32()
	}
	if err == nil {
		rowCount, err = io.GetInt32()
	}
	if magic != MAGIC || rowCount != int32(count) {
		err = fmt.Errorf("开始读取归档数据错误，头部信息 %d,%d,%d", magic, flag, rowCount)
	}
	return err
}

// Next - 读取下一个归档数据块
func (q *ArchiveQuery) Next() (ar *Archive, err error) {
	if q.mode&ModeStatMask != 0 {
		err = fmt.Errorf("ArchiveQuery 当前为统计模式，请使用 NextStat")
		return nil, err
	}

	var index uint32
	var next int8
	io := q.io
	next, err = io.GetInt8()
	if err != nil {
		return nil, err // 检查 GetInt8 的错误
	}
	if next == 1 {
		ar = &Archive{}
		index, err = io.GetUint32() // 读取索引
		if err != nil {             // 首先检查读取索引的错误
			return nil, err
		}
		// 索引读取成功，现在检查索引是否越界
		if index >= uint32(len(q.ids)) {
			err = fmt.Errorf("ArchiveQuery 索引 %d 超出范围", index)
		} else {
			// 索引有效，读取归档数据
			ar.ID = q.ids[index]
			err = ar.Read(io) // 读取归档数据，此处的 err 会被最终返回
		}
	} else {
		magic, e := io.GetInt32() // 读取结束标记
		if e != nil {
			err = e // 如果读取结束标记出错，则返回该错误
		} else if magic != MAGIC {
			// 如果读取成功但结束标记不正确，则构造错误信息
			err = fmt.Errorf("结束读取归档数据错误，尾部标识 %d", magic)
		}
		// 如果 e == nil 且 magic == MAGIC，则 err 保持为 nil，表示正常结束
	}
	return ar, err
}

// NextStat - 读取下一个统计数据块
func (q *ArchiveQuery) NextStat() (st *Stat, err error) {
	if q.mode&ModeStatMask == 0 {
		err = fmt.Errorf("ArchiveQuery 当前非统计模式，请使用 Next")
		return nil, err
	}

	var index uint32
	var next int8
	io := q.io
	next, err = io.GetInt8() // 读取下一个块标识
	if err != nil {
		return nil, err // 首先检查 GetInt8 的错误
	}

	if next == 1 { // 还有数据块
		st = &Stat{}
		index, err = io.GetUint32() // 读取数据块对应的 ID 索引
		if err != nil {
			return nil, err // 检查 GetUint32 的错误
		}

		// 检查索引是否越界
		if index >= uint32(len(q.ids)) {
			err = fmt.Errorf("ArchiveQuery 索引 %d 超出范围", index)
			return nil, err // 索引越界，返回错误
		}

		// 索引有效，读取统计数据
		st.ID = q.ids[index]
		err = st.Read(io, q.mode) // 读取数据，此操作的 err 会在最后返回
	} else { // 没有更多数据块了 (next != 1)
		// 读取结束标记
		magic, e := io.GetInt32()
		if e != nil {
			err = e // 如果读取结束标记出错，则返回该错误
		} else if magic != MAGIC {
			// 如果读取成功但结束标记不正确，则构造错误信息
			err = fmt.Errorf("结束读取归档数据错误，尾部标识 %d", magic)
		}
		// 如果 e == nil 且 magic == MAGIC，则 err 保持为 nil，表示正常结束
	}
	return st, err // 返回读取到的 Stat (如果 next == 1) 和最终的错误状态
}

// ReadArchive -
func (op *IOConnect) ReadArchive(ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Archive, error) {
	var ar *Archive
	query := NewArchiveQuery(op, ids, mode, begin, end, interval)
	result := make([]*Archive, 0, 1024)
	err := query.Begin()
	for err == nil {
		ar, err = query.Next()
		if ar != nil && err == nil {
			fmt.Println(ar.ID, len(ar.Data), err)
			result = append(result, ar)
		}
		if ar == nil {
			break // 数据结束
		}
	}
	return result, nil
}

// ReadStat -
func (op *IOConnect) ReadStat(ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Stat, error) {
	var st *Stat
	query := NewArchiveQuery(op, ids, mode, begin, end, interval)
	result := make([]*Stat, 0, 1024)
	err := query.Begin()
	for err == nil {
		st, err = query.NextStat()
		if st != nil && err == nil {
			fmt.Println(st.ID, st.Data, err)
			result = append(result, st)
		}
		if st == nil {
			break // 数据结束
		}
	}
	return result, nil
}
