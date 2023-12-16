package opio

import (
	"fmt"
	"time"

	"github.com/tc252617228/opio/driver/internal/utils"
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
		_, err = io.GetInt32() // N/A
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
			err = fmt.Errorf("ArchiveRead invalid type %d", a.Type)
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
			err = fmt.Errorf("get realtime error magic=%d", magic)
		}
		return err
	}
	_, _ = io.GetInt32()     // flag
	size, _ := io.GetInt32() // count
	if size != int32(count) {
		err = fmt.Errorf("get realtime error count=%d, expected=%d", size, count)
	}
	for i := 0; i < count && err == nil; i++ {
		err = v[i].Read(io)
	}
	magic, err = io.GetInt32()
	if magic != MAGIC && err == nil {
		err = fmt.Errorf("get realtime error magic=%d", magic)
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
		err = fmt.Errorf("WriteReal error %d", int32(echo))
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
		err = fmt.Errorf("WriteArchive error %d", int32(echo))
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
		err = fmt.Errorf("ArchiveBegin error head %d,%d,%d", magic, flag, rowCount)
	}
	return err
}

// Next -
func (q *ArchiveQuery) Next() (ar *Archive, err error) {
	if q.mode&ModeStatMask != 0 {
		err = fmt.Errorf("ArchiveQuery is stat mode")
		return nil, err
	}

	var index uint32
	var next int8
	io := q.io
	next, err = io.GetInt8()
	if next == 1 {
		ar = &Archive{}
		index, err = io.GetUint32()
		if index >= uint32(len(q.ids)) {
			err = fmt.Errorf("ArchiveQuery index %d out of range", index)
		} else {
			ar.ID = q.ids[index]
			err = ar.Read(io)
		}
	} else {
		magic, e := io.GetInt32()
		err = e
		if magic != MAGIC && e == nil {
			err = fmt.Errorf("ArchiveEnd error tail %d", magic)
		}
	}
	return ar, err
}

// NextStat -
func (q *ArchiveQuery) NextStat() (st *Stat, err error) {
	if q.mode&ModeStatMask == 0 {
		err = fmt.Errorf("ArchiveQuery is not stat mode")
		return nil, err
	}

	var index uint32
	var next int8
	io := q.io
	next, err = io.GetInt8()
	if next == 1 {
		st = &Stat{}
		index, err = io.GetUint32()
		if index >= uint32(len(q.ids)) {
			err = fmt.Errorf("ArchiveQuery index %d out of range", index)
		} else {
			st.ID = q.ids[index]
			err = st.Read(io, q.mode)
		}
	} else {
		magic, e := io.GetInt32()
		err = e
		if magic != MAGIC && e == nil {
			err = fmt.Errorf("ArchiveEnd error tail %d", magic)
		}
	}
	return st, err
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
			break // EOF
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
			break // EOF
		}
	}
	return result, nil
}
