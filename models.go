package opio

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// ====================================================================================
// Data Models (Potentially for Metadata or Future Use)
// ====================================================================================

// GormPoint 对应数据库中的 'point' 表 (点表) - 保留用于可能的元数据查询
type GormPoint struct {
	ID int32     `gorm:"column:ID;primaryKey"`     // 点标识
	UD int64     `gorm:"column:UD"`                // UUID
	ND int32     `gorm:"column:ND"`                // 父节点标识
	PT int8      `gorm:"column:PT"`                // 点的来源
	RT int8      `gorm:"column:RT"`                // 点的类型
	PN string    `gorm:"column:PN;type:char(32)"`  // 点名
	AN string    `gorm:"column:AN;type:char(32)"`  // 别名
	ED string    `gorm:"column:ED;type:char(60)"`  // 描述
	KR string    `gorm:"column:KR;type:char(16)"`  // 特征字
	SG []byte    `gorm:"column:SG;type:binary(4)"` // 安全组
	FQ int16     `gorm:"column:FQ"`                // 分辨率
	CP int16     `gorm:"column:CP"`                // 处理器
	HW int32     `gorm:"column:HW"`                // 模块地址
	BP int16     `gorm:"column:BP"`                // 通道号
	LC int8      `gorm:"column:LC"`                // 报警类型
	AP int8      `gorm:"column:AP"`                // 报警优先级
	AR int8      `gorm:"column:AR"`                // 存档
	FL int32     `gorm:"column:FL"`                // 标志位
	ST string    `gorm:"column:ST;type:char(6)"`   // 值为 1 时的描述
	RS string    `gorm:"column:RS;type:char(6)"`   // 值为 0 时的描述
	EU string    `gorm:"column:EU;type:char(12)"`  // 单位
	FM int16     `gorm:"column:FM"`                // 显示小数位
	IV float32   `gorm:"column:IV"`                // 初始值
	TV float32   `gorm:"column:TV"`                // 量程上限
	BV float32   `gorm:"column:BV"`                // 量程下限
	LL float32   `gorm:"column:LL"`                // 报警低限
	HL float32   `gorm:"column:HL"`                // 报警高限
	ZL float32   `gorm:"column:ZL"`                // 报警低 2 限
	ZH float32   `gorm:"column:ZH"`                // 报警高 2 限
	L3 float32   `gorm:"column:L3"`                // 报警低 3 限
	H3 float32   `gorm:"column:H3"`                // 报警高 3 限
	L4 float32   `gorm:"column:L4"`                // 报警低 4 限
	H4 float32   `gorm:"column:H4"`                // 报警高 4 限
	DB float32   `gorm:"column:DB"`                // 死区
	DT int8      `gorm:"column:DT"`                // 死区类型
	KZ int8      `gorm:"column:KZ"`                // 压缩类型
	KT int8      `gorm:"column:KT"`                // 计算类型
	KO int8      `gorm:"column:KO"`                // 计算顺序
	CT time.Time `gorm:"column:CT"`                // 修改时间
	EX string    `gorm:"column:EX"`                // 计算表达式
	GN string    `gorm:"column:GN"`                // 全局名称
}

// TableName 指定 GormPoint 结构体对应的数据库表名
func (GormPoint) TableName() string {
	return "point"
}

// GormNode 对应数据库中的 'node' 表 (节点表) - 保留用于可能的元数据查询
type GormNode struct {
	ID int32     `gorm:"column:ID;primaryKey"`    // 点标识
	UD int64     `gorm:"column:UD"`               // UUID
	ND int32     `gorm:"column:ND"`               // 父节点标识
	PN string    `gorm:"column:PN;type:char(24)"` // 名称
	ED string    `gorm:"column:ED;type:char(60)"` // 描述
	FQ int32     `gorm:"column:FQ"`               // 分辨率
	LC int32     `gorm:"column:LC"`               // 报警类型
	AR int8      `gorm:"column:AR"`               // 存档
	OF int8      `gorm:"column:OF"`               // 离线
	CT time.Time `gorm:"column:CT"`               // 修改时间
	GN string    `gorm:"column:GN"`               // 全局名称
}

// TableName 指定 GormNode 结构体对应的数据库表名
func (GormNode) TableName() string {
	return "node"
}

// GormRealtime 对应数据库中的 'realtime' 表 (实时表) - 保留用于可能的元数据查询
type GormRealtime struct {
	ID int32     `gorm:"column:ID;primaryKey"` // 测点 ID (假设为主键)
	GN string    `gorm:"column:GN"`            // 测点名称
	TM time.Time `gorm:"column:TM"`            // 测点更新时间
	DS int16     `gorm:"column:DS"`            // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"`  // 测点数值
}

// TableName 指定 GormRealtime 结构体对应的数据库表名
func (GormRealtime) TableName() string {
	return "realtime"
}

// GormArchive 对应数据库中的 'archive' 表 (历史表) - 保留用于可能的元数据查询
type GormArchive struct {
	ID int32     `gorm:"column:ID"`           // 测点 ID (可能与 TM 组成复合主键/索引)
	GN string    `gorm:"column:GN"`           // 测点名称
	TM time.Time `gorm:"column:TM"`           // 测点数据更新时间 (可能与 ID 组成复合主键/索引)
	DS int16     `gorm:"column:DS"`           // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"` // 测点数值
}

// TableName 指定 GormArchive 结构体对应的数据库表名
func (GormArchive) TableName() string {
	return "archive"
}

// GormStat 对应数据库中的 'stat' 表 (历史统计表) - 保留用于可能的元数据查询
type GormStat struct {
	ID      int32     `gorm:"column:ID"`      // 测点 ID (可能与 TM, INTERVAL 组成复合主键/索引)
	GN      string    `gorm:"column:GN"`      // 测点名称
	TM      time.Time `gorm:"column:TM"`      // 测点更新时间 (可能与 ID, INTERVAL 组成复合主键/索引)
	DS      int16     `gorm:"column:DS"`      // 测点状态
	FLOW    float64   `gorm:"column:FLOW"`    // 累积值
	AVGV    float64   `gorm:"column:AVGV"`    // 时均平均值
	MAXV    float64   `gorm:"column:MAXV"`    // 最大值
	MINV    float64   `gorm:"column:MINV"`    // 最小值
	MAXTIME time.Time `gorm:"column:MAXTIME"` // 最大值时间
	MINTIME time.Time `gorm:"column:MINTIME"` // 最小值时间
}

// TableName 指定 GormStat 结构体对应的数据库表名
func (GormStat) TableName() string {
	return "stat"
}

// GormAlarm 对应数据库中的 'alarm' 表 (实时报警表) - 保留用于可能的元数据查询
type GormAlarm struct {
	ID int32     `gorm:"column:ID"`           // 测点 ID (主键/索引待定)
	GN string    `gorm:"column:GN"`           // 测点名称
	RT int8      `gorm:"column:RT"`           // 测点类型
	AL int8      `gorm:"column:AL"`           // 报警优先级
	AC int32     `gorm:"column:AC"`           // 报警颜色
	TF time.Time `gorm:"column:TF"`           // 首次报警时间
	TA time.Time `gorm:"column:TA"`           // 报警时间 (主键/索引待定)
	TM time.Time `gorm:"column:TM"`           // 测点更新时间 (主键/索引待定)
	DS int16     `gorm:"column:DS"`           // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"` // 测点数值
}

// TableName 指定 GormAlarm 结构体对应的数据库表名
func (GormAlarm) TableName() string {
	return "alarm"
}

// GormAAlarm 对应数据库中的 'aalarm' 表 (历史报警表) - 保留用于可能的元数据查询
type GormAAlarm struct {
	ID int32     `gorm:"column:ID"`           // 测点 ID (主键/索引待定)
	GN string    `gorm:"column:GN"`           // 测点名称
	RT int8      `gorm:"column:RT"`           // 测点类型
	AL int8      `gorm:"column:AL"`           // 报警优先级
	AC int32     `gorm:"column:AC"`           // 报警颜色
	TF time.Time `gorm:"column:TF"`           // 首次报警时间
	TA time.Time `gorm:"column:TA"`           // 报警时间 (主键/索引待定)
	TM time.Time `gorm:"column:TM"`           // 测点更新时间 (主键/索引待定)
	DS int16     `gorm:"column:DS"`           // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"` // 测点数值
}

// TableName 指定 GormAAlarm 结构体对应的数据库表名
func (GormAAlarm) TableName() string {
	return "aalarm"
}

// GormUser 对应数据库中的 'user' 表 (用户表) - 保留用于可能的元数据查询
type GormUser struct {
	US string `gorm:"column:US;type:text;primaryKey"` // 用户信息 (假设为主键)
	PW string `gorm:"column:PW;type:text"`            // 用户密码
}

// TableName 指定 GormUser 结构体对应的数据库表名
func (GormUser) TableName() string {
	return "user"
}

// GormGroup 对应数据库中的 'groups' 表 (资源组表) - 保留用于可能的元数据查询
type GormGroup struct {
	ID int    `gorm:"column:ID;primaryKey"` // 资源组 ID
	GP string `gorm:"column:GP;type:text"`  // 资源组信息
}

// TableName 指定 GormGroup 结构体对应的数据库表名
func (GormGroup) TableName() string {
	return "groups"
}

// GormAccess 对应数据库中的 'access' 表 (权限表) - 保留用于可能的元数据查询
type GormAccess struct {
	US string `gorm:"column:US;type:text;primaryKey"` // 用户信息 (复合主键)
	GP int    `gorm:"column:GP;primaryKey"`           // 资源组 (复合主键)
	PL string `gorm:"column:PL;type:text"`            // 权限信息
}

// TableName 指定 GormAccess 结构体对应的数据库表名
func (GormAccess) TableName() string {
	return "access"
}

// ====================================================================================
// Custom Error Types (Moved from client.go)
// ====================================================================================

var (
	ErrConnectionClosed  = errors.New("opio: client connection is closed")
	ErrTimeout           = errors.New("opio: operation timed out")
	ErrOpioServer        = errors.New("opio: opio server returned an error")
	ErrRecordNotFound    = errors.New("opio: record not found") // Redefined
	ErrUnsupportedIDType = errors.New("opio: unsupported ID type")
	// ErrScanTargetInvalid = errors.New("opio: Scan target must be a non-nil pointer to a slice")
	// ErrScanElementInvalid = errors.New("opio: Scan target slice element must be a struct")
	ErrUpdateRequiresFilters = errors.New("opio: update operation requires filters")
	// ErrNoFieldsToUpdate = errors.New("opio: no fields found to update")
	ErrSubscriptionClosed = errors.New("opio: subscription is closed")
)

type OpioServerError struct {
	Code    int32
	Message string
}

func (e *OpioServerError) Error() string {
	return fmt.Sprintf("opio: server error %d: %s", e.Code, e.Message)
}

func (e *OpioServerError) Unwrap() error {
	return ErrOpioServer
}

// ====================================================================================
// Query and Subscription Structures (Moved from client.go)
// ====================================================================================

// QueryOptions 定义结构化查询的选项。
type QueryOptions struct {
	Filters []Filter // 过滤条件
	OrderBy string   // 排序条件 (例如 "ID ASC", "TM DESC")
	Limit   string   // 限制返回记录数 (例如 "10", "10,20")
}

type SubscriptionEvent struct {
	Data map[string]interface{}
	Err  error
}

func (e *SubscriptionEvent) Scan(dest interface{}) error {
	if e.Err != nil {
		return fmt.Errorf("无法扫描带有错误的事件: %w", e.Err)
	}
	if e.Data == nil {
		return errors.New("无法扫描空的事件数据 (event.Data is nil)")
	}

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("目标必须是一个非 nil 指针")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return errors.New("目标必须指向一个结构体")
	}

	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		field := rt.Field(i)
		fieldVal := rv.Field(i)

		if !fieldVal.CanSet() {
			continue
		}
		tag := field.Tag.Get("opio")
		if tag == "-" {
			continue
		}
		if tag == "" {
			tag = field.Tag.Get("db")
			if tag == "-" {
				continue
			}
		}

		mapKey := tag
		if mapKey == "" {
			mapKey = field.Name
		}

		mapValue, ok := e.Data[mapKey]
		if !ok {
			foundCaseInsensitive := false
			for k, v := range e.Data {
				if strings.EqualFold(k, mapKey) {
					mapValue = v
					ok = true
					foundCaseInsensitive = true
					break
				}
			}
			if !foundCaseInsensitive {
				continue
			}
		}

		if mapValue == nil {
			if fieldVal.Kind() == reflect.Ptr || fieldVal.Kind() == reflect.Interface || fieldVal.Kind() == reflect.Map || fieldVal.Kind() == reflect.Slice {
				if !fieldVal.IsNil() {
					fieldVal.Set(reflect.Zero(fieldVal.Type()))
				}
			}
			continue
		}

		sourceVal := reflect.ValueOf(mapValue)
		targetType := fieldVal.Type()

		if sourceVal.Type().AssignableTo(targetType) {
			fieldVal.Set(sourceVal)
		} else if sourceVal.Type().ConvertibleTo(targetType) {
			fieldVal.Set(sourceVal.Convert(targetType))
		} else {
			// Fallback to custom conversion logic (`assignWithConversion`)
			err := assignWithConversion(fieldVal, sourceVal)
			if err != nil {
				// Return the error from assignWithConversion
				return fmt.Errorf("字段 '%s' (%s) 赋值失败: %w", field.Name, mapKey, err)
			}
		}
	}
	return nil // Scan completed successfully
}

type Subscription struct {
	client   *OpioClient // Changed from GormClient
	sub      *Subscribe
	eventCh  chan SubscriptionEvent
	cancelFn context.CancelFunc
	closed   chan struct{}
	closeMu  sync.Mutex
}

func (s *Subscription) Events() <-chan SubscriptionEvent {
	return s.eventCh
}

func (s *Subscription) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	select {
	case <-s.closed:
		return ErrSubscriptionClosed
	default:
		close(s.closed)
	}

	if s.cancelFn != nil {
		s.cancelFn()
	}

	if s.sub != nil {
		s.sub.Close()
		s.sub = nil
	}
	return nil
}

func (s *Subscription) AddKeys(keys interface{}) error {
	select {
	case <-s.closed:
		return ErrSubscriptionClosed
	default:
		if s.sub == nil {
			return errors.New("opio: 底层订阅对象无效")
		}
		return s.sub.Subscribe(keys)
	}
}

func (s *Subscription) RemoveKeys(keys interface{}) error {
	select {
	case <-s.closed:
		return ErrSubscriptionClosed
	default:
		if s.sub == nil {
			return errors.New("opio: 底层订阅对象无效")
		}
		return s.sub.UnSubscribe(keys)
	}
}

type SubscribeOptions struct {
	Snapshot        bool
	EventChanBuffer int
}

// ====================================================================================
// Helper Functions (Moved from client.go)
// ====================================================================================

func assignWithConversion(targetField reflect.Value, sourceValue reflect.Value) error {
	targetType := targetField.Type()
	sourceType := sourceValue.Type()

	if sourceType.Kind() == reflect.Slice && sourceType.Elem().Kind() == reflect.Uint8 {
		byteSlice := sourceValue.Bytes()
		switch targetType.Kind() {
		case reflect.String:
			targetField.SetString(string(byteSlice))
			return nil
		}
	}

	if targetType.Kind() == reflect.String {
		strVal := fmt.Sprintf("%v", sourceValue.Interface())
		targetField.SetString(strVal)
		return nil
	}

	if isNumeric(targetType.Kind()) && isNumeric(sourceType.Kind()) {
		var floatVal float64
		if sourceValue.CanFloat() {
			floatVal = sourceValue.Float()
		} else if sourceValue.CanInt() {
			floatVal = float64(sourceValue.Int())
		} else if sourceValue.CanUint() {
			floatVal = float64(sourceValue.Uint())
		} else {
			return fmt.Errorf("无法将源类型 %s 转换为 float64", sourceType)
		}

		switch targetType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			targetField.SetInt(int64(floatVal))
			return nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if floatVal < 0 {
				// Return error for negative float to unsigned int conversion
				return fmt.Errorf("无法将负数 %f 转换为无符号整数 %s", floatVal, targetType)
			}
			// Add overflow check (optional but good practice)
			if floatVal > float64(^uint64(0)) {
				return fmt.Errorf("数值 %f 超出目标无符号整数类型 %s 的范围", floatVal, targetType)
			}
			targetField.SetUint(uint64(floatVal)) // Truncates decimal part
			return nil
		case reflect.Float32, reflect.Float64:
			// Add overflow check (optional but good practice)
			if targetType.Kind() == reflect.Float32 && (floatVal > float64(3.402823466e+38) || floatVal < float64(-3.402823466e+38)) {
				return fmt.Errorf("数值 %f 超出目标浮点类型 %s 的范围", floatVal, targetType)
			}
			targetField.SetFloat(floatVal)
			return nil
		}
	}

	return fmt.Errorf("不支持从类型 %s 转换为 %s", sourceType, targetType)
}

func isNumeric(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func scanRowToStruct(dataSet *OPDataSet, dest interface{}) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return errors.New("dest 必须是指向结构体的非 nil 指针")
	}
	elem := rv.Elem()
	elemType := elem.Type()
	columns := dataSet.GetColumns()
	colMap := make(map[string]uint32, len(columns))
	for i, col := range columns {
		colMap[col.GetName()] = uint32(i)
	}

	for i := 0; i < elem.NumField(); i++ {
		field := elemType.Field(i)
		fieldVal := elem.Field(i)
		if !fieldVal.CanSet() {
			continue
		}

		tag := field.Tag.Get("opio")
		if tag == "-" {
			continue
		}
		if tag == "" {
			tag = field.Tag.Get("db")
			if tag == "-" {
				continue
			}
		}
		colName := tag
		if colName == "" {
			colName = field.Name
		}

		var colIndex uint32
		found := false
		for name, index := range colMap {
			if strings.EqualFold(name, colName) {
				colIndex = index
				found = true
				break
			}
		}
		if !found {
			continue
		}

		rawValue, err := dataSet.GetValue(colIndex)
		if err != nil {
			continue
		}

		if rawValue == nil {
			if fieldVal.Kind() == reflect.Ptr || fieldVal.Kind() == reflect.Interface || fieldVal.Kind() == reflect.Map || fieldVal.Kind() == reflect.Slice {
				if !fieldVal.IsNil() {
					fieldVal.Set(reflect.Zero(fieldVal.Type()))
				}
			}
			continue
		}

		sourceVal := reflect.ValueOf(rawValue)
		targetType := fieldVal.Type()

		if sourceVal.Type().AssignableTo(targetType) {
			fieldVal.Set(sourceVal)
		} else if sourceVal.Type().ConvertibleTo(targetType) {
			fieldVal.Set(sourceVal.Convert(targetType))
		} else {
			errAssign := assignWithConversion(fieldVal, sourceVal)
			if errAssign != nil {
				// Ignore assignment error
			}
		}
	}
	return nil
}

func inferOpioType(value interface{}) int {
	if value == nil {
		return VtNull
	}
	switch value.(type) {
	case bool:
		return VtBool
	case int8, uint8:
		return VtInt8
	case int16, uint16:
		return VtInt16
	case int32, uint32:
		return VtInt32
	case int64, uint64, int:
		return VtInt64
	case float32:
		return VtFloat
	case float64:
		return VtDouble
	case time.Time:
		return VtDateTime
	case string:
		return VtString
	case []byte:
		return VtBinary
	case map[string]interface{}:
		return VtMap
	case []interface{}:
		return VtSlice
	default:
		return VtObject
	}
}
