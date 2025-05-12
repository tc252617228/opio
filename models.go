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
// Custom Error Types (Moved from client.go)
// ====================================================================================

var (
	ErrConnectionClosed  = errors.New("github.com/tc252617228/opio: client connection is closed")
	ErrTimeout           = errors.New("github.com/tc252617228/opio: operation timed out")
	ErrOpioServer        = errors.New("github.com/tc252617228/opio: opio server returned an error")
	ErrRecordNotFound    = errors.New("github.com/tc252617228/opio: record not found") // Redefined
	ErrUnsupportedIDType = errors.New("github.com/tc252617228/opio: unsupported ID type")
	// ErrScanTargetInvalid = errors.New("opio: Scan target must be a non-nil pointer to a slice")
	// ErrScanElementInvalid = errors.New("opio: Scan target slice element must be a struct")
	ErrUpdateRequiresFilters = errors.New("github.com/tc252617228/opio: update operation requires filters")
	// ErrNoFieldsToUpdate = errors.New("opio: no fields found to update")
	ErrSubscriptionClosed = errors.New("github.com/tc252617228/opio: subscription is closed")
)

type OpioServerError struct {
	Code    int32
	Message string
}

func (e *OpioServerError) Error() string {
	return fmt.Sprintf("github.com/tc252617228/opio: server error %d: %s", e.Code, e.Message)
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
			return errors.New("github.com/tc252617228/opio: 底层订阅对象无效")
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
			return errors.New("github.com/tc252617228/opio: 底层订阅对象无效")
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
