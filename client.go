package opio

import (
	"context"
	"errors"
	"fmt"
	"log" // 用于日志记录
	"sync"
	"time"
	// GORM imports are removed
	// reflect and strings imports removed as they are no longer used directly here
)

// ====================================================================================
// OpioClient Definition and Basic Operations
// ====================================================================================

type OpioClient struct {
	opioConn       *IOConnect
	opioConnMu     sync.RWMutex
	defaultTimeout time.Duration
	Logger         *log.Logger
}

func Connect(ctx context.Context, opioHost string, opioPort int, opioUser string, opioPass string, connectTimeout time.Duration) (*OpioClient, error) {
	op, err := Init(opioHost, opioPort, int(connectTimeout.Seconds()), opioUser, opioPass)
	if err != nil {
		return nil, fmt.Errorf("opio.Connect: 无法初始化 opio 连接: %w", err)
	}

	client := &OpioClient{
		opioConn: op,
	}
	client.opioConn.SetCompressModel(ZIP_MODEL_Uncompressed)
	return client, nil
}

// Close 关闭 Opio 连接。
// 多次调用 Close 是安全的，后续调用会返回 ErrConnectionClosed。
func (c *OpioClient) Close() error {
	c.opioConnMu.Lock()
	// 检查连接是否已关闭
	if c.opioConn == nil {
		c.opioConnMu.Unlock()
		return ErrConnectionClosed // 如果已经是 nil，直接返回错误
	}
	// 获取连接并准备关闭
	connToClose := c.opioConn
	c.opioConn = nil // 先标记为 nil
	c.opioConnMu.Unlock()

	// 执行实际的关闭操作
	err := connToClose.Close()
	if err != nil {
		// 记录错误，但仍然认为连接已关闭
		if c.Logger != nil {
			c.Logger.Printf("关闭 opio 连接时发生错误: %v", err)
		}
		// 可以选择返回包装后的错误，或者仅返回 ErrConnectionClosed
		// 这里选择返回包装错误，以便调用者了解底层细节
		return fmt.Errorf("关闭 opio 连接失败: %w", err)
	}
	return nil // 第一次成功关闭返回 nil
}

func (c *OpioClient) SetDefaultTimeout(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	c.defaultTimeout = duration
}

func (c *OpioClient) SetLogger(logger *log.Logger) {
	c.Logger = logger
}

func (c *OpioClient) getContextWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, deadlineSet := ctx.Deadline(); !deadlineSet && c.defaultTimeout > 0 {
		return context.WithTimeout(ctx, c.defaultTimeout)
	}
	return ctx, func() {}
}

// ====================================================================================
// Opio Data Operations (V2 Style Protocol) - Placeholders
// ====================================================================================

// Create 使用 Opio 协议插入记录。(需要基于 opio_test.go 实现)
func (c *OpioClient) Create(ctx context.Context, tableName string, data []map[string]interface{}) error {
	return errors.New("Create not implemented for OpioClient (V2 Protocol)")
}

// FindByID 使用 Opio 协议根据主键查询单条记录。(需要基于 opio_test.go 实现)
func (c *OpioClient) FindByID(ctx context.Context, dest interface{}, tableName string, idColumn string, idValue interface{}) error {
	return errors.New("FindByID not implemented for OpioClient (V2 Protocol)")
}

// Query 使用 Opio 协议根据条件查询记录。(需要基于 opio_test.go 实现)
func (c *OpioClient) Query(ctx context.Context, dest interface{}, tableName string, columns []string, opts *QueryOptions) error {
	return errors.New("Query not implemented for OpioClient (V2 Protocol)")
}

// Update 使用 Opio 协议更新记录。(需要基于 opio_test.go 实现)
func (c *OpioClient) Update(ctx context.Context, tableName string, updates map[string]interface{}, filters []Filter) error {
	return errors.New("Update not implemented for OpioClient (V2 Protocol)")
}

// Delete 使用 Opio 协议删除记录。(需要基于 opio_test.go 实现)
func (c *OpioClient) Delete(ctx context.Context, tableName string, filters []Filter) error {
	return errors.New("Delete not implemented for OpioClient (V2 Protocol)")
}

// ====================================================================================
// Opio SQL Execution - Placeholders
// ====================================================================================

// ExecSQL 使用 Opio 协议执行非查询类的 SQL 语句。(需要基于 opio_test.go 实现)
func (c *OpioClient) ExecSQL(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	return 0, errors.New("ExecSQL not implemented for OpioClient")
}

// QuerySQL 使用 Opio 协议执行 SELECT 查询。(需要基于 opio_test.go 实现)
func (c *OpioClient) QuerySQL(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	return errors.New("QuerySQL not implemented for OpioClient")
}

// ====================================================================================
// Subscription API
// ====================================================================================

func (c *OpioClient) Subscribe(ctx context.Context, tableName string, keyName string, keys interface{}, opts *SubscribeOptions) (*Subscription, error) {
	c.opioConnMu.RLock()
	if c.opioConn == nil {
		c.opioConnMu.RUnlock()
		return nil, ErrConnectionClosed
	}
	sub, err := c.opioConn.NewSubscribe(tableName)
	c.opioConnMu.RUnlock()

	if err != nil {
		return nil, fmt.Errorf("创建底层订阅失败: %w", err)
	}

	useSnapshot := false
	eventChanBuffer := 100
	if opts != nil {
		useSnapshot = opts.Snapshot
		if opts.EventChanBuffer > 0 {
			eventChanBuffer = opts.EventChanBuffer
		}
	}
	if err := sub.SetSnapshot(useSnapshot); err != nil {
		sub.Close()
		return nil, fmt.Errorf("设置快照选项失败: %w", err)
	}

	eventCh := make(chan SubscriptionEvent, eventChanBuffer)
	closedCh := make(chan struct{})
	subCtx, cancelFn := context.WithCancel(ctx)

	subscription := &Subscription{
		client:   c,
		sub:      sub,
		eventCh:  eventCh,
		cancelFn: cancelFn,
		closed:   closedCh,
	}

	callback := func(res *Response) {
		select {
		case <-subCtx.Done():
			return
		default:
		}

		if res.GetErrNo() != 0 {
			serverErr := &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
			errEvent := SubscriptionEvent{Err: fmt.Errorf("opio.Subscription: 收到服务器错误: %w", serverErr)}
			select {
			case eventCh <- errEvent:
			case <-subCtx.Done():
			}
			return
		}

		dataSet := res.GetDataSet()
		if dataSet == nil {
			return
		}
		defer dataSet.Close()

		columns := dataSet.GetColumns()
		for {
			hasNext, err := dataSet.Next()
			if err != nil {
				errEvent := SubscriptionEvent{Err: fmt.Errorf("读取订阅数据行失败: %w", err)}
				select {
				case eventCh <- errEvent:
				case <-subCtx.Done():
				}
				return
			}
			if !hasNext {
				break
			}

			// Process the current row
			rowMap := make(map[string]interface{})
			var rowErr error // Track errors within a row
			for i, col := range columns {
				val, err := dataSet.GetValue(uint32(i))
				if err != nil {
					// Log the error if a logger is configured
					if c.Logger != nil {
						c.Logger.Printf("Error getting value for column %s (index %d): %v", col.name, i, err)
					}
					rowMap[col.name] = nil // Keep setting nil for now, but logged the error
					if rowErr == nil {     // Store the first error encountered for this row
						rowErr = fmt.Errorf("error getting value for column %s: %w", col.name, err)
					}
				} else {
					rowMap[col.name] = val
				}
			}

			// Create the event (potentially with an error if GetValue failed)
			dataEvent := SubscriptionEvent{Data: rowMap, Err: rowErr}
			select {
			case eventCh <- dataEvent: // Send the event
			case <-subCtx.Done():
				return
			}
		}
	}

	// Initialize the underlying subscription
	err = sub.InitSubscribe(keys, keyName, callback)
	if err != nil {
		// Cleanup if InitSubscribe fails
		sub.Close()
		cancelFn()
		close(closedCh)
		// Rely on the cleanup goroutine's defer to close eventCh
		// close(eventCh) // REMOVED
		return nil, fmt.Errorf("初始化底层订阅失败: %w", err)
	}

	// Cleanup goroutine
	go func() {
		defer close(eventCh) // Ensure event channel is closed when this goroutine exits
		select {
		case <-ctx.Done(): // Wait for the original context to be done
			// log.Println("Original context cancelled, closing subscription.")
			subscription.Close()
		case <-closedCh: // Wait for Subscription.Close() to be called explicitly
			// log.Println("Subscription explicitly closed.")
			// No need to call subscription.Close() again here, it already happened.
		}
	}()

	return subscription, nil
}

// ====================================================================================
// V3 API Operations (Direct Realtime/Archive/Stat Access)
// ====================================================================================

func (c *OpioClient) ReadRealtime(ctx context.Context, values []Value) error {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}
	return conn.ReadRealtime(values)
}

func (c *OpioClient) WriteRealtime(ctx context.Context, values []Value) error {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}
	return conn.WriteRealtime(values)
}

func (c *OpioClient) ReadArchive(ctx context.Context, ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Archive, error) {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return nil, ErrConnectionClosed
	}
	return conn.ReadArchive(ids, mode, begin, end, interval)
}

func (c *OpioClient) WriteArchive(ctx context.Context, archives []*Archive, cache bool) error {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}
	return conn.WriteArchive(archives, cache)
}

func (c *OpioClient) ReadStat(ctx context.Context, ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Stat, error) {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return nil, ErrConnectionClosed
	}
	return conn.ReadStat(ids, mode, begin, end, interval)
}

// ====================================================================================
// Helper Functions (Moved to models.go)
// ====================================================================================
// scanRowToStruct and inferOpioType are now in models.go
