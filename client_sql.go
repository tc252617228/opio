package opio

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	// "log" // log 在这些方法中未使用
	// "sync" // sync 在这些方法中未使用
	// "time" // time 在这些方法中未使用
)

// ====================================================================================
// Opio SQL 执行 (从 client.go 移动)
// ====================================================================================

// ExecSQL 使用 Opio 协议执行非查询类的 SQL 语句。
// 注意：此实现不支持参数化查询 (args ...interface{})。
func (c *OpioClient) ExecSQL(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	// 检查是否提供了不支持的参数
	if len(args) > 0 {
		return 0, errors.New("opio: ExecSQL 不支持参数化查询，请直接格式化 SQL 字符串")
	}

	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return 0, ErrConnectionClosed
	}

	// 使用带超时的 context
	reqCtx, cancel := c.getContextWithTimeout(ctx)
	defer cancel()

	// 创建请求
	req := conn.NewRequest(nil) // 使用 nil map 初始化
	req.SetService("openPlant") // 假设服务名固定
	req.SetAction(ActionExecSQL)
	req.SetSQL(sql)

	// 发送请求并获取响应
	// 使用 select 结合 context 超时来发送和接收
	var res *Response
	var err error
	done := make(chan struct{})

	go func() {
		defer close(done)
		err = req.WriteAndFlush()
		if err != nil {
			return
		}
		// 读取响应（GetResponse 内部会调用 Read）
		res, err = req.GetResponse()
	}()

	select {
	case <-reqCtx.Done():
		// 超时或取消
		// 尝试关闭底层连接或取消操作（如果 IOConnect 支持）
		// 注意：直接关闭 conn 可能影响其他操作，需要更精细的控制
		// conn.Close() // 谨慎使用
		return 0, fmt.Errorf("opio: ExecSQL context 超时或取消: %w", reqCtx.Err())
	case <-done:
		// 操作完成（成功或失败）
		if err != nil {
			return 0, fmt.Errorf("opio: ExecSQL 请求/响应失败: %w", err)
		}
	}

	// 检查响应错误
	if res.GetErrNo() != 0 {
		return 0, &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
	}

	// V2 ExecSQL 似乎不返回影响的行数，返回 0
	return 0, nil
}

// QuerySQL 使用 Opio 协议执行 SELECT 查询。
// dest 必须是指向 slice 的指针，slice 的元素类型必须是 struct 或 *struct。
// 注意：此实现不支持参数化查询 (args ...interface{})。
func (c *OpioClient) QuerySQL(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	// 检查是否提供了不支持的参数
	if len(args) > 0 {
		return errors.New("opio: QuerySQL 不支持参数化查询，请直接格式化 SQL 字符串")
	}

	// 验证 dest 类型
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("opio: QuerySQL 的 dest 参数必须是一个非 nil 指针")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return errors.New("opio: QuerySQL 的 dest 参数必须指向一个 slice")
	}
	sliceElemType := rv.Type().Elem()
	if sliceElemType.Kind() != reflect.Struct {
		// 允许元素为指向结构体的指针
		if sliceElemType.Kind() != reflect.Ptr || sliceElemType.Elem().Kind() != reflect.Struct {
			return errors.New("opio: QuerySQL 的 dest slice 的元素必须是 struct 或指向 struct 的指针")
		}
	}

	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}

	// 使用带超时的 context
	reqCtx, cancel := c.getContextWithTimeout(ctx)
	defer cancel()

	// 创建请求
	req := conn.NewRequest(nil)
	req.SetService("openPlant") // 假设服务名固定
	req.SetAction(ActionSelect) // 使用 Select Action
	req.SetSQL(sql)

	// 发送请求并获取响应
	var res *Response
	var err error
	done := make(chan struct{})

	go func() {
		defer close(done)
		err = req.WriteAndFlush()
		if err != nil {
			return
		}
		res, err = req.GetResponse()
	}()

	select {
	case <-reqCtx.Done():
		return fmt.Errorf("opio: QuerySQL context 超时或取消: %w", reqCtx.Err())
	case <-done:
		if err != nil {
			return fmt.Errorf("opio: QuerySQL 请求/响应失败: %w", err)
		}
	}

	// 检查响应错误
	if res.GetErrNo() != 0 {
		// 检查是否是记录未找到的特定错误（如果协议支持）
		// if res.GetErrNo() == someRecordNotFoundErrorCode {
		//     return ErrRecordNotFound // 或者返回 nil，因为没有找到记录不是一个真正的错误
		// }
		return &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
	}

	// 获取数据集
	dataSet := res.GetDataSet()
	if dataSet == nil {
		// 没有返回数据集，可能是 0 行，返回 nil 错误
		return nil
	}
	defer dataSet.Close()

	// --- 处理结果集 ---
	directSlice := rv // slice 本身
	isPtrElement := sliceElemType.Kind() == reflect.Ptr
	baseStructType := sliceElemType
	if isPtrElement {
		baseStructType = sliceElemType.Elem() // 如果元素是指针，获取其指向的 struct 类型
	}

	// 清空目标 slice (如果需要)
	directSlice.Set(reflect.MakeSlice(directSlice.Type(), 0, 0))

	// 迭代数据集
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			return fmt.Errorf("opio: QuerySQL 读取下一行失败: %w", err)
		}
		if !hasNext {
			break // 结束
		}

		// 创建 slice 元素 (struct 或 *struct)
		newElem := reflect.New(baseStructType) // 创建一个指向新 struct 零值的指针

		// 扫描行到新创建的元素 (需要传递指针)
		scanErr := scanRowToStruct(dataSet, newElem.Interface()) // scanRowToStruct 在 models.go 中
		if scanErr != nil {
			// 记录错误但继续处理下一行？或者直接返回错误？
			// 暂时选择返回错误
			if c.Logger != nil {
				c.Logger.Printf("QuerySQL: scanRowToStruct 对某行失败: %v", scanErr)
			}
			return fmt.Errorf("opio: QuerySQL scanRowToStruct 失败: %w", scanErr)
			// continue
		}

		// 将扫描到的元素添加到目标 slice
		if isPtrElement {
			directSlice.Set(reflect.Append(directSlice, newElem)) // 添加指针
		} else {
			directSlice.Set(reflect.Append(directSlice, newElem.Elem())) // 添加 struct 值
		}
	}

	return nil // 成功
}
