package opio

import (
	"context"
	"errors"
	"fmt"
	"log" // 用于日志记录
	"reflect"
	"sync"
	"time"
	// GORM 导入已移除
)

// ====================================================================================
// OpioClient 定义和基本操作
// ====================================================================================

// OpioClient 是与 OpenPlant 服务器交互的客户端。
type OpioClient struct {
	opioConn       *IOConnect    // 底层连接对象
	opioConnMu     sync.RWMutex  // 保护 opioConn 的读写锁
	defaultTimeout time.Duration // 默认操作超时时间
	Logger         *log.Logger   // 用于记录日志的对象
}

// Connect 初始化并连接到 OpenPlant 服务器，返回一个新的 OpioClient 实例。
func Connect(ctx context.Context, opioHost string, opioPort int, opioUser string, opioPass string, connectTimeout time.Duration) (*OpioClient, error) {
	// 注意：这里的 Init 函数可能需要 context 来处理连接超时
	op, err := Init(opioHost, opioPort, int(connectTimeout.Seconds()), opioUser, opioPass)
	if err != nil {
		return nil, fmt.Errorf("opio.Connect: 无法初始化 opio 连接: %w", err)
	}

	client := &OpioClient{
		opioConn: op,
	}
	// 设置默认不压缩
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
	c.opioConn = nil // 先标记为 nil，表示正在关闭或已关闭
	c.opioConnMu.Unlock()

	// 执行实际的关闭操作
	err := connToClose.Close()
	if err != nil {
		// 记录错误，但仍然认为连接已关闭
		if c.Logger != nil {
			c.Logger.Printf("关闭 opio 连接时发生错误: %v", err)
		}
		// 返回包装后的错误，以便调用者了解底层细节
		return fmt.Errorf("关闭 opio 连接失败: %w", err)
	}
	return nil // 第一次成功关闭返回 nil
}

// SetDefaultTimeout 设置客户端操作的默认超时时间。
func (c *OpioClient) SetDefaultTimeout(duration time.Duration) {
	if duration < 0 {
		duration = 0 // 不允许负数超时
	}
	c.defaultTimeout = duration
}

// SetLogger 设置用于客户端日志记录的 logger。
func (c *OpioClient) SetLogger(logger *log.Logger) {
	c.Logger = logger
}

// getContextWithTimeout 根据客户端的默认超时设置，获取可能带有超时的 context。
func (c *OpioClient) getContextWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, deadlineSet := ctx.Deadline(); !deadlineSet && c.defaultTimeout > 0 {
		return context.WithTimeout(ctx, c.defaultTimeout)
	}
	// 如果原始 context 已有 deadline 或未设置默认超时，则返回原始 context
	return ctx, func() {} // 返回一个无操作的 cancel 函数
}

// ====================================================================================
// Opio 数据操作 (V2 风格协议)
// ====================================================================================

// mapSliceToTable 将 map 切片转换为 opio.Table，并推断列类型。
// 注意：这是一个基础实现。类型推断可能需要优化。
// 假设切片中的所有 map 都具有相同的键。
func mapSliceToTable(tableName string, data []map[string]interface{}) (*Table, error) {
	if len(data) == 0 {
		return nil, errors.New("opio: 无法从空数据切片创建表")
	}

	// 使用 NewTable 构造函数设置表名
	table := NewTable(tableName, uint(len(data))) // 预分配行切片容量

	// 从第一行推断列信息
	firstRow := data[0]
	colNames := make([]string, 0, len(firstRow))
	colIndexMap := make(map[string]uint32) // 列名到索引的映射

	for name, value := range firstRow {
		colNames = append(colNames, name)
		colType := inferOpioType(value) // 使用 models.go 中的辅助函数
		// AddColumnExtension 需要长度和 ext ([]byte)。使用 0 和 nil。
		if !table.AddColumnExtension(name, colType, 0, nil) {
			return nil, fmt.Errorf("opio: 添加列 '%s' 失败", name)
		}
		colIndexMap[name] = table.colCount - 1 // 添加后存储索引
	}

	// 通过设置列值并绑定行来添加数据行
	for _, rowMap := range data {
		for _, colName := range colNames { // 按一致的顺序迭代
			value, exists := rowMap[colName]
			colIndex := colIndexMap[colName] // 获取列索引

			var err error
			if !exists {
				// 处理行中缺失的键 - 设置列为空
				err = table.SetColumnEmpty(colIndex)
			} else {
				// 设置实际值
				err = table.SetColumnValue(colIndex, value)
			}
			if err != nil {
				// 收集 SetColumnValue/SetColumnEmpty 的错误
				// table.errors 应包含错误，如果需要可以记录日志
				if table.errors == nil {
					table.errors = make([]error, 0)
				} // 确保 errors 切片存在
				table.errors = append(table.errors, fmt.Errorf("列 '%s' 的行处理错误: %w", colName, err))
				// 决定是继续处理该行还是停止
				// continue // 选项 1: 继续处理此行中的其他列
				// break    // 选项 2: 停止处理此行，移动到下一个 map
			}
		}
		// 设置完当前 map 的所有列后绑定行
		table.BindRow()
	}

	// 检查行处理期间累积的错误
	if tableErrors := table.GetErrors(); len(tableErrors) > 0 {
		// 合并错误？返回第一个错误？
		// 目前返回合并的错误消息
		errorMessages := ""
		for _, e := range tableErrors {
			errorMessages += e.Error() + "; "
		}
		return nil, fmt.Errorf("opio: 表创建期间发生错误: %s", errorMessages)
	}

	return table, nil
}

// Create 使用 Opio 协议插入记录。
func (c *OpioClient) Create(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return errors.New("opio: Create 操作需要至少一条数据记录")
	}

	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}

	// 将数据转换为 opio.Table
	table, err := mapSliceToTable(tableName, data)
	if err != nil {
		return fmt.Errorf("opio: Create 转换数据到表失败: %w", err)
	}

	// 使用带超时的 context
	reqCtx, cancel := c.getContextWithTimeout(ctx)
	defer cancel()

	// 创建请求
	req := conn.NewRequest(nil)
	req.SetService("openPlant") // 假设服务名固定
	req.SetAction(ActionInsert) // Create 使用 Insert 操作
	// SetTable 内部也会设置 PropTable 和 PropColumns
	err = req.SetTable(table)
	if err != nil {
		// 如果 mapSliceToTable 成功，这里不应发生，但还是检查一下
		return fmt.Errorf("opio: Create 在请求上设置表失败: %w", err)
	}

	// 发送请求并获取响应
	var res *Response
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
		return fmt.Errorf("opio: Create context 超时或取消: %w", reqCtx.Err())
	case <-done:
		if err != nil {
			return fmt.Errorf("opio: Create 请求/响应失败: %w", err)
		}
	}

	// 检查响应错误
	if res.GetErrNo() != 0 {
		return &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
	}

	return nil // 成功
}

// FindByID 使用 Opio 协议根据主键查询单条记录。
// dest 必须是指向 struct 的非 nil 指针。
func (c *OpioClient) FindByID(ctx context.Context, dest interface{}, tableName string, idColumn string, idValue interface{}) error {
	// 验证 dest 类型
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("opio: FindByID 的 dest 参数必须是一个非 nil 指针")
	}
	if rv.Elem().Kind() != reflect.Struct {
		return errors.New("opio: FindByID 的 dest 参数必须指向一个 struct")
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
	req.SetAction(ActionSelect)
	req.SetTableName(tableName)

	// 设置过滤条件
	// 注意：NewFilter 需要值作为字符串，需要转换 idValue
	idValueStr := fmt.Sprintf("%v", idValue)
	filters := []Filter{*NewFilter(idColumn, OperEQ, idValueStr, RelationAnd)}
	req.SetFilters(filters)

	// 设置 Limit 为 1
	req.SetLimit("1")

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
		return fmt.Errorf("opio: FindByID context 超时或取消: %w", reqCtx.Err())
	case <-done:
		if err != nil {
			return fmt.Errorf("opio: FindByID 请求/响应失败: %w", err)
		}
	}

	// 检查响应错误
	if res.GetErrNo() != 0 {
		// TODO: 检查特定的 "未找到" 错误码（如果协议定义了）
		// if res.GetErrNo() == SpecificNotFoundErrorCode {
		// 	return ErrRecordNotFound
		// }
		return &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
	}

	// 获取数据集
	dataSet := res.GetDataSet()
	if dataSet == nil {
		return ErrRecordNotFound // 没有数据集意味着未找到
	}
	defer dataSet.Close()

	// 尝试读取第一行
	hasNext, err := dataSet.Next()
	if err != nil {
		return fmt.Errorf("opio: FindByID 读取数据行失败: %w", err)
	}
	if !hasNext {
		return ErrRecordNotFound // 没有行意味着未找到
	}

	// 扫描行到目标结构体
	scanErr := scanRowToStruct(dataSet, dest) // scanRowToStruct 在 models.go 中
	if scanErr != nil {
		return fmt.Errorf("opio: FindByID scanRowToStruct 失败: %w", scanErr)
	}

	// 检查是否有多余的行（理论上不应该发生，因为 Limit=1）
	hasNextAfterScan, errAfterScan := dataSet.Next()
	if errAfterScan == nil && hasNextAfterScan {
		// 记录警告？这表示有多条记录匹配 ID。
		if c.Logger != nil {
			c.Logger.Printf("FindByID 警告: 表 '%s' 中找到多条记录匹配 idColumn '%s', idValue '%v'", tableName, idColumn, idValue)
		}
	}

	return nil // 成功
}

// Query 使用 Opio 协议根据条件查询记录。
// dest 必须是指向 slice 的指针，slice 的元素类型必须是 struct 或 *struct。
func (c *OpioClient) Query(ctx context.Context, dest interface{}, tableName string, columns []string, opts *QueryOptions) error {
	// 验证 dest 类型
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("opio: Query 的 dest 参数必须是一个非 nil 指针")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return errors.New("opio: Query 的 dest 参数必须指向一个 slice")
	}
	sliceElemType := rv.Type().Elem()
	if sliceElemType.Kind() != reflect.Struct {
		// 允许元素为指向结构体的指针
		if sliceElemType.Kind() != reflect.Ptr || sliceElemType.Elem().Kind() != reflect.Struct {
			return errors.New("opio: Query 的 dest slice 的元素必须是 struct 或指向 struct 的指针")
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
	req.SetAction(ActionSelect)
	req.SetTableName(tableName)

	// 设置要查询的列 (如果提供了)
	if len(columns) > 0 {
		// 需要将 []string 转换为 opio.Table 或类似结构来设置 PropColumns
		// 或者检查协议是否支持直接传递列名列表
		// 假设需要构建一个只包含列名的 Table
		colsTable := NewTable(tableName, 0) // 容量为 0，因为我们只关心列定义
		for _, colName := range columns {
			// 类型和长度未知，暂时用 VtNull 作为占位符类型
			// 服务器应根据请求的列名返回数据
			colsTable.AddColumn(colName, VtNull, 0)
		}
		// 使用 SetTable 来传递列信息 (这会覆盖 tableName，但 NewTable 内部已设置)
		err := req.SetTable(colsTable)
		if err != nil {
			return fmt.Errorf("opio: Query 设置列到请求失败: %w", err)
		}
		// 如果 SetTable 不合适，可能需要一个专门的 SetColumns 方法或属性
		// req.Set(PropColumns, columns) // 检查协议是否支持直接传递字符串列表
	}

	// 设置选项 (Filters, OrderBy, Limit)
	if opts != nil {
		if len(opts.Filters) > 0 {
			req.SetFilters(opts.Filters)
		}
		if opts.OrderBy != "" {
			req.SetOrderBy(opts.OrderBy)
		}
		if opts.Limit != "" {
			req.SetLimit(opts.Limit)
		}
	}

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
		return fmt.Errorf("opio: Query context 超时或取消: %w", reqCtx.Err())
	case <-done:
		if err != nil {
			return fmt.Errorf("opio: Query 请求/响应失败: %w", err)
		}
	}

	// 检查响应错误
	if res.GetErrNo() != 0 {
		return &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
	}

	// 获取数据集
	dataSet := res.GetDataSet()
	if dataSet == nil {
		return nil // 没有数据集，返回成功 (0 行)
	}
	defer dataSet.Close()

	// --- 处理结果集 (与 QuerySQL 类似) ---
	directSlice := rv // slice 本身
	isPtrElement := sliceElemType.Kind() == reflect.Ptr
	baseStructType := sliceElemType
	if isPtrElement {
		baseStructType = sliceElemType.Elem() // 如果元素是指针，获取其指向的 struct 类型
	}

	// 清空目标 slice
	directSlice.Set(reflect.MakeSlice(directSlice.Type(), 0, 0))

	// 迭代数据集
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			return fmt.Errorf("opio: Query 读取下一行失败: %w", err)
		}
		if !hasNext {
			break // 结束
		}

		newElem := reflect.New(baseStructType)                   // 创建一个指向新 struct 零值的指针
		scanErr := scanRowToStruct(dataSet, newElem.Interface()) // scanRowToStruct 在 models.go 中
		if scanErr != nil {
			if c.Logger != nil {
				c.Logger.Printf("Query: scanRowToStruct 对某行失败: %v", scanErr)
			}
			return fmt.Errorf("opio: Query scanRowToStruct 失败: %w", scanErr)
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

// mapToTable 将单个 map (表示更新或单行) 转换为 opio.Table。
func mapToTable(tableName string, data map[string]interface{}) (*Table, error) {
	if len(data) == 0 {
		return nil, errors.New("opio: 无法从空数据 map 创建表")
	}
	// 使用 NewTable 构造函数设置表名
	table := NewTable(tableName, 1) // 容量为 1，用于单行

	colIndexMap := make(map[string]uint32)

	// 添加列并在单行缓冲区中设置值
	for name, value := range data {
		colType := inferOpioType(value)
		if !table.AddColumnExtension(name, colType, 0, nil) {
			return nil, fmt.Errorf("opio: 添加列 '%s' 失败", name)
		}
		colIndex := table.colCount - 1
		colIndexMap[name] = colIndex

		// 在表的内部缓冲区中设置值
		err := table.SetColumnValue(colIndex, value)
		if err != nil {
			// 收集错误
			if table.errors == nil {
				table.errors = make([]error, 0)
			}
			table.errors = append(table.errors, fmt.Errorf("列 '%s' 的行处理错误: %w", name, err))
		}
	}

	// 绑定包含更新值的单行
	table.BindRow()

	// 检查行处理期间累积的错误
	if tableErrors := table.GetErrors(); len(tableErrors) > 0 {
		errorMessages := ""
		for _, e := range tableErrors {
			errorMessages += e.Error() + "; "
		}
		return nil, fmt.Errorf("opio: 更新表创建期间发生错误: %s", errorMessages)
	}

	return table, nil
}

// Update 使用 Opio 协议更新记录。
func (c *OpioClient) Update(ctx context.Context, tableName string, updates map[string]interface{}, filters []Filter) error {
	if len(updates) == 0 {
		return errors.New("opio: Update 操作需要更新数据")
	}
	if len(filters) == 0 {
		// 为安全起见需要过滤器，类似 GORM 的行为
		return ErrUpdateRequiresFilters // 使用预定义的错误
	}

	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}

	// 将 updates map 转换为单行 opio.Table
	updateTable, err := mapToTable(tableName, updates)
	if err != nil {
		return fmt.Errorf("opio: Update 转换更新数据到表失败: %w", err)
	}

	// 使用带超时的 context
	reqCtx, cancel := c.getContextWithTimeout(ctx)
	defer cancel()

	// 创建请求
	req := conn.NewRequest(nil)
	req.SetService("openPlant") // 假设服务名固定
	req.SetAction(ActionUpdate)
	req.SetFilters(filters) // 设置 WHERE 子句的过滤器
	// 设置包含更新数据的表
	err = req.SetTable(updateTable)
	if err != nil {
		return fmt.Errorf("opio: Update 在请求上设置更新表失败: %w", err)
	}

	// 发送请求并获取响应
	var res *Response
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
		return fmt.Errorf("opio: Update context 超时或取消: %w", reqCtx.Err())
	case <-done:
		if err != nil {
			return fmt.Errorf("opio: Update 请求/响应失败: %w", err)
		}
	}

	// 检查响应错误
	if res.GetErrNo() != 0 {
		return &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
	}

	// TODO: 检查响应是否包含受影响的行数？
	// 当前的 Response 结构似乎没有专用字段。

	return nil // 成功
}

// Delete 使用 Opio 协议删除记录。
func (c *OpioClient) Delete(ctx context.Context, tableName string, filters []Filter) error {
	if len(filters) == 0 {
		// 为安全起见需要过滤器
		return errors.New("opio: Delete 操作需要过滤器以确保安全")
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
	req.SetAction(ActionDelete)
	req.SetTableName(tableName)
	req.SetFilters(filters) // 设置 WHERE 子句的过滤器

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
		return fmt.Errorf("opio: Delete context 超时或取消: %w", reqCtx.Err())
	case <-done:
		if err != nil {
			return fmt.Errorf("opio: Delete 请求/响应失败: %w", err)
		}
	}

	// 检查响应错误
	if res.GetErrNo() != 0 {
		return &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
	}

	// TODO: 检查响应是否包含受影响的行数？

	return nil // 成功
}

// ====================================================================================
// V3 API 操作 (直接访问实时/历史/统计数据)
// ====================================================================================

// ReadRealtime 读取实时数据。
func (c *OpioClient) ReadRealtime(ctx context.Context, values []Value) error {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}
	// 注意：底层 ReadRealtime 可能需要 context 处理超时
	return conn.ReadRealtime(values)
}

// WriteRealtime 写入实时数据。
func (c *OpioClient) WriteRealtime(ctx context.Context, values []Value) error {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}
	// 注意：底层 WriteRealtime 可能需要 context 处理超时
	return conn.WriteRealtime(values)
}

// ReadArchive 读取历史数据。
func (c *OpioClient) ReadArchive(ctx context.Context, ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Archive, error) {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return nil, ErrConnectionClosed
	}
	// 注意：底层 ReadArchive 可能需要 context 处理超时
	return conn.ReadArchive(ids, mode, begin, end, interval)
}

// WriteArchive 写入历史数据。
func (c *OpioClient) WriteArchive(ctx context.Context, archives []*Archive, cache bool) error {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return ErrConnectionClosed
	}
	// 注意：底层 WriteArchive 可能需要 context 处理超时
	return conn.WriteArchive(archives, cache)
}

// ReadStat 读取统计数据。
func (c *OpioClient) ReadStat(ctx context.Context, ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Stat, error) {
	c.opioConnMu.RLock()
	conn := c.opioConn
	c.opioConnMu.RUnlock()
	if conn == nil {
		return nil, ErrConnectionClosed
	}
	// 注意：底层 ReadStat 可能需要 context 处理超时
	return conn.ReadStat(ids, mode, begin, end, interval)
}

// ====================================================================================
// 订阅 API
// ====================================================================================

// Subscribe 订阅指定表的数据变更。
func (c *OpioClient) Subscribe(ctx context.Context, tableName string, keyName string, keys interface{}, opts *SubscribeOptions) (*Subscription, error) {
	c.opioConnMu.RLock()
	if c.opioConn == nil {
		c.opioConnMu.RUnlock()
		return nil, ErrConnectionClosed
	}
	// 创建底层的订阅对象
	sub, err := c.opioConn.NewSubscribe(tableName)
	c.opioConnMu.RUnlock()

	if err != nil {
		return nil, fmt.Errorf("创建底层订阅失败: %w", err)
	}

	// 处理订阅选项
	useSnapshot := false
	eventChanBuffer := 100 // 默认事件通道缓冲区大小
	if opts != nil {
		useSnapshot = opts.Snapshot
		if opts.EventChanBuffer > 0 {
			eventChanBuffer = opts.EventChanBuffer
		}
	}
	// 设置是否获取快照
	if err := sub.SetSnapshot(useSnapshot); err != nil {
		sub.Close() // 设置失败时关闭底层订阅
		return nil, fmt.Errorf("设置快照选项失败: %w", err)
	}

	// 创建事件通道和关闭信号通道
	eventCh := make(chan SubscriptionEvent, eventChanBuffer)
	closedCh := make(chan struct{})
	// 创建用于此特定订阅的 Context，以便可以独立取消
	subCtx, cancelFn := context.WithCancel(ctx)

	// 创建 Subscription 包装对象
	subscription := &Subscription{
		client:   c,
		sub:      sub,
		eventCh:  eventCh,
		cancelFn: cancelFn,
		closed:   closedCh,
	}

	// 定义处理服务器响应的回调函数
	callback := func(res *Response) {
		// 检查订阅 context 是否已被取消
		select {
		case <-subCtx.Done():
			// 如果需要可以记录日志: log.Println("订阅 context 已取消，跳过回调处理")
			return
		default:
		}

		// 处理响应中的服务器级错误
		if res.GetErrNo() != 0 {
			serverErr := &OpioServerError{Code: res.GetErrNo(), Message: res.GetError()}
			errEvent := SubscriptionEvent{Err: fmt.Errorf("opio.Subscription: 收到服务器错误: %w", serverErr)}
			// 尝试发送错误事件，但如果 context 已取消则不无限期阻塞
			select {
			case eventCh <- errEvent:
			case <-subCtx.Done():
			}
			return // 发生服务器错误时停止处理此响应
		}

		// 获取数据集
		dataSet := res.GetDataSet()
		if dataSet == nil {
			// 这种情况可能发生，或许记录一下？
			// log.Println("收到带有 nil 数据集的订阅响应")
			return // 没有数据需要处理
		}
		defer dataSet.Close() // 确保关闭数据集

		columns := dataSet.GetColumns()
		// 迭代数据集中的行
		for {
			hasNext, err := dataSet.Next()
			if err != nil {
				// 迭代行时出错
				errEvent := SubscriptionEvent{Err: fmt.Errorf("读取订阅数据行失败: %w", err)}
				select {
				case eventCh <- errEvent:
				case <-subCtx.Done():
				}
				return // 迭代错误时停止处理此数据集
			}
			if !hasNext {
				break // 没有更多行
			}

			// 处理当前行
			rowMap := make(map[string]interface{})
			var rowErr error // 跟踪行内的错误
			for i, col := range columns {
				val, err := dataSet.GetValue(uint32(i))
				if err != nil {
					// 如果配置了 logger，则记录错误
					if c.Logger != nil {
						c.Logger.Printf("获取列 %s (索引 %d) 的值时出错: %v", col.name, i, err)
					}
					rowMap[col.name] = nil // 暂时仍设置为 nil，但已记录错误
					if rowErr == nil {     // 存储此行遇到的第一个错误
						rowErr = fmt.Errorf("获取列 %s 的值时出错: %w", col.name, err)
					}
				} else {
					rowMap[col.name] = val
				}
			}

			// 创建事件（如果 GetValue 失败，可能带有错误）
			dataEvent := SubscriptionEvent{Data: rowMap, Err: rowErr}
			select {
			case eventCh <- dataEvent: // 发送事件
			case <-subCtx.Done():
				// 尝试发送时 context 已取消
				return
			}
		}
	}
	// --- 回调函数结束 ---

	// 初始化底层订阅
	err = sub.InitSubscribe(keys, keyName, callback)
	if err != nil {
		// 如果 InitSubscribe 失败则进行清理
		sub.Close()
		cancelFn()
		close(closedCh)
		// 依赖清理 goroutine 的 defer 来关闭 eventCh
		// close(eventCh) // 已移除
		return nil, fmt.Errorf("初始化底层订阅失败: %w", err)
	}

	// 清理 goroutine，用于在 context 取消或显式关闭时进行清理
	go func() {
		defer close(eventCh) // 确保此 goroutine 退出时关闭事件通道
		select {
		case <-ctx.Done(): // 等待原始 context 完成
			// log.Println("原始 context 已取消，正在关闭订阅。")
			subscription.Close() // 调用包装对象的 Close 方法
		case <-closedCh: // 等待 Subscription.Close() 被显式调用
			// log.Println("订阅已被显式关闭。")
			// 此处无需再次调用 subscription.Close()
		}
	}()

	return subscription, nil
}

// ====================================================================================
// 辅助函数 (已移至 models.go)
// ====================================================================================
// scanRowToStruct 和 inferOpioType 现在位于 models.go 中
