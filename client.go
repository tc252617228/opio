package opio

import (
	"context"
	"errors"
	"fmt"
	"reflect" // 导入反射包
	"strconv" // 导入字符串转换包
	"strings" // 导入字符串处理包
	"time"
	// "github.com/tc252617228/opio/internal/utils" // 可能需要内部工具包
)

// ====================================================================================
// Client Definition and Basic Operations
// ====================================================================================

// Client 代表与 OpenPlant 服务的连接，封装了底层操作。
// 它提供了 V2 风格的通用 CRUD、V3 风格的时序数据操作、SQL 执行和实时数据订阅功能。
type Client struct {
	conn            *IOConnect // 底层连接对象
	compressionMode byte       // 当前连接的压缩模式
	// 可以添加其他配置选项，例如日志记录器等
}

// Connect 建立到 OpenPlant 服务的新连接。
// ctx: 用于控制连接生命周期的上下文。当此上下文被取消时，客户端将尝试自动关闭。
// host: 服务器主机名或 IP 地址。
// port: 服务器端口号。
// user: 用户名。
// pass: 密码。
// timeout: 连接尝试的超时时间。
// 返回一个 Client 实例或错误。
func Connect(ctx context.Context, host string, port int, user string, pass string, timeout time.Duration) (*Client, error) {
	// 注意：原始的 Init 函数不接受 context，超时是在内部的 DialTimeout 中处理的。
	// 这里暂时选择重用现有的 Init 函数。
	op, err := Init(host, port, int(timeout.Seconds()), user, pass) // 重用现有的 Init 函数进行连接
	if err != nil {
		return nil, fmt.Errorf("无法初始化连接: %w", err)
	}

	// 可以在这里添加一个 PING 操作或检查连接是否真正活跃。
	// 例如:
	// if !op.Alive() {
	//  op.Close() // 如果不活跃则关闭连接
	//  return nil, errors.New("连接建立后不活跃")
	// }

	client := &Client{conn: op} // 创建 Client 实例

	// 启动一个 goroutine，使用传入的 context 来监听取消信号
	go func() {
		<-ctx.Done()   // 等待 context 被取消
		client.Close() // 当 context 取消时，自动关闭客户端连接
	}()

	// 设置默认压缩模式为不压缩。
	client.compressionMode = ZIP_MODEL_Uncompressed // 使用 const.go 中定义的常量
	// 如果底层的 IOConnect 支持获取当前压缩模式，可以在这里同步状态。
	// 例如: currentMode, err := op.GetCompressModel(); if err == nil { client.compressionMode = currentMode }

	return client, nil // 返回创建的客户端实例和 nil 错误
}

// Close 关闭与 OpenPlant 服务的连接。
// 如果连接已关闭或从未建立，则返回错误。
func (c *Client) Close() error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭") // 防止重复关闭或对空连接操作
	}
	err := c.conn.Close() // 调用底层 IOConnect 的 Close 方法
	c.conn = nil          // 将底层连接设为 nil，标记客户端为已关闭状态
	if err != nil {
		return fmt.Errorf("关闭连接时出错: %w", err) // 如果底层关闭出错，包装并返回错误
	}
	return nil // 成功关闭，返回 nil
}

// Ping 检查与服务器的连接是否仍然活跃。
// ctx: 用于控制 Ping 操作的上下文（当前实现未使用，但保留以备将来扩展）。
// 如果连接不活跃或已关闭，则返回错误。
func (c *Client) Ping(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}

	// 原始的 IOConnect.Alive 方法似乎有自己的内部超时逻辑。
	// 未来可以考虑如何将这里的 context 与底层操作结合，
	// 或者在 Client 层面实现带 context 的 Ping 逻辑（可能需要发送一个 ECHO 请求）。
	// 当前暂时直接调用底层的 Alive 方法。
	if !c.conn.Alive() {
		return errors.New("连接不活跃") // 如果底层连接报告不活跃，返回错误
	}
	return nil // 连接活跃，返回 nil
}

// SetCompression 设置客户端连接的压缩模式。
// model: 压缩模式常量 (例如 opio.ZIP_MODEL_Uncompressed, opio.ZIP_MODEL_Frame)。类型为 byte。
// 如果客户端未连接，则返回错误。
func (c *Client) SetCompression(model byte) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}
	// 调用底层 IOConnect 的 SetCompressModel 方法
	err := c.conn.SetCompressModel(model)
	if err == nil {
		c.compressionMode = model // 如果设置成功，更新客户端实例中保存的压缩模式状态
	}
	return err // 返回底层方法可能产生的错误
}

// ====================================================================================
// V2 Query (Map Interface)
// ====================================================================================

// QueryOptions 定义 V2 风格数据查询时可以使用的选项。
type QueryOptions struct {
	DB      string   // 指定要查询的数据库名称 (如果需要)
	Filters []Filter // 查询过滤器列表
	OrderBy string   // 排序条件 (例如 "column_name ASC")
	Limit   string   // 分页限制 (例如 "10" 或 "10, 20")
	// 可以根据 request.go 中的 Set* 方法添加其他相关选项，如 Key, Indexes 等。
}

// QueryResult 代表 V2 风格查询操作返回的结果集。
// 这是一个通用的结构，用于存储列信息和行数据 (map 形式)。
type QueryResult struct {
	Columns []Column                 // 列定义信息列表 (来自 table.go/columns.go)
	Rows    []map[string]interface{} // 行数据列表，每行是一个 map，键是列名，值是列数据。
}

// Query 执行 V2 风格的结构化数据查询，返回 map 形式的结果。
// ctx: 用于控制操作的上下文。
// tableName: 要查询的表名。
// columns: 要查询的列名列表。如果为空或 `["*"]`，则查询所有列。
// opts: 查询选项，如过滤器、排序、分页等。
// 返回包含结果的 QueryResult 指针或错误。
// 此方法封装了构建 Request、发送请求和解析 Response 的过程。
func (c *Client) Query(ctx context.Context, tableName string, columns []string, opts *QueryOptions) (*QueryResult, error) {
	if c.conn == nil {
		return nil, errors.New("客户端未连接或已关闭")
	}

	// 定义一个结构体，用于在 goroutine 和主线程之间传递结果和错误
	type queryResultWithError struct {
		result *QueryResult
		err    error
	}
	done := make(chan queryResultWithError, 1) // 创建带缓冲的通道

	// 启动一个 goroutine 来执行可能阻塞的网络操作
	go func() {
		// --- 在 goroutine 中执行实际的查询逻辑 ---
		req := c.conn.NewRequest(nil) // 创建一个新的请求对象
		defer req.Reset()             // 确保请求对象在使用后被重置，以便复用

		// 设置基本的请求属性
		req.SetService("openplant") // 假设服务名总是 "openplant"
		req.SetAction(ActionSelect) // 设置操作类型为查询
		req.SetTableName(tableName) // 设置要查询的表名

		// --- 处理要查询的列 ---
		// 创建一个临时的 Table 对象来承载要查询的列信息
		queryTable := NewTable(tableName, 0) // 容量设为 0，因为我们只用它定义列
		if len(columns) == 0 || (len(columns) == 1 && columns[0] == "*") {
			// 如果请求查询所有列 ("*")
			// 需要一种方式来告诉服务器查询所有列。
			// 假设通过添加一个名为 "*" 的特殊列来实现。
			queryTable.AddColumn("*", VtNull, 0) // 类型和长度可能不重要
		} else {
			// 如果指定了具体的列名
			for _, colName := range columns {
				// 添加指定的列名。查询时通常不需要指定类型和长度，服务器会返回。
				queryTable.AddColumn(colName, VtNull, 0) // 使用 VtNull 作为占位符类型？
			}
		}
		// 将包含列定义的 Table 对象设置到请求的 PropColumns 属性中
		// 注意：原始的 SetTable 方法同时设置了 PropTable 和 PropColumns。
		// 这里假设有一个 Set 方法可以直接设置属性，或者需要调整 SetTable/SetColumns 的逻辑。
		req.Set(PropColumns, queryTable) // 假设这样可以将列定义传递给请求

		// --- 应用查询选项 ---
		if opts != nil {
			if opts.DB != "" {
				req.SetDB(opts.DB) // 设置数据库名
			}
			if len(opts.Filters) > 0 {
				// 假设 Filter 结构与底层兼容
				req.SetFilters(opts.Filters) // 设置过滤器
			}
			if opts.OrderBy != "" {
				req.SetOrderBy(opts.OrderBy) // 设置排序条件
			}
			if opts.Limit != "" {
				req.SetLimit(opts.Limit) // 设置分页限制
			}
			// 在此可以添加设置其他选项的逻辑...
		}

		// --- 发送请求 ---
		// 对于查询操作，通常只发送请求头 (包含属性)，没有数据体。
		err := req.WriteAndFlush() // 发送请求头并刷新缓冲区
		if err != nil {
			// 发送失败，将错误发送到通道
			done <- queryResultWithError{result: nil, err: fmt.Errorf("发送查询请求失败: %w", err)}
			return
		}

		// --- 获取并处理响应 ---
		// GetResponse 方法会读取响应头和可能的数据体 (DataSet)
		res, err := req.GetResponse()
		if err != nil {
			// 获取响应失败，将错误发送到通道
			done <- queryResultWithError{result: nil, err: fmt.Errorf("获取查询响应失败: %w", err)}
			return
		}
		// 注意：Response 对象本身通常不需要关闭，但其包含的 DataSet 需要关闭。

		// 检查响应中是否包含错误信息
		if res.GetErrNo() != 0 {
			// 服务器返回错误，将错误信息发送到通道
			done <- queryResultWithError{result: nil, err: fmt.Errorf("查询失败: %s (错误码: %d)", res.GetError(), res.GetErrNo())}
			return
		}

		// 从响应中提取数据集 (DataSet)
		dataSet := res.GetDataSet()
		if dataSet == nil {
			// 如果没有返回数据集 (可能是空结果或实现问题)
			// 返回一个空的 QueryResult
			done <- queryResultWithError{result: &QueryResult{Columns: []Column{}, Rows: []map[string]interface{}{}}, err: nil}
			return
		}
		defer dataSet.Close() // 确保数据集在使用完毕后关闭

		// --- 构建 QueryResult ---
		result := &QueryResult{}
		result.Columns = dataSet.GetColumns()           // 从数据集中获取列定义信息
		result.Rows = make([]map[string]interface{}, 0) // 初始化行数据切片

		// 迭代读取数据集中的每一行
		for {
			hasNext, err := dataSet.Next() // 移动到下一行
			if err != nil {
				// 在迭代过程中发生错误 (例如网络中断)
				// 关闭数据集，并将已读取的部分数据和错误发送到通道
				dataSet.Close()
				done <- queryResultWithError{result: result, err: fmt.Errorf("读取数据集下一行时出错: %w", err)}
				return
			}
			if !hasNext {
				// 没有更多行了，退出循环
				break
			}

			// 处理当前行的数据
			rowMap := make(map[string]interface{}) // 为当前行创建一个 map
			for i, col := range result.Columns {   // 遍历所有列
				// 根据列索引获取当前行的列值
				// 假设 DataSet 提供了 GetValue(columnIndex) 方法
				val, err := dataSet.GetValue(uint32(i)) // 假设 GetValue 返回 (interface{}, error)
				if err != nil {
					// 获取列值时发生错误
					// 可以选择记录警告、跳过该列或中断整个过程
					fmt.Printf("警告: 获取行 %d 列 %d (%s) 的值失败: %v\n", len(result.Rows), i, col.name, err)
					rowMap[col.name] = nil // 将该列的值设为 nil 或其他标记
				} else {
					rowMap[col.name] = val // 将获取到的值存入 map
				}
			}
			result.Rows = append(result.Rows, rowMap) // 将当前行的 map 添加到结果切片中
		}

		// 所有行都已成功处理，将最终结果和 nil 错误发送到通道
		// dataSet 会在 defer 语句中关闭
		done <- queryResultWithError{result: result, err: nil}
		// --- goroutine 结束 ---
	}()

	// 使用 select 语句等待 goroutine 完成或外部 context 被取消
	select {
	case <-ctx.Done():
		// 如果外部 context 被取消或超时
		return nil, fmt.Errorf("查询操作被取消或超时: %w", ctx.Err())
	case res := <-done:
		// 如果 goroutine 完成
		return res.result, res.err // 返回 goroutine 计算得到的结果和错误
	}
}

// Scan 将 QueryResult 中的行数据映射到目标结构体切片。
// dest: 必须是一个指向结构体切片的指针 (例如 *[]MyStruct)。
// 映射规则:
// 1. 结构体字段优先使用 `opio:"column_name"` 标签匹配列名。
// 2. 其次使用 `db:"column_name"` 标签匹配列名 (为了兼容性)。
// 3. 如果没有标签，则尝试将字段名 (不区分大小写) 与列名 (不区分大小写) 匹配。
// 4. 带有 `"-"` 标签的字段将被忽略。
// 5. 未导出的字段将被忽略。
// 6. 支持将数据库 NULL 值映射到 Go 的指针类型 (结果为 nil)。
// 7. 支持基本类型之间的自动转换 (例如 int 到 string, string 到 int 等)。
// 如果映射失败或类型不兼容，将返回错误。
func (qr *QueryResult) Scan(dest interface{}) error {
	// 1. 验证目标类型是否为指向切片的非空指针
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return errors.New("Scan 的目标必须是一个非空的指针")
	}
	sliceVal := destVal.Elem() // 获取指针指向的实际值
	if sliceVal.Kind() != reflect.Slice {
		return errors.New("Scan 的目标必须指向一个切片")
	}

	// 2. 获取切片元素的基础类型（应该是结构体）
	structType := sliceVal.Type().Elem() // 获取切片元素的类型
	if structType.Kind() != reflect.Struct {
		return errors.New("Scan 的目标切片元素必须是结构体")
	}

	// 3. 准备映射关系
	// a) 列名 (小写) 到 QueryResult 中列索引的映射
	colNameToIndex := make(map[string]int)
	for i, col := range qr.Columns {
		colNameToIndex[strings.ToLower(col.GetName())] = i // 使用小写列名作为键
	}

	// b) 目标结构体字段名或标签名 (小写) 到结构体字段索引的映射
	fieldMap := make(map[string]int)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if !field.IsExported() { // 跳过未导出的字段 (私有字段)
			continue
		}
		// 优先使用 "opio" 标签
		tag := field.Tag.Get("opio")
		if tag == "" {
			tag = field.Tag.Get("db") // 其次使用 "db" 标签 (兼容常用库)
		}
		if tag == "-" { // 如果标签是 "-", 则忽略此字段
			continue
		}
		if tag == "" {
			// 没有标签，使用字段名本身 (转换为小写) 作为映射键
			fieldMap[strings.ToLower(field.Name)] = i
		} else {
			// 有标签，使用标签值 (转换为小写) 作为映射键
			fieldMap[strings.ToLower(tag)] = i
		}
	}

	// 4. 迭代查询结果的每一行，并填充到新的结构体实例中
	newSlice := reflect.MakeSlice(sliceVal.Type(), len(qr.Rows), len(qr.Rows)) // 创建一个新的目标类型切片，容量足够

	for rowIndex, rowMap := range qr.Rows { // 遍历查询结果的每一行 (map)
		newStruct := reflect.New(structType).Elem() // 为每一行创建一个新的结构体实例

		for mapKey, fieldIndex := range fieldMap { // 遍历结构体字段映射 (mapKey 是小写的字段名或标签名)
			// 查找当前结构体字段对应的 QueryResult 列索引
			colIndex, colExists := colNameToIndex[mapKey]
			if !colExists {
				// 如果结构体字段在查询结果中没有对应的列，则跳过该字段
				continue
			}

			// 获取原始的列名 (大小写敏感，用于从 rowMap 中取值)
			originalColName := qr.Columns[colIndex].GetName()

			// 从当前行数据 (rowMap) 中获取对应列的值
			mapValue, valueExists := rowMap[originalColName]
			if !valueExists || mapValue == nil {
				// 如果列值不存在或为 nil，则跳过该字段 (保持结构体字段为零值)
				continue
			}

			// 获取目标结构体中对应的字段
			structField := newStruct.Field(fieldIndex)
			if !structField.CanSet() {
				continue // 如果字段不可设置 (例如未导出但错误地包含在 fieldMap 中)，跳过
			}

			// --- 进行类型转换和赋值 ---
			mapValueReflect := reflect.ValueOf(mapValue) // 获取值的反射表示
			targetType := structField.Type()             // 获取目标结构体字段的类型

			// 特殊处理：目标是指针类型
			if targetType.Kind() == reflect.Ptr && mapValueReflect.Kind() != reflect.Ptr {
				// 如果目标字段是指针，而源值不是指针
				elemType := targetType.Elem() // 获取指针指向的元素类型
				// 检查源值类型是否可以直接赋值或转换为目标元素类型
				if mapValueReflect.Type().AssignableTo(elemType) {
					// 创建一个指向源值副本的新指针，并赋值给目标字段
					ptr := reflect.New(elemType)
					ptr.Elem().Set(mapValueReflect)
					structField.Set(ptr)
				} else if mapValueReflect.Type().ConvertibleTo(elemType) {
					// 创建一个指向转换后值的新指针，并赋值给目标字段
					ptr := reflect.New(elemType)
					ptr.Elem().Set(mapValueReflect.Convert(elemType))
					structField.Set(ptr)
				} else {
					// 尝试通过 assignWithConversion 进行更复杂的转换
					tempVal := reflect.New(elemType).Elem() // 创建临时变量以接收转换结果
					err := assignWithConversion(tempVal, mapValueReflect)
					if err != nil {
						// 转换失败，返回详细错误信息
						return fmt.Errorf("无法将列 '%s' 的值 (%T: %v) 赋给结构体字段 %s (*%s): %w",
							originalColName, mapValue, mapValue, structType.Field(fieldIndex).Name, elemType.String(), err)
					}
					// 将转换后的值的地址赋给目标指针字段
					structField.Set(tempVal.Addr())
				}
			} else if targetType.Kind() != reflect.Ptr && mapValueReflect.Kind() == reflect.Ptr {
				// 特殊处理：源是指针类型，而目标不是
				if mapValueReflect.IsNil() {
					continue // 如果源指针为 nil，则跳过 (目标字段保持零值)
				}
				mapValueReflect = mapValueReflect.Elem() // 解引用源指针
				// 解引用后，再次检查类型兼容性或可转换性
				if mapValueReflect.Type().AssignableTo(targetType) {
					structField.Set(mapValueReflect)
				} else if mapValueReflect.Type().ConvertibleTo(targetType) {
					structField.Set(mapValueReflect.Convert(targetType))
				} else {
					// 尝试通过 assignWithConversion 进行转换
					err := assignWithConversion(structField, mapValueReflect)
					if err != nil {
						return fmt.Errorf("无法将列 '%s' 的解引用值 (%T: %v) 赋给结构体字段 %s (%s): %w",
							originalColName, mapValueReflect.Interface(), mapValueReflect.Interface(), structType.Field(fieldIndex).Name, targetType.String(), err)
					}
				}
			} else if mapValueReflect.Type().AssignableTo(targetType) {
				// 类型可以直接赋值 (包括两者都是相同类型的指针，或都不是指针且类型兼容)
				structField.Set(mapValueReflect)
			} else if mapValueReflect.Type().ConvertibleTo(targetType) {
				// 类型可以通过 Go 的 Convert 方法转换
				convertedValue := mapValueReflect.Convert(targetType)
				structField.Set(convertedValue)
			} else {
				// 尝试使用自定义的转换逻辑
				err := assignWithConversion(structField, mapValueReflect)
				if err != nil {
					// 自定义转换也失败，返回错误
					return fmt.Errorf("无法将列 '%s' 的值 (%T: %v) 赋给结构体字段 %s (%s): %w",
						originalColName, mapValue, mapValue, structType.Field(fieldIndex).Name, targetType.String(), err)
				}
			}
		}
		// 将填充好的结构体实例设置到新创建的切片的对应索引位置
		newSlice.Index(rowIndex).Set(newStruct)
	}

	// 5. 最后，将新创建并填充好的切片赋值给传入的目标指针 `dest` 指向的切片
	sliceVal.Set(newSlice)

	return nil // Scan 成功完成
}

// assignWithConversion 尝试在类型不直接兼容或可转换时，进行更复杂的类型转换并赋值。
// targetField: 目标结构体字段的 reflect.Value。
// sourceValue: 源数据的 reflect.Value。
// 这是 Scan 的辅助函数，可以扩展以处理更多自定义的转换规则。
func assignWithConversion(targetField reflect.Value, sourceValue reflect.Value) error {
	targetType := targetField.Type() // 目标类型
	sourceType := sourceValue.Type() // 源类型

	// 如果源值无效 (例如来自 nil 接口) 或源是指针且为 nil，则无需转换，直接返回
	if !sourceValue.IsValid() || (sourceValue.Kind() == reflect.Ptr && sourceValue.IsNil()) {
		return nil
	}
	// 如果源值是指针，先获取其指向的元素值进行后续处理
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
		sourceType = sourceValue.Type() // 更新源类型为解引用后的类型
	}

	// --- 添加具体的转换规则 ---

	// 示例：尝试将各种数字类型转换为字符串
	if targetType.Kind() == reflect.String {
		switch sourceType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			targetField.SetString(strconv.FormatInt(sourceValue.Int(), 10)) // 整型转字符串
			return nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			targetField.SetString(strconv.FormatUint(sourceValue.Uint(), 10)) // 无符号整型转字符串
			return nil
		case reflect.Float32, reflect.Float64:
			targetField.SetString(strconv.FormatFloat(sourceValue.Float(), 'f', -1, sourceType.Bits())) // 浮点型转字符串
			return nil
		case reflect.Bool:
			targetField.SetString(strconv.FormatBool(sourceValue.Bool())) // 布尔型转字符串 ("true" 或 "false")
			return nil
		}
	}

	// 示例：尝试将字符串转换为各种数字类型或布尔类型
	if sourceType.Kind() == reflect.String {
		sourceStr := sourceValue.String()
		switch targetType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if i, err := strconv.ParseInt(sourceStr, 10, targetType.Bits()); err == nil {
				if targetField.OverflowInt(i) { // 检查是否溢出
					return fmt.Errorf("值 '%s' 对于类型 %s 溢出", sourceStr, targetType.String())
				}
				targetField.SetInt(i)
				return nil
			} else {
				return fmt.Errorf("无法将字符串 '%s' 解析为 %s: %w", sourceStr, targetType.String(), err)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if u, err := strconv.ParseUint(sourceStr, 10, targetType.Bits()); err == nil {
				if targetField.OverflowUint(u) { // 检查是否溢出
					return fmt.Errorf("值 '%s' 对于类型 %s 溢出", sourceStr, targetType.String())
				}
				targetField.SetUint(u)
				return nil
			} else {
				return fmt.Errorf("无法将字符串 '%s' 解析为 %s: %w", sourceStr, targetType.String(), err)
			}
		case reflect.Float32, reflect.Float64:
			if f, err := strconv.ParseFloat(sourceStr, targetType.Bits()); err == nil {
				if targetField.OverflowFloat(f) { // 检查是否溢出
					return fmt.Errorf("值 '%s' 对于类型 %s 溢出", sourceStr, targetType.String())
				}
				targetField.SetFloat(f)
				return nil
			} else {
				return fmt.Errorf("无法将字符串 '%s' 解析为 %s: %w", sourceStr, targetType.String(), err)
			}
		case reflect.Bool:
			if b, err := strconv.ParseBool(sourceStr); err == nil {
				targetField.SetBool(b)
				return nil
			} else {
				// 尝试更宽松的布尔值判断 (例如 "1", "0", "yes", "no")
				lowerStr := strings.ToLower(sourceStr)
				if lowerStr == "1" || lowerStr == "true" || lowerStr == "yes" || lowerStr == "on" {
					targetField.SetBool(true)
					return nil
				}
				if lowerStr == "0" || lowerStr == "false" || lowerStr == "no" || lowerStr == "off" {
					targetField.SetBool(false)
					return nil
				}
				// 如果宽松判断也失败，则返回原始的 ParseBool 错误
				return fmt.Errorf("无法将字符串 '%s' 解析为 bool: %w", sourceStr, err)
			}
		}
	}

	// 示例：处理时间转换 (目标是 time.Time)
	timeType := reflect.TypeOf(time.Time{})
	if targetType == timeType {
		if sourceType == timeType { // 源已经是 time.Time
			targetField.Set(sourceValue)
			return nil
		} else if sourceType.Kind() == reflect.String { // 源是字符串
			// 尝试多种常用时间格式进行解析
			formats := []string{
				time.RFC3339, time.RFC3339Nano, // ISO 8601 格式
				"2006-01-02 15:04:05.999999999 -0700 MST", // Go 默认 String() 格式
				"2006-01-02 15:04:05",                     // 常见的数据库时间戳格式
				"2006-01-02",                              // 仅日期格式
			}
			sourceStr := sourceValue.String()
			parsed := false
			for _, format := range formats {
				if t, err := time.Parse(format, sourceStr); err == nil {
					targetField.Set(reflect.ValueOf(t)) // 解析成功，赋值
					parsed = true
					break
				}
			}
			if parsed {
				return nil
			}
			// 如果标准格式解析失败，尝试将其解析为 Unix 时间戳字符串 (秒)
			if ts, err := strconv.ParseInt(sourceStr, 10, 64); err == nil {
				targetField.Set(reflect.ValueOf(time.Unix(ts, 0))) // 使用 Unix 时间戳创建 time.Time
				return nil
			}
			// 所有尝试都失败
			return fmt.Errorf("无法将字符串 '%s' 解析为 time.Time", sourceStr)
		} else if sourceType.Kind() == reflect.Int32 || sourceType.Kind() == reflect.Int64 { // 源是整数
			// 假设源是 Unix 时间戳 (秒)
			targetField.Set(reflect.ValueOf(time.Unix(sourceValue.Int(), 0)))
			return nil
		}
		// 可以添加对其他源类型 (如 float 表示时间戳) 的支持
	}

	// 示例：处理 []byte 和 string 之间的转换
	if targetType.Kind() == reflect.String && sourceType == reflect.TypeOf([]byte{}) { // []byte -> string
		targetField.SetString(string(sourceValue.Bytes()))
		return nil
	}
	if targetType == reflect.TypeOf([]byte{}) && sourceType.Kind() == reflect.String { // string -> []byte
		targetField.SetBytes([]byte(sourceValue.String()))
		return nil
	}

	// --- 在此添加更多自定义转换规则 ---
	// 例如，处理 JSON 字符串到结构体/map 的转换等

	// 如果以上所有转换规则都不匹配，则返回不支持转换的错误
	return fmt.Errorf("不支持从类型 %s 到 %s 的转换", sourceType.String(), targetType.String())
}

// ====================================================================================
// V2 Modify (Map Interface)
// ====================================================================================

// Insert 向指定表插入多行数据 (使用 map 接口)。
// ctx: 用于控制操作的上下文。
// tableName: 要插入数据的目标表名。
// data: 一个 map 切片，每个 map 代表一行数据，键是列名，值是对应的列值。
// 注意：
// - 此实现会从 `data` 的第一行推断列名和数据类型。它假设所有行具有相同的结构。
// - 对于更复杂或类型不一致的数据，可能需要提供显式的列定义或采用不同的处理方式。
// 如果插入成功，返回 nil，否则返回错误。
func (c *Client) Insert(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}
	if len(data) == 0 {
		return errors.New("没有要插入的数据") // 如果没有数据，直接返回错误
	}

	done := make(chan error, 1) // 用于 goroutine 通信的通道

	go func() {
		// --- 在 goroutine 中执行实际的插入逻辑 ---
		req := c.conn.NewRequest(nil) // 创建请求对象
		defer req.Reset()             // 确保重置

		// 设置基本请求属性
		req.SetService("openplant") // 服务名
		req.SetAction(ActionInsert) // 操作类型为插入
		req.SetTableName(tableName) // 目标表名

		// --- 创建并准备用于插入的 Table 对象 ---
		insertTable := NewTable(tableName, uint(len(data))) // 创建 Table，容量为数据行数

		// 从第一行数据推断列名和 opio 数据类型
		firstRow := data[0]
		columnNames := make([]string, 0, len(firstRow)) // 存储列名顺序
		columnTypes := make(map[string]int)             // 存储列名到 opio 类型 (Vt*) 的映射

		for colName, val := range firstRow { // 遍历第一行的列
			columnNames = append(columnNames, colName) // 记录列名
			// 推断 opio 类型 (这是一个简化的辅助函数)
			vtType := inferOpioType(val)
			if vtType == VtNull && val != nil { // 如果推断为 Null 但实际值不是 nil，则使用更通用的 VtObject
				vtType = VtObject
			}
			columnTypes[colName] = vtType // 存储推断的类型
			// 将列添加到 Table 定义中，长度设为 0 (让 SetColumnValue 自动处理或根据类型决定)
			insertTable.AddColumn(colName, vtType, 0)
		}

		// --- 填充 Table 的行数据 ---
		for _, rowMap := range data { // 遍历每一行要插入的数据
			for i, colName := range columnNames { // 按照推断的列顺序填充
				value, exists := rowMap[colName] // 获取当前行对应列的值
				if !exists {
					// 如果当前行缺少某个列，可以选择设置为空值或返回错误
					// 这里选择尝试设置为空值
					err := insertTable.SetColumnEmpty(uint32(i))
					if err != nil {
						// 记录设置空值时可能出现的错误
						fmt.Printf("警告: 设置行 %d 列 %d (%s) 为空时出错: %v\n", insertTable.RowCount(), i, colName, err)
					}
					continue // 继续处理下一列
				}
				// 使用 SetColumnValue 将值设置到 Table 的当前行、指定列索引
				err := insertTable.SetColumnValue(uint32(i), value)
				if err != nil {
					// 处理设置列值时可能发生的错误 (例如类型不匹配)
					// 可以选择记录错误并继续，或者立即中断并返回错误
					// 这里选择记录警告并继续，最终错误会在 SetTable 时统一检查
					fmt.Printf("警告: 设置行 %d 列 %d (%s) 的值 (%v) 时出错: %v\n", insertTable.RowCount(), i, colName, value, err)
				}
			}
			// 当前行的所有列都已设置 (或尝试设置)，绑定当前行数据到 Table
			insertTable.BindRow()
			// 可以在这里检查 insertTable.GetErrors() 来提前发现错误，但 SetTable 也会进行检查
		}

		// --- 将填充好的 Table 设置到 Request 对象中 ---
		err := req.SetTable(insertTable) // SetTable 会进行内部验证，例如检查是否有错误
		if err != nil {
			// 如果 SetTable 失败 (例如内部有错误)，将错误发送到通道
			done <- fmt.Errorf("设置插入表时出错: %w", err)
			return
		}

		// --- 发送请求 ---
		// 插入操作需要发送请求头 (属性) 和请求体 (Table 数据)
		// 1. 发送请求头
		err = req.Write()
		if err != nil {
			done <- fmt.Errorf("发送插入请求头失败: %w", err)
			return
		}
		// 2. 发送请求体 (Table 数据)
		err = req.WriteContent(insertTable)
		if err != nil {
			done <- fmt.Errorf("发送插入数据体失败: %w", err)
			return
		}
		// 3. 刷新网络缓冲区，确保数据发送出去
		req.Flush()

		// --- 获取并处理响应 ---
		res, err := req.GetResponse() // 获取服务器的响应
		if err != nil {
			done <- fmt.Errorf("获取插入响应失败: %w", err)
			return
		}

		// 检查响应中是否包含错误
		if res.GetErrNo() != 0 {
			done <- fmt.Errorf("插入失败: %s (错误码: %d)", res.GetError(), res.GetErrNo())
			return
		}

		// 如果没有错误，表示插入成功
		done <- nil
		// --- goroutine 结束 ---
	}()

	// 使用 select 等待 goroutine 完成或 context 取消
	select {
	case <-ctx.Done():
		// 如果外部 context 被取消或超时
		return fmt.Errorf("插入操作被取消或超时: %w", ctx.Err())
	case err := <-done:
		// 如果 goroutine 完成
		return err // 返回 goroutine 的执行结果 (可能为 nil 或错误)
	}
}

// Update 更新指定表中符合过滤条件的行 (使用 map 接口)。
// ctx: 用于控制操作的上下文。
// tableName: 要更新的目标表名。
// updates: 一个 map，键是要更新的列名，值是对应的新列值。
// filters: 一个 Filter 切片，定义了要更新哪些行。如果为空，则可能更新所有行 (取决于后端实现和权限)。
// 如果更新成功，返回 nil，否则返回错误。
func (c *Client) Update(ctx context.Context, tableName string, updates map[string]interface{}, filters []Filter) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}
	if len(updates) == 0 {
		return errors.New("没有要更新的数据") // 如果没有指定更新内容，返回错误
	}

	done := make(chan error, 1) // 用于 goroutine 通信

	go func() {
		// --- 在 goroutine 中执行实际的更新逻辑 ---
		req := c.conn.NewRequest(nil) // 创建请求对象
		defer req.Reset()             // 确保重置

		// 设置基本请求属性
		req.SetService("openplant") // 服务名
		req.SetAction(ActionUpdate) // 操作类型为更新
		req.SetTableName(tableName) // 目标表名

		// --- 设置过滤条件 ---
		if len(filters) > 0 {
			// 假设 Filter 结构与底层兼容
			req.SetFilters(filters) // 设置过滤器
		}
		// 注意：如果没有过滤器，此操作可能会更新表中的所有行，需要谨慎使用。

		// --- 创建并准备包含更新数据的 Table 对象 ---
		// 更新操作通常只需要发送要更新的列和它们的新值。
		// 可以创建一个只包含一行的 Table 来承载这些更新数据。
		updateTable := NewTable(tableName, 1)          // 创建容量为 1 的 Table
		columnNames := make([]string, 0, len(updates)) // 存储更新的列名
		columnTypes := make(map[string]int)            // 存储推断的类型

		for colName, val := range updates { // 遍历要更新的列和值
			columnNames = append(columnNames, colName)
			// 推断类型
			vtType := inferOpioType(val)
			if vtType == VtNull && val != nil {
				vtType = VtObject
			}
			columnTypes[colName] = vtType
			// 将要更新的列添加到 Table 定义
			updateTable.AddColumn(colName, vtType, 0)
		}

		// 填充这一行更新数据
		for i, colName := range columnNames {
			value := updates[colName] // 获取新值
			// 设置到 Table 的第一行 (行索引 0，列索引 i)
			err := updateTable.SetColumnValue(uint32(i), value)
			if err != nil {
				// 记录设置值时可能发生的错误
				fmt.Printf("警告: 设置更新列 %d (%s) 的值 (%v) 时出错: %v\n", i, colName, value, err)
			}
		}
		updateTable.BindRow() // 绑定这一行数据

		// --- 将包含更新数据的 Table 设置到 Request 对象中 ---
		err := req.SetTable(updateTable) // SetTable 会进行验证
		if err != nil {
			done <- fmt.Errorf("设置更新表时出错: %w", err)
			return
		}

		// --- 发送请求 ---
		// 更新操作也需要发送请求头 (属性，包含过滤器) 和请求体 (Table 数据)
		err = req.Write() // 发送请求头
		if err != nil {
			done <- fmt.Errorf("发送更新请求头失败: %w", err)
			return
		}
		err = req.WriteContent(updateTable) // 发送包含更新数据的请求体
		if err != nil {
			done <- fmt.Errorf("发送更新数据体失败: %w", err)
			return
		}
		req.Flush() // 刷新缓冲区

		// --- 获取并处理响应 ---
		res, err := req.GetResponse() // 获取服务器响应
		if err != nil {
			done <- fmt.Errorf("获取更新响应失败: %w", err)
			return
		}

		// 检查响应中是否包含错误
		if res.GetErrNo() != 0 {
			done <- fmt.Errorf("更新失败: %s (错误码: %d)", res.GetError(), res.GetErrNo())
			return
		}

		// 更新成功
		done <- nil
		// --- goroutine 结束 ---
	}()

	// 使用 select 等待 goroutine 完成或 context 取消
	select {
	case <-ctx.Done():
		return fmt.Errorf("更新操作被取消或超时: %w", ctx.Err())
	case err := <-done:
		return err // 返回 goroutine 的执行结果
	}
}

// Delete 删除指定表中符合过滤条件的行。
// ctx: 用于控制操作的上下文。
// tableName: 要删除数据的目标表名。
// filters: 一个 Filter 切片，定义了要删除哪些行。
// **警告:** 如果 filters 为空，此操作可能会删除表中的所有数据，请务必谨慎！
// 如果删除成功，返回 nil，否则返回错误。
func (c *Client) Delete(ctx context.Context, tableName string, filters []Filter) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}
	// 强烈建议在调用此方法前进行检查，防止在 filters 为空时意外删除整个表的数据
	// if len(filters) == 0 {
	//  return errors.New("不允许在没有过滤器的情况下执行删除操作，以防止意外删除全表数据")
	// }

	done := make(chan error, 1) // 用于 goroutine 通信

	go func() {
		// --- 在 goroutine 中执行实际的删除逻辑 ---
		req := c.conn.NewRequest(nil) // 创建请求对象
		defer req.Reset()             // 确保重置

		// 设置基本请求属性
		req.SetService("openplant") // 服务名
		req.SetAction(ActionDelete) // 操作类型为删除
		req.SetTableName(tableName) // 目标表名

		// --- 设置过滤条件 ---
		if len(filters) > 0 {
			req.SetFilters(filters) // 设置过滤器
		}
		// 如果 filters 为空，请求将不带过滤条件发送

		// --- 发送请求 ---
		// 删除操作通常只需要发送请求头 (包含属性和过滤器)，不需要数据体。
		err := req.WriteAndFlush() // 发送请求头并刷新缓冲区
		if err != nil {
			done <- fmt.Errorf("发送删除请求失败: %w", err)
			return
		}

		// --- 获取并处理响应 ---
		res, err := req.GetResponse() // 获取服务器响应
		if err != nil {
			done <- fmt.Errorf("获取删除响应失败: %w", err)
			return
		}

		// 检查响应中是否包含错误
		if res.GetErrNo() != 0 {
			done <- fmt.Errorf("删除失败: %s (错误码: %d)", res.GetError(), res.GetErrNo())
			return
		}

		// 删除成功
		done <- nil
		// --- goroutine 结束 ---
	}()

	// 使用 select 等待 goroutine 完成或 context 取消
	select {
	case <-ctx.Done():
		return fmt.Errorf("删除操作被取消或超时: %w", ctx.Err())
	case err := <-done:
		return err // 返回 goroutine 的执行结果
	}
}

// inferOpioType 是一个辅助函数，尝试从给定的 Go interface{} 值推断出对应的 opio Vt* 类型常量。
// value: 要推断类型的 Go 值。
// 返回值: 推断出的 opio Vt* 类型常量 (int)。
// 注意: 这是一个简化的实现，可能无法覆盖所有边缘情况或自定义类型。
func inferOpioType(value interface{}) int {
	if value == nil {
		return VtNull // nil 值对应 VtNull
	}
	switch value.(type) { // 使用类型断言来判断具体类型
	// --- 基本类型映射 ---
	case bool:
		return VtBool // bool 对应 VtBool
	case int8, uint8:
		return VtInt8 // int8, uint8 对应 VtInt8
	case int16, uint16:
		return VtInt16 // int16, uint16 对应 VtInt16
	case int32, uint32:
		return VtInt32 // int32, uint32 对应 VtInt32
	case int64, uint64, int: // int 通常是 64 位，也映射到 VtInt64
		// 数据库中的 LONG 类型也应映射到 VtInt64
		return VtInt64
	case float32:
		return VtFloat // float32 对应 VtFloat (或 VtFloat32)
	case float64:
		return VtDouble // float64 对应 VtDouble (或 VtFloat64)
	case time.Time:
		return VtDateTime // time.Time 对应 VtDateTime
	case string:
		// 数据库中的 TEXT, VARCHAR 等通常映射到 string
		return VtString
	case []byte:
		// 数据库中的 BLOB, BINARY 等映射到 []byte
		return VtBinary

	// --- 复合类型映射 (简化处理) ---
	// 对于 map, slice, struct，通常映射为 VtMap, VtSlice, VtStructure 或通用的 VtObject
	case map[string]interface{}:
		return VtMap // map[string]interface{} 暂时映射到 VtMap
	case []interface{}:
		return VtSlice // []interface{} 暂时映射到 VtSlice
	default:
		// 尝试使用反射检查是否是结构体
		// v := reflect.ValueOf(value)
		// if v.Kind() == reflect.Struct {
		//  return VtStructure // 如果协议明确支持结构体类型
		// }

		// 对于无法精确匹配的类型 (例如自定义类型、其他 map/slice 类型)，
		// 默认映射为 VtObject。
		// 这要求底层的 SetColumnValue 能够处理 VtObject 类型或进行进一步的序列化。
		return VtObject
	}
}

// ====================================================================================
// V2 Query/Modify (Struct Interface - Convenience Methods)
// ====================================================================================

// QueryInto 执行查询并将结果直接扫描到目标结构体切片。
// 这是 Query 和 QueryResult.Scan 的便捷封装。
// ctx: 用于控制操作的上下文。
// dest: 必须是一个指向结构体切片的指针 (例如 *[]MyStruct)。
// tableName: 要查询的表名。
// columns: 要查询的列名列表。如果为空或 `["*"]`，则查询所有列。
// opts: 查询选项，如过滤器、排序、分页等。
// 如果查询或扫描失败，返回错误。
func (c *Client) QueryInto(ctx context.Context, dest interface{}, tableName string, columns []string, opts *QueryOptions) error {
	// 1. 执行查询
	queryResult, err := c.Query(ctx, tableName, columns, opts)
	if err != nil {
		return fmt.Errorf("执行查询失败: %w", err) // 返回查询错误
	}

	// 2. 扫描结果到目标
	err = queryResult.Scan(dest)
	if err != nil {
		return fmt.Errorf("扫描查询结果失败: %w", err) // 返回扫描错误
	}

	return nil // 成功
}

// InsertStructs 从结构体切片插入数据。
// 这是 Insert 方法的结构体版本，提供了类型安全和便捷性。
// ctx: 用于控制操作的上下文。
// tableName: 要插入数据的目标表名。
// data: 必须是一个结构体切片或指向结构体切片的指针 (例如 []MyStruct 或 *[]MyStruct)。
//
//	结构体字段通过 `opio:"列名"` 或 `db:"列名"` 标签映射到数据库列。
//	没有标签的导出字段会尝试按字段名 (忽略大小写) 匹配列名。
//	标签为 `"-"` 或未导出的字段会被忽略。
//
// 如果插入成功，返回 nil，否则返回错误。
func (c *Client) InsertStructs(ctx context.Context, tableName string, data interface{}) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}

	// 1. 验证输入类型并获取切片值
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr { // 如果是指针，获取其指向的值
		val = val.Elem()
	}
	if val.Kind() != reflect.Slice { // 必须是切片
		return errors.New("InsertStructs 的 data 参数必须是切片或指向切片的指针")
	}
	if val.Len() == 0 { // 如果切片为空，无需插入
		return nil // 或者返回错误 "没有要插入的数据"
	}

	// 2. 获取结构体类型和字段映射
	structType := val.Type().Elem() // 获取切片元素类型
	if structType.Kind() != reflect.Struct {
		return errors.New("InsertStructs 的 data 参数切片元素必须是结构体")
	}

	// 存储列名 (按首次遇到的顺序) 和字段索引的映射
	columnNames := []string{}
	fieldIndices := make(map[string]int)    // 列名 (小写) -> 结构体字段索引
	columnOpioTypes := make(map[string]int) // 列名 (小写) -> 推断的 opio 类型

	// 遍历第一个结构体实例以确定列和类型
	firstStruct := val.Index(0)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if !field.IsExported() { // 跳过未导出字段
			continue
		}
		tag := field.Tag.Get("opio")
		if tag == "" {
			tag = field.Tag.Get("db")
		}
		if tag == "-" { // 跳过忽略的字段
			continue
		}

		colName := tag
		if colName == "" { // 无标签，使用字段名
			colName = field.Name
		}
		lowerColName := strings.ToLower(colName)

		// 避免重复添加同一列 (如果标签和字段名映射到同一列)
		if _, exists := fieldIndices[lowerColName]; !exists {
			columnNames = append(columnNames, colName) // 记录原始大小写列名
			fieldIndices[lowerColName] = i             // 记录字段索引

			// 从第一个实例推断类型
			fieldValue := firstStruct.Field(i).Interface()
			vtType := inferOpioType(fieldValue)
			if vtType == VtNull && fieldValue != nil {
				vtType = VtObject
			}
			columnOpioTypes[lowerColName] = vtType // 存储推断的类型
		}
	}

	if len(columnNames) == 0 {
		return errors.New("未找到可用于插入的结构体字段")
	}

	// 3. 创建并填充 Table 对象
	insertTable := NewTable(tableName, uint(val.Len())) // 创建 Table，容量为数据行数
	// 添加列定义到 Table
	for _, colName := range columnNames {
		lowerColName := strings.ToLower(colName)
		vtType := columnOpioTypes[lowerColName]
		insertTable.AddColumn(colName, vtType, 0) // 添加列，长度让 SetColumnValue 处理
	}

	// 遍历结构体切片，填充 Table 行数据
	for rowIndex := 0; rowIndex < val.Len(); rowIndex++ {
		structInstance := val.Index(rowIndex) // 获取当前结构体实例
		for i, colName := range columnNames { // 遍历列定义
			lowerColName := strings.ToLower(colName)
			fieldIndex := fieldIndices[lowerColName]                   // 获取对应的结构体字段索引
			fieldValue := structInstance.Field(fieldIndex).Interface() // 获取字段值

			// 设置列值
			err := insertTable.SetColumnValue(uint32(i), fieldValue)
			if err != nil {
				// 记录设置列值时的错误
				fmt.Printf("警告: InsertStructs 设置行 %d 列 %d (%s) 的值 (%v) 时出错: %v\n", rowIndex, i, colName, fieldValue, err)
			}
		}
		insertTable.BindRow() // 绑定当前行
	}

	// 4. 执行插入操作 (使用 goroutine 和 context)
	done := make(chan error, 1)
	go func() {
		req := c.conn.NewRequest(nil)
		defer req.Reset()
		req.SetService("openplant")
		req.SetAction(ActionInsert)
		req.SetTableName(tableName)

		err := req.SetTable(insertTable) // 设置 Table 到请求
		if err != nil {
			done <- fmt.Errorf("设置插入表时出错: %w", err)
			return
		}

		// 发送请求头和内容
		err = req.Write()
		if err != nil {
			done <- fmt.Errorf("发送插入请求头失败: %w", err)
			return
		}
		err = req.WriteContent(insertTable)
		if err != nil {
			done <- fmt.Errorf("发送插入数据体失败: %w", err)
			return
		}
		req.Flush()

		// 获取响应
		res, err := req.GetResponse()
		if err != nil {
			done <- fmt.Errorf("获取插入响应失败: %w", err)
			return
		}
		if res.GetErrNo() != 0 {
			done <- fmt.Errorf("插入失败: %s (错误码: %d)", res.GetError(), res.GetErrNo())
			return
		}
		done <- nil // 成功
	}()

	// 等待完成或 context 取消
	select {
	case <-ctx.Done():
		return fmt.Errorf("插入操作被取消或超时: %w", ctx.Err())
	case err := <-done:
		return err
	}
}

// UpdateStruct 根据结构体实例更新数据。
// 这是 Update 方法的结构体版本。
// ctx: 用于控制操作的上下文。
// tableName: 要更新的目标表名。
// data: 必须是一个结构体实例或指向结构体的指针 (例如 MyStruct 或 *MyStruct)。
//
//	结构体字段通过 `opio:"列名"` 或 `db:"列名"` 标签映射到要更新的数据库列。
//	默认情况下，所有带标签的导出字段都会被用于更新。
//	(未来可以增加选项，例如只更新非零值字段或指定字段)。
//
// filters: 一个 Filter 切片，定义了要更新哪些行。**不能为空**，以防止意外更新全表。
// 如果更新成功，返回 nil，否则返回错误。
func (c *Client) UpdateStruct(ctx context.Context, tableName string, data interface{}, filters []Filter) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}
	if len(filters) == 0 {
		// 强制要求提供过滤器，防止意外更新整个表
		return errors.New("UpdateStruct 必须提供过滤器 (filters)")
	}

	// 1. 验证输入类型并获取结构体值
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr { // 如果是指针，获取其指向的值
		if val.IsNil() {
			return errors.New("UpdateStruct 的 data 参数指针不能为 nil")
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct { // 必须是结构体
		return errors.New("UpdateStruct 的 data 参数必须是结构体或指向结构体的指针")
	}
	structType := val.Type()

	// 2. 提取要更新的列和值
	updates := make(map[string]interface{})
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if !field.IsExported() { // 跳过未导出字段
			continue
		}
		tag := field.Tag.Get("opio")
		if tag == "" {
			tag = field.Tag.Get("db")
		}
		if tag == "-" || tag == "" { // 跳过忽略的字段或没有标签的字段 (更新时通常需要明确指定列)
			continue
		}
		// 获取字段值并添加到 updates map
		updates[tag] = val.Field(i).Interface()
	}

	if len(updates) == 0 {
		return errors.New("未找到可用于更新的带标签的结构体字段")
	}

	// 3. 调用现有的 Update 方法 (基于 map 的版本)
	return c.Update(ctx, tableName, updates, filters)
}

// DeleteByID 根据单个 ID 删除记录的便捷方法。
// 这是 Delete 方法针对按 ID 删除场景的封装。
// ctx: 用于控制操作的上下文。
// tableName: 要删除数据的目标表名。
// idColumn: 作为主键或唯一标识的列名。
// id: 要删除的记录的 ID 值。其类型应与 idColumn 的数据库类型兼容 (例如 int, int32, int64, string)。
// 如果删除成功，返回 nil，否则返回错误。
func (c *Client) DeleteByID(ctx context.Context, tableName string, idColumn string, id interface{}) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}

	// 1. 将 ID 值转换为适合过滤器的字符串形式
	var idStr string
	switch v := id.(type) {
	case string:
		// 对字符串进行简单的 SQL 转义 (替换单引号) 并添加引号
		// 注意：这只是基础的转义，不能完全防止 SQL 注入。
		// 对于生产环境，应考虑使用更健壮的库或参数化查询（如果后端支持）。
		escaped := strings.ReplaceAll(v, "'", "''")
		idStr = fmt.Sprintf("'%s'", escaped)
	case int, int8, int16, int32, int64:
		idStr = strconv.FormatInt(reflect.ValueOf(v).Int(), 10)
	case uint, uint8, uint16, uint32, uint64:
		idStr = strconv.FormatUint(reflect.ValueOf(v).Uint(), 10)
	case float32:
		idStr = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		idStr = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		// 根据数据库习惯，可能需要 'true'/'false' 或 1/0
		if v {
			idStr = "1" // 或者 'true'
		} else {
			idStr = "0" // 或者 'false'
		}
	default:
		// 对于其他不支持的类型，返回错误
		return fmt.Errorf("DeleteByID 不支持的 ID 类型: %T", id)
	}

	// 2. 创建过滤器
	filters := []Filter{
		*NewFilter(idColumn, OperEQ, idStr, RelationAnd),
	}

	// 3. 调用现有的 Delete 方法 (基于 Filter 的版本)
	return c.Delete(ctx, tableName, filters)
}

// ====================================================================================
// V3 Time Series API
// ====================================================================================

// ReadRealtime 读取指定 ID 列表的实时数据 (V3 API)。
// ctx: 用于控制操作的上下文。
// values: 一个 Value 切片。调用前，每个 Value 的 ID 字段必须被设置。
//
//	函数执行后，将填充每个 Value 对应的 TM (时间戳), DS (状态), AV (值) 字段。
//
// 如果读取成功，返回 nil，否则返回错误。
func (c *Client) ReadRealtime(ctx context.Context, values []Value) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}
	if len(values) == 0 {
		return errors.New("没有要读取的实时数据点") // 如果输入为空，直接返回
	}

	// 使用通道在 goroutine 和主线程间传递错误信息
	done := make(chan error, 1)

	// 启动 goroutine 执行可能阻塞的底层调用
	go func() {
		err := c.conn.ReadRealtime(values) // 调用底层的 ReadRealtime
		done <- err                        // 将执行结果 (错误或 nil) 发送到通道
	}()

	// 使用 select 等待 goroutine 完成或 context 被取消
	select {
	case <-ctx.Done():
		// 如果 context 被取消或超时
		// 注意：底层的网络操作可能无法直接中断，但我们可以不再等待其结果。
		// 这可能导致资源暂时未释放，但最终会被 Go 的垃圾回收处理。
		return fmt.Errorf("读取实时数据操作被取消或超时: %w", ctx.Err())
	case err := <-done:
		// 如果 goroutine 完成
		if err != nil {
			// 如果底层调用返回错误，包装并返回
			return fmt.Errorf("读取实时数据失败: %w", err)
		}
		return nil // 操作成功
	}
}

// WriteRealtime 写入实时数据 (V3 API)。
// ctx: 用于控制操作的上下文。
// values: 一个 Value 切片，包含要写入的点位 ID, TM (时间戳), DS (状态), AV (值) 数据。
// 如果写入成功，返回 nil，否则返回错误。
func (c *Client) WriteRealtime(ctx context.Context, values []Value) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}
	if len(values) == 0 {
		return errors.New("没有要写入的实时数据") // 如果输入为空，直接返回
	}

	done := make(chan error, 1) // 用于 goroutine 通信
	go func() {
		err := c.conn.WriteRealtime(values) // 调用底层的 WriteRealtime
		done <- err                         // 发送结果
	}()

	// 等待完成或取消
	select {
	case <-ctx.Done():
		return fmt.Errorf("写入实时数据操作被取消或超时: %w", ctx.Err())
	case err := <-done:
		if err != nil {
			return fmt.Errorf("写入实时数据失败: %w", err)
		}
		return nil // 操作成功
	}
}

// ReadArchive 读取历史数据 (V3 API)。
// ctx: 用于控制操作的上下文。
// ids: 要查询历史数据的点位 ID 列表。
// mode: 查询模式 (例如 opio.ModeRaw, opio.ModeAvg 等，定义在 api_v3.go)。
// begin, end: 查询的时间范围。
// interval: 查询间隔（仅在某些聚合查询模式下有效，单位秒）。
// 返回值: 包含查询结果的 Archive 指针切片，或者一个错误。
func (c *Client) ReadArchive(ctx context.Context, ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Archive, error) {
	if c.conn == nil {
		return nil, errors.New("客户端未连接或已关闭")
	}

	// 定义用于 goroutine 通信的结构体
	type result struct {
		archives []*Archive
		err      error
	}
	done := make(chan result, 1) // 通道传递结果和错误

	go func() {
		// 调用底层的 ReadArchive
		archives, err := c.conn.ReadArchive(ids, mode, begin, end, interval)
		done <- result{archives: archives, err: err} // 发送结果
	}()

	// 等待完成或取消
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("读取历史数据操作被取消或超时: %w", ctx.Err())
	case res := <-done:
		if res.err != nil {
			return nil, fmt.Errorf("读取历史数据失败: %w", res.err) // 返回错误
		}
		return res.archives, nil // 返回结果
	}
}

// WriteArchive 写入历史数据 (V3 API)。
// ctx: 用于控制操作的上下文。
// archives: 一个 Archive 指针切片，包含要写入的历史数据 (每个 Archive 包含点位 ID 和 Value 数据点)。
// cache: 是否使用缓存写入模式 (通常用于批量写入)。
// 如果写入成功，返回 nil，否则返回错误。
func (c *Client) WriteArchive(ctx context.Context, archives []*Archive, cache bool) error {
	if c.conn == nil {
		return errors.New("客户端未连接或已关闭")
	}

	done := make(chan error, 1) // 用于 goroutine 通信
	go func() {
		err := c.conn.WriteArchive(archives, cache) // 调用底层的 WriteArchive
		done <- err                                 // 发送结果
	}()

	// 等待完成或取消
	select {
	case <-ctx.Done():
		return fmt.Errorf("写入历史数据操作被取消或超时: %w", ctx.Err())
	case err := <-done:
		if err != nil {
			return fmt.Errorf("写入历史数据失败: %w", err)
		}
		return nil // 操作成功
	}
}

// ReadStat 读取统计数据 (V3 API)。
// ctx: 用于控制操作的上下文。
// ids: 要查询统计数据的点位 ID 列表。
// mode: 查询模式 (必须是统计模式，例如 opio.ModeAvg, opio.ModeMax 等，定义在 api_v3.go)。
// begin, end: 查询的时间范围。
// interval: 统计间隔（单位秒）。
// 返回值: 包含查询结果的 Stat 指针切片，或者一个错误。
func (c *Client) ReadStat(ctx context.Context, ids []int32, mode int32, begin, end time.Time, interval int32) ([]*Stat, error) {
	if c.conn == nil {
		return nil, errors.New("客户端未连接或已关闭")
	}

	// 定义用于 goroutine 通信的结构体
	type result struct {
		stats []*Stat
		err   error
	}
	done := make(chan result, 1) // 通道传递结果和错误

	go func() {
		// 调用底层的 ReadStat
		stats, err := c.conn.ReadStat(ids, mode, begin, end, interval)
		done <- result{stats: stats, err: err} // 发送结果
	}()

	// 等待完成或取消
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("读取统计数据操作被取消或超时: %w", ctx.Err())
	case res := <-done:
		if res.err != nil {
			return nil, fmt.Errorf("读取统计数据失败: %w", res.err) // 返回错误
		}
		return res.stats, nil // 返回结果
	}
}

// ====================================================================================
// Subscription API
// ====================================================================================

// SubscriptionEvent 代表从实时数据订阅接收到的单个事件。
// 它可能包含一行数据 (Data) 或一个错误 (Err)。
type SubscriptionEvent struct {
	Data map[string]interface{} // 接收到的行数据 (列名 -> 值)
	Err  error                  // 如果发生错误，此字段非 nil
}

// Subscription 代表一个活动的实时数据订阅会话。
type Subscription struct {
	sub      *Subscribe             // 底层的 opio Subscribe 对象，管理实际的订阅连接和逻辑
	eventCh  chan SubscriptionEvent // 用于向用户传递数据事件或错误的通道
	cancelFn context.CancelFunc     // 用于取消内部 goroutine 的函数
	closed   chan struct{}          // 一个关闭信号通道，用于标记订阅是否已关闭
}

// Events 返回一个只读通道，用户可以从此通道接收订阅事件 (SubscriptionEvent)。
// 当订阅关闭或发生不可恢复的错误时，此通道将被关闭。
func (s *Subscription) Events() <-chan SubscriptionEvent {
	return s.eventCh // 返回内部事件通道的只读视图
}

// Close 关闭此订阅会话。
// 它会停止接收数据，取消内部处理 goroutine，并关闭底层的 opio 订阅连接。
// 多次调用 Close 是安全的。
func (s *Subscription) Close() error {
	// 使用 select 和 closed 通道防止重复关闭
	select {
	case <-s.closed:
		return errors.New("订阅已关闭") // 如果已经关闭，直接返回错误
	default:
		close(s.closed) // 关闭 closed 通道，标记为已关闭状态
	}

	// 调用 cancel 函数来取消与此订阅关联的内部 goroutine
	if s.cancelFn != nil {
		s.cancelFn()
	}

	// 关闭底层的 opio Subscribe 对象 (这通常会关闭其独立的网络连接)
	if s.sub != nil {
		s.sub.Close()
	}

	// 注意：事件通道 (eventCh) 由负责发送数据的 goroutine (在 Subscribe 方法中启动) 关闭。
	// 因此，这里不需要手动关闭 eventCh。
	// close(s.eventCh)

	return nil // 成功关闭
}

// AddKeys 动态地向当前活动的订阅添加新的键 (例如，点位 ID)。
// keys: 要添加的键列表。其类型应与初始化订阅时使用的类型匹配 (例如 []int32, []int64, 或 []string)。
// 如果订阅已关闭或添加失败，返回错误。
func (s *Subscription) AddKeys(keys interface{}) error {
	select {
	case <-s.closed: // 检查订阅是否已关闭
		return errors.New("订阅已关闭")
	default:
		if s.sub == nil { // 检查底层对象是否有效
			return errors.New("底层订阅无效")
		}
		// 调用底层 Subscribe 对象的 Subscribe 方法来添加键
		// 注意：底层的 Subscribe 方法可能用于添加键，而不是创建新订阅
		return s.sub.Subscribe(keys)
	}
}

// RemoveKeys 动态地从当前活动的订阅中移除键。
// keys: 要移除的键列表。其类型应与初始化订阅时使用的类型匹配。
// 如果订阅已关闭或移除失败，返回错误。
func (s *Subscription) RemoveKeys(keys interface{}) error {
	select {
	case <-s.closed: // 检查订阅是否已关闭
		return errors.New("订阅已关闭")
	default:
		if s.sub == nil { // 检查底层对象是否有效
			return errors.New("底层订阅无效")
		}
		// 调用底层 Subscribe 对象的 UnSubscribe 方法来移除键
		return s.sub.UnSubscribe(keys)
	}
}

// SubscribeOptions 定义创建订阅时的选项。
type SubscribeOptions struct {
	Snapshot bool // 是否在订阅建立时获取一次初始快照数据
	// 可以添加其他选项，例如设置内部事件通道的缓冲区大小等
}

// Subscribe 创建一个新的实时数据订阅。
// ctx: 用于控制整个订阅生命周期的上下文。当此上下文被取消时，订阅将自动关闭。
// tableName: 要订阅数据的目标表名 (例如 "Realtime")。
// keyName: 用于标识订阅目标的键列名 (例如 "ID")。
// keys: 初始要订阅的键列表。类型必须是 []int32, []int64 或 []string，与 keyName 列的数据类型匹配。
// opts: 订阅选项 (例如是否获取快照)。
// 返回值: 一个 Subscription 对象用于管理订阅和接收事件，或者一个错误。
func (c *Client) Subscribe(ctx context.Context, tableName string, keyName string, keys interface{}, opts *SubscribeOptions) (*Subscription, error) {
	if c.conn == nil {
		return nil, errors.New("客户端未连接或已关闭")
	}

	// 验证 keys 参数的类型 - 底层的 InitSubscribe 方法会进行验证，这里可以省略显式检查
	// （保留注释作为参考）
	// var keyType int
	// switch keys.(type) {
	// case []int32:
	// 	keyType = 1
	// case []int64:
	// 	keyType = 2
	// case []string:
	// 	keyType = 3
	// default:
	// 	return nil, errors.New("无效的 keys 类型，必须是 []int32, []int64, 或 []string")
	// }

	// 创建底层的 opio.Subscribe 对象。
	// 注意：NewSubscribe 方法通常会复制当前的连接设置，为订阅创建一个独立的连接，
	// 以避免与客户端上的其他操作（如查询、写入）互相干扰。
	sub, err := c.conn.NewSubscribe(tableName)
	if err != nil {
		return nil, fmt.Errorf("创建底层订阅失败: %w", err)
	}

	// 设置订阅选项 (例如是否获取快照)
	useSnapshot := false
	if opts != nil {
		useSnapshot = opts.Snapshot
	}
	err = sub.SetSnapshot(useSnapshot) // 调用底层方法设置快照选项
	if err != nil {
		sub.Close() // 如果设置选项失败，需要关闭刚刚创建的底层订阅连接
		return nil, fmt.Errorf("设置快照选项失败: %w", err)
	}

	// 创建用于传递事件的通道和用于标记关闭的通道
	eventCh := make(chan SubscriptionEvent, 100) // 使用带缓冲的通道，避免阻塞回调函数
	closedCh := make(chan struct{})              // 用于内部关闭信号

	// 创建一个新的、可取消的上下文，用于控制内部处理 goroutine 的生命周期
	// 这个上下文会链接到传入的外部上下文 ctx
	subCtx, cancelFn := context.WithCancel(ctx)

	// 创建 Subscription 对象，包含底层对象、通道和取消函数
	subscription := &Subscription{
		sub:      sub,
		eventCh:  eventCh,
		cancelFn: cancelFn,
		closed:   closedCh,
	}

	// 定义一个回调函数，该函数将在底层 opio 库接收到数据或错误时被调用
	callback := func(res *Response) {
		// 在处理之前，检查内部上下文是否已被取消 (可能由 Close 方法触发)
		select {
		case <-subCtx.Done():
			return // 如果已取消，则不处理此响应
		default:
			// 继续处理响应
		}

		// 检查响应中是否包含错误
		if res.GetErrNo() != 0 {
			// 如果有错误，创建一个错误事件并尝试发送到事件通道
			errEvent := SubscriptionEvent{Err: fmt.Errorf("%s (错误码: %d)", res.GetError(), res.GetErrNo())}
			select {
			case eventCh <- errEvent: // 发送错误事件
			case <-subCtx.Done(): // 如果在发送时上下文被取消，则放弃发送
			}
			// 注意：原始的 opio 回调可能包含自动重连逻辑。
			// 在这个封装中，我们简化为仅报告错误。用户可以根据收到的错误决定是否重新订阅。
			// 如果是连接断开等严重错误，可能需要考虑自动关闭此 Subscription。
			// 例如:
			// if res.GetErrNo() == -97 || res.GetErrNo() == -90 { // 假设这些是连接错误码
			//  subscription.Close() // 触发外部和内部的清理逻辑
			// }
			return // 处理完错误后返回
		}

		// 如果响应没有错误，处理返回的数据集
		dataSet := res.GetDataSet()
		if dataSet == nil {
			// 如果没有数据集 (可能只是一个心跳响应或无数据更新)，则无需处理
			return
		}
		// 注意：需要确保 GetDataSet 返回的 dataSet 在回调函数执行期间是有效的。
		// 假设底层库保证了这一点，或者我们需要在回调内部完整地处理完 dataSet。
		// 处理完后需要关闭 dataSet。
		defer dataSet.Close() // 确保数据集被关闭

		columns := dataSet.GetColumns() // 获取列信息
		// 迭代处理数据集中的每一行
		for {
			hasNext, err := dataSet.Next() // 移动到下一行
			if err != nil {
				// 读取下一行时出错
				errEvent := SubscriptionEvent{Err: fmt.Errorf("读取订阅数据行失败: %w", err)}
				select {
				case eventCh <- errEvent: // 发送错误事件
				case <-subCtx.Done(): // 检查取消状态
				}
				// 如果 Next 出错，通常无法继续读取此批数据，直接返回
				return
			}
			if !hasNext {
				break // 没有更多行了，退出循环
			}

			// 处理当前行的数据
			rowMap := make(map[string]interface{}) // 为当前行创建 map
			for i, col := range columns {          // 遍历所有列
				val, err := dataSet.GetValue(uint32(i)) // 获取列值
				if err != nil {
					// 获取列值时出错，记录警告，并将值设为 nil
					fmt.Printf("警告: 获取订阅行 %d 列 %d (%s) 的值失败: %v\n", dataSet.rowCursor-1, i, col.name, err) // rowCursor 可能已增加
					rowMap[col.name] = nil
				} else {
					rowMap[col.name] = val // 存入 map
				}
			}
			// 创建数据事件并尝试发送到事件通道
			dataEvent := SubscriptionEvent{Data: rowMap}
			select {
			case eventCh <- dataEvent: // 发送数据事件
			case <-subCtx.Done(): // 如果在发送时上下文被取消，则放弃发送并返回
				return
			}
		}
		// 当前批次的数据处理完毕 (dataSet 会在 defer 中关闭)
	}

	// 调用底层 Subscribe 对象的 InitSubscribe 方法，传入初始键、键列名和回调函数
	// 这将启动实际的订阅过程
	err = sub.InitSubscribe(keys, keyName, callback)
	if err != nil {
		// 如果初始化订阅失败
		sub.Close()     // 关闭已创建的底层连接
		cancelFn()      // 取消内部上下文
		close(closedCh) // 标记为关闭
		close(eventCh)  // 关闭事件通道
		return nil, fmt.Errorf("初始化底层订阅失败: %w", err)
	}

	// 启动一个 goroutine，用于监听外部传入的 context (ctx) 的取消信号
	go func() {
		select {
		case <-ctx.Done(): // 如果外部 context 被取消
			subscription.Close() // 调用 Close 方法来关闭整个订阅
		case <-closedCh: // 如果订阅被内部关闭 (例如调用了 subscription.Close())
			// 不需要额外操作，因为清理逻辑已经在 Close 中执行
		}
		// 确保在 goroutine 退出时，事件通道最终被关闭，以通知接收者订阅已结束
		close(eventCh)
	}()

	// 返回创建好的 Subscription 对象
	return subscription, nil
}

// ====================================================================================
// SQL Execution
// ====================================================================================

// ExecSQL 执行原始的 SQL 查询或命令。
// ctx: 用于控制操作的上下文。
// sql: 要执行的 SQL 语句。
// 返回值:
// - 对于 SELECT 语句，返回包含结果的 QueryResult 指针。
// - 对于非 SELECT 语句 (如 INSERT, UPDATE, DELETE, CREATE)，如果执行成功，通常返回一个空的 QueryResult (无行无列) 和 nil 错误。
// - 如果执行失败，返回 nil QueryResult 和一个错误。
// 此方法支持通过 context 进行取消或超时控制。
func (c *Client) ExecSQL(ctx context.Context, sql string) (*QueryResult, error) {
	if c.conn == nil {
		return nil, errors.New("客户端未连接或已关闭")
	}

	// 定义用于 goroutine 通信的结构体
	type execResultWithError struct {
		result *QueryResult
		err    error
	}
	done := make(chan execResultWithError, 1) // 通道传递结果和错误

	go func() {
		// --- 在 goroutine 中执行 SQL ---
		req := c.conn.NewRequest(nil) // 创建请求对象
		defer req.Reset()             // 确保重置

		// 设置请求属性
		req.SetService("openplant")  // 服务名
		req.SetAction(ActionExecSQL) // 操作类型为执行 SQL
		req.SetSQL(sql)              // 设置要执行的 SQL 语句

		// 发送请求 (ExecSQL 通常只发送请求头)
		err := req.WriteAndFlush()
		if err != nil {
			done <- execResultWithError{result: nil, err: fmt.Errorf("发送 SQL 请求失败: %w", err)}
			return
		}

		// 获取响应
		res, err := req.GetResponse()
		if err != nil {
			done <- execResultWithError{result: nil, err: fmt.Errorf("获取 SQL 响应失败: %w", err)}
			return
		}

		// 检查响应错误
		if res.GetErrNo() != 0 {
			done <- execResultWithError{result: nil, err: fmt.Errorf("SQL 执行失败: %s (错误码: %d)", res.GetError(), res.GetErrNo())}
			return
		}

		// 处理可能返回的结果集 (主要针对 SELECT 语句)
		dataSet := res.GetDataSet()
		if dataSet == nil {
			// 如果没有数据集 (例如非 SELECT 语句或 SELECT 无结果)，返回空的 QueryResult
			done <- execResultWithError{result: &QueryResult{Columns: []Column{}, Rows: []map[string]interface{}{}}, err: nil}
			return
		}
		defer dataSet.Close() // 确保关闭数据集

		// 构建 QueryResult
		result := &QueryResult{}
		result.Columns = dataSet.GetColumns()           // 获取列信息
		result.Rows = make([]map[string]interface{}, 0) // 初始化行数据切片

		// 迭代读取结果集行
		for {
			hasNext, err := dataSet.Next()
			if err != nil {
				// 读取行出错
				dataSet.Close() // 关闭数据集
				done <- execResultWithError{result: result, err: fmt.Errorf("读取 SQL 结果集下一行时出错: %w", err)}
				return
			}
			if !hasNext {
				break // 没有更多行
			}

			// 处理当前行
			rowMap := make(map[string]interface{})
			for i, col := range result.Columns {
				val, err := dataSet.GetValue(uint32(i)) // 获取列值
				if err != nil {
					// 获取值出错，记录警告
					fmt.Printf("警告: 获取 SQL 结果行 %d 列 %d (%s) 的值失败: %v\n", len(result.Rows), i, col.name, err)
					rowMap[col.name] = nil
				} else {
					rowMap[col.name] = val
				}
			}
			result.Rows = append(result.Rows, rowMap) // 添加到结果列表
		}

		// 成功处理完结果集
		done <- execResultWithError{result: result, err: nil}
		// --- goroutine 结束 ---
	}()

	// 使用 select 等待 goroutine 完成或 context 取消
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("SQL 执行操作被取消或超时: %w", ctx.Err())
	case res := <-done:
		return res.result, res.err // 返回 goroutine 的结果和错误
	}
}

// ====================================================================================
// TODOs and Future Considerations
// ====================================================================================

// TODO: (待办事项)
// 1. 错误处理: 可以考虑提供更具体的错误类型，方便调用者进行错误判断和处理。
// 2. UpdateStruct: 更新策略可以更灵活 (例如只更新非零值字段，或提供选项指定要更新的字段)。
// 3. DeleteByID: ID 到字符串的转换可以进一步增强，例如处理更多数据库特定的引号规则或数据类型。
// 4. Scan/assignWithConversion: 可以添加更多自定义类型转换规则，例如处理 JSON 字符串到 map/struct 的转换。
// 5. 配置选项: Client 可以增加更多配置项，如日志记录器、默认请求超时等。
// 6. 连接池: 对于高并发场景，可以考虑实现或集成连接池。
