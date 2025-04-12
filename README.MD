# opio 包 - Client 使用指南

`opio.Client` 提供了一个更高级别的接口，用于与 OpenPlant 服务进行交互，封装了底层的连接管理、请求构建和响应解析逻辑。它支持 V2 风格的结构化查询/修改、原始 SQL 执行、实时数据订阅以及 V3 风格的实时/历史/统计数据读写。

## 1. 连接与关闭

使用 `opio.Connect` 函数来建立与 OpenPlant 服务器的连接并创建一个 `Client` 实例。

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tc252617228/opio"
)

func main() {
	host := "10.75.39.143" // 替换为你的服务器地址
	port := 8200         // 替换为你的服务器端口
	user := "sis"          // 替换为你的用户名
	pass := "openplant"    // 替换为你的密码
	connectTimeout := 10 * time.Second // 连接超时时间
	operationTimeout := 30 * time.Second // 后续操作的默认超时时间

	// 创建一个带超时的 context 用于连接
	connectCtx, connectCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer connectCancel() // 确保 context 被取消

	// 连接服务器
	client, err := opio.Connect(connectCtx, host, port, user, pass, 5*time.Second) // 内部连接超时可以设置短一些
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	log.Println("连接成功!")

	// 创建一个用于后续操作的 context
	// 当这个 ctx 超时或被取消时，进行中的操作会尝试中止，并且 client 会自动关闭
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel() // 确保操作 context 被取消

	// 使用 client 进行操作...
	// 例如: Ping 测试连接
	err = client.Ping(ctx)
	if err != nil {
		log.Printf("Ping 失败: %v", err)
	} else {
		log.Println("Ping 成功!")
	}

	// 操作完成后，可以通过取消 context 或等待其超时来隐式关闭 client
	// 或者显式调用 Close (通常由创建 client 时传入的 context 管理)
	// err = client.Close()
	// if err != nil {
	//  log.Printf("关闭客户端时出错: %v", err)
	// }

	log.Println("操作完成或超时，客户端将自动关闭（如果 context 被取消）")
}

```

**关键点:**

*   `opio.Connect` 返回一个 `*Client` 和一个 `error`。
*   强烈建议使用 `context.Context` 来管理连接的生命周期和操作的超时/取消。当传递给 `Connect` 的 `context` 被取消时，`Client` 会尝试自动关闭。
*   `client.Close()` 用于显式关闭连接。重复关闭会返回错误。

## 2. 基本操作

### Ping

检查与服务器的连接是否仍然活跃。

```go
err := client.Ping(ctx)
if err != nil {
    log.Printf("连接不活跃: %v", err)
}
```

### 设置压缩模式

设置连接使用的数据压缩模式。

```go
err := client.SetCompression(opio.ZIP_MODEL_Frame) // 使用帧压缩
if err != nil {
    log.Printf("设置压缩模式失败: %v", err)
}

err = client.SetCompression(opio.ZIP_MODEL_Uncompressed) // 不使用压缩
if err != nil {
    log.Printf("设置压缩模式失败: %v", err)
}
```

支持的模式常量定义在 `const.go` 中 (例如 `opio.ZIP_MODEL_Uncompressed`, `opio.ZIP_MODEL_Frame`)。

## 3. 数据查询 (V2 风格)

### 结构化查询 (`client.Query`)

使用 `client.Query` 方法执行基于表名、列名和选项的结构化查询。

```go
tableName := "Point"
columns := []string{"ID", "GN", "ED"} // 查询特定列
// columns := []string{"*"} // 查询所有列

opts := &opio.QueryOptions{
	Filters: []opio.Filter{
		// *opio.NewFilter("RT", opio.OperEQ, "1", opio.RelationAnd), // 示例过滤器：RT = 1
		*opio.NewFilter("GN", opio.OperLike, "'W3.AX.%'", opio.RelationAnd), // 示例过滤器：GN LIKE 'W3.AX.%'
	},
	OrderBy: "ID ASC", // 按 ID 升序排序
	Limit:   "10",     // 最多返回 10 条记录
}

// 执行查询
queryResult, err := client.Query(ctx, tableName, columns, opts)
if err != nil {
	log.Fatalf("查询失败: %v", err)
}

log.Printf("查询到 %d 行数据", len(queryResult.Rows))

// 打印列名
colNames := []string{}
for _, col := range queryResult.Columns {
	colNames = append(colNames, col.GetName())
}
log.Printf("列名: %v", colNames)

// 打印行数据 (每行是一个 map[string]interface{})
for i, row := range queryResult.Rows {
	log.Printf("行 %d: %v", i, row)
	// 可以访问具体列的值
	// if idVal, ok := row["ID"]; ok {
	//  log.Printf("  ID: %v (类型: %T)", idVal, idVal)
	// }
}
```

**关键点:**

*   `QueryOptions` 用于指定过滤器 (`Filters`)、排序 (`OrderBy`)、分页 (`Limit`) 等。`Filter` 结构需要使用 `opio.NewFilter` 创建。
*   `client.Query` 返回 `*QueryResult` 和 `error`。
*   `QueryResult` 包含列信息 (`Columns`) 和行数据 (`Rows`)。`Rows` 是一个 `[]map[string]interface{}`，其中 map 的键是列名。

### 结果映射 (`QueryResult.Scan`)

`QueryResult.Scan` 方法可以将查询结果方便地映射到一个结构体切片中。

```go
// 定义目标结构体
type PointInfo struct {
	PointID          int32   `opio:"ID"` // 标签匹配列名 "ID"
	GroupName        string  `opio:"GN"` // 标签匹配列名 "GN"
	ExtendedDesc     *string `opio:"ED"` // 指针类型用于处理 nullable 列
	NonExistentField float64 // 此字段在查询结果中不存在，Scan 会忽略
	RT               int32   // 假设查询结果中包含 RT 列，但结构体字段名不同
}

// ... 执行查询获取 queryResult ...

var points []PointInfo // 准备目标切片

// 执行 Scan
err = queryResult.Scan(&points) // 传入目标切片的指针
if err != nil {
	log.Fatalf("Scan 失败: %v", err)
}

// 遍历 Scan 后的结果
for _, p := range points {
	log.Printf("Scan 结果: ID=%d, GN=%s, ED=%v, RT=%d", p.PointID, p.GroupName, p.ExtendedDesc, p.RT)
	if p.ExtendedDesc != nil {
		log.Printf("  (ED 值: %s)", *p.ExtendedDesc)
	} else {
		log.Printf("  (ED 值: <nil>)")
	}
}
```

**Scan 映射规则:**

1.  优先使用 `opio:"列名"` 标签匹配。
2.  其次使用 `db:"列名"` 标签匹配 (兼容常用库)。
3.  若无标签，尝试将字段名 (忽略大小写) 与列名 (忽略大小写) 匹配。
4.  标签为 `"-"` 的字段被忽略。
5.  未导出的字段被忽略。
6.  数据库 `NULL` 值可以映射到 Go 的指针类型 (结果为 `nil`)。
7.  支持常见基本类型间的自动转换 (如 `int` <-> `string`, `string` -> `bool` 等)。

## 4. 原始 SQL 执行 (`client.ExecSQL`)

使用 `client.ExecSQL` 执行任意 SQL 语句。

```go
// 执行 SELECT 语句
sqlSelect := "SELECT ID, GN, RT FROM Point WHERE RT = 1 LIMIT 5"
selectResult, err := client.ExecSQL(ctx, sqlSelect)
if err != nil {
	log.Fatalf("执行 SELECT SQL 失败: %v", err)
}
log.Printf("SELECT SQL 查询到 %d 行", len(selectResult.Rows))
// 可以像处理 client.Query 的结果一样处理 selectResult，包括使用 Scan

// 执行非 SELECT 语句 (例如 UPDATE, INSERT, DELETE, CREATE TABLE 等)
// 假设存在 test_table (ID INT, NAME VARCHAR(50))
// sqlUpdate := "UPDATE test_table SET NAME = 'new name' WHERE ID = 1"
// updateResult, err := client.ExecSQL(ctx, sqlUpdate)
// if err != nil {
//  log.Fatalf("执行 UPDATE SQL 失败: %v", err)
// }
// log.Println("UPDATE SQL 执行成功")
// 对于非 SELECT 语句，updateResult 通常是空的 (无行无列)
// log.Printf("UPDATE 结果行数: %d", len(updateResult.Rows))
```

**关键点:**

*   对于 `SELECT` 语句，`ExecSQL` 返回包含结果的 `*QueryResult`。
*   对于非 `SELECT` 语句，成功时通常返回一个空的 `*QueryResult` 和 `nil` 错误。
*   失败时返回 `nil` 和 `error`。

## 5. 便捷 CRUD 操作 (类 ORM 风格)

为了简化常见的增删改查操作，`Client` 提供了一些基于结构体和标签的便捷方法。

### 查询并扫描到结构体 (`client.QueryInto`)

此方法将 `Query` 和 `Scan` 合二为一，直接将查询结果填充到目标结构体切片。

```go
// 定义目标结构体
type PointInfo struct {
	PointID          int32   `opio:"ID"`
	GroupName        string  `opio:"GN"`
	ExtendedDesc     *string `opio:"ED"`
}

tableName := "Point"
columns := []string{"ID", "GN", "ED"}
opts := &opio.QueryOptions{
	Filters: []opio.Filter{
		*opio.NewFilter("GN", opio.OperLike, "'W3.AX.%'", opio.RelationAnd),
	},
	Limit: "5",
}

var points []PointInfo // 准备目标切片

// 执行查询并扫描
err := client.QueryInto(ctx, &points, tableName, columns, opts)
if err != nil {
	log.Fatalf("QueryInto 失败: %v", err)
}

// 直接使用扫描后的结果
for _, p := range points {
	log.Printf("查询结果: ID=%d, GN=%s", p.PointID, p.GroupName)
}
```

**关键点:**

*   第一个参数 `dest` 必须是目标结构体切片的指针 (例如 `&points`)。
*   内部调用 `client.Query` 和 `queryResult.Scan`。

### 从结构体插入数据 (`client.InsertStructs`)

直接从结构体切片插入数据，无需手动构建 `map`。

```go
// 定义要插入的结构体
type NewRecord struct {
	Name  string  `opio:"NAME"` // 假设表有 NAME 列
	Value float64 `opio:"VALUE"`// 假设表有 VALUE 列
	Notes string  // 没有标签，如果表中有 Notes 列，会尝试按名称匹配
}

tableName := "test_struct_insert" // 假设此表存在
recordsToInsert := []NewRecord{
	{Name: "Record A", Value: 123.45, Notes: "First record"},
	{Name: "Record B", Value: 67.89}, // Notes 为空字符串
}

// 执行插入 (需要先创建表)
// err = client.InsertStructs(ctx, tableName, recordsToInsert)
// if err != nil {
//  log.Fatalf("InsertStructs 失败: %v", err)
// }
// log.Println("从结构体插入数据成功")
```

**关键点:**

*   `data` 参数可以是结构体切片或指向结构体切片的指针。
*   根据结构体字段的 `opio`/`db` 标签或字段名 (忽略大小写) 自动映射到数据库列。
*   标签为 `"-"` 或未导出的字段会被忽略。

### 从结构体更新数据 (`client.UpdateStruct`)

根据结构体实例的值更新匹配的行。

```go
// 定义要更新的结构体
type RecordToUpdate struct {
	ID    int32   `opio:"-"` // 标签为 "-", 此字段不用于更新
	Name  string  `opio:"NAME"`
	Value float64 `opio:"VALUE"`
}

tableName := "test_struct_insert"
record := RecordToUpdate{
	ID:    1, // 这个 ID 不会用于更新内容，仅用于标识记录
	Name:  "Updated Record A",
	Value: 543.21,
}

filters := []opio.Filter{
	*opio.NewFilter("ID", opio.OperEQ, "1", opio.RelationAnd), // 假设用 ID=1 作为更新条件
}

// 执行更新 (需要表存在且有匹配数据)
// err = client.UpdateStruct(ctx, tableName, record, filters) // 传入结构体实例
// if err != nil {
//  log.Fatalf("UpdateStruct 失败: %v", err)
// }
// log.Println("从结构体更新数据成功")
```

**关键点:**

*   `data` 参数可以是结构体实例或指向结构体的指针。
*   只有带 `opio`/`db` 标签的导出字段会被用于构建更新内容。
*   **必须**提供 `filters` 参数来指定更新条件，防止意外更新全表。

### 按 ID 删除数据 (`client.DeleteByID`)

提供一个按单个 ID 删除记录的快捷方式。

```go
tableName := "test_struct_insert"
idColumn := "ID" // 假设 ID 是主键列
recordIDToDelete := int32(2) // 要删除的记录 ID

// 执行删除 (需要表存在且有匹配数据)
// err = client.DeleteByID(ctx, tableName, idColumn, recordIDToDelete)
// if err != nil {
//  log.Fatalf("DeleteByID 失败: %v", err)
// }
// log.Println("按 ID 删除数据成功")
```

**关键点:**

*   `idColumn` 指定用作删除条件的列名。
*   `id` 是要删除的记录的 ID 值。
*   内部会构建一个 `OperEQ` 的过滤器并调用 `client.Delete`。

## 6. 数据修改 (V2 风格 - 原始方法)

### 插入数据 (`client.Insert`)

```go
tableName := "test_insert_table" // 假设此表存在 (例如: col_int INT, col_str VARCHAR, col_float FLOAT)
insertData := []map[string]interface{}{
	{"col_int": 101, "col_str": "client_test1", "col_float": 101.1},
	{"col_int": 102, "col_str": "client_test2", "col_float": 102.2, "extra_col": "会被忽略"}, // extra_col 会被忽略，因为表结构中没有
}

// 执行插入 (需要先创建表)
// err = client.Insert(ctx, tableName, insertData)
// if err != nil {
//  log.Fatalf("插入数据失败: %v", err)
// }
// log.Println("数据插入成功")
```

**关键点:**

*   `data` 是一个 `[]map[string]interface{}`，每个 map 代表一行。
*   `Insert` 方法会尝试从第一行数据推断列名和类型。确保数据结构基本一致。
*   如果 map 中的键在目标表中不存在，该列会被忽略。

### 更新数据 (`client.Update`)

```go
tableName := "test_insert_table"
updates := map[string]interface{}{
	"col_str":   "updated_value",
	"col_float": 999.9,
}
filters := []opio.Filter{
	*opio.NewFilter("col_int", opio.OperEQ, "101", opio.RelationAnd), // 更新 col_int = 101 的行
}

// 执行更新 (需要表存在且有匹配数据)
// err = client.Update(ctx, tableName, updates, filters)
// if err != nil {
//  log.Fatalf("更新数据失败: %v", err)
// }
// log.Println("数据更新成功")
```

**关键点:**

*   `updates` map 定义了要更新的列和新值。
*   `filters` 定义了更新的条件。如果 `filters` 为空，可能会更新所有行 (危险！)。

### 删除数据 (`client.Delete`)

```go
tableName := "test_insert_table"
filters := []opio.Filter{
	*opio.NewFilter("col_str", opio.OperLike, "'%test%'", opio.RelationAnd), // 删除 col_str 包含 'test' 的行
}

// 执行删除 (需要表存在且有匹配数据)
// err = client.Delete(ctx, tableName, filters)
// if err != nil {
//  log.Fatalf("删除数据失败: %v", err)
// }
// log.Println("数据删除成功")
```

**关键点:**

*   `filters` 定义了删除的条件。
*   **警告:** 如果 `filters` 为空，可能会删除表中的所有数据，请务必谨慎！(建议在应用层添加检查)。

## 7. 实时数据订阅 (`client.Subscribe`)

订阅允许实时接收特定数据的变化。

```go
tableName := "Realtime" // 通常订阅实时表
keyColumn := "ID"       // 使用点位 ID 作为订阅键
initialKeys := []int32{1024, 1025} // 初始订阅的点位 ID 列表 (类型需匹配 keyColumn)

subOpts := &opio.SubscribeOptions{
	Snapshot: true, // 订阅时获取一次当前值的快照
}

// 创建订阅
subscription, err := client.Subscribe(ctx, tableName, keyColumn, initialKeys, subOpts)
if err != nil {
	log.Fatalf("创建订阅失败: %v", err)
}
log.Println("订阅已创建")

// 启动一个 goroutine 来处理订阅事件
go func() {
	eventChan := subscription.Events() // 获取事件通道
	for event := range eventChan {     // 循环读取事件，通道关闭时循环结束
		if event.Err != nil {
			log.Printf("订阅错误: %v", event.Err)
			// 可以根据错误类型决定是否需要重新订阅或关闭
			// if errors.Is(event.Err, some_disconnect_error) {
			//  subscription.Close() // 关闭当前订阅
			//  break
			// }
		} else {
			log.Printf("收到订阅数据: %v", event.Data)
			// 处理 event.Data (map[string]interface{})
		}
	}
	log.Println("订阅事件处理 goroutine 结束")
}()

// 订阅期间可以动态添加或移除 Key
time.Sleep(5 * time.Second) // 等待接收一些数据
additionalKeys := []int32{1026}
err = subscription.AddKeys(additionalKeys)
if err != nil {
	log.Printf("动态添加 Key 失败: %v", err)
} else {
	log.Printf("已添加 Key: %v", additionalKeys)
}

time.Sleep(5 * time.Second)
err = subscription.RemoveKeys(additionalKeys)
if err != nil {
	log.Printf("动态移除 Key 失败: %v", err)
} else {
	log.Printf("已移除 Key: %v", additionalKeys)
}

// 等待一段时间或直到程序退出
// time.Sleep(20 * time.Second)

// 关闭订阅 (通常由创建订阅时传入的 context 控制，或显式调用)
// log.Println("准备关闭订阅...")
// err = subscription.Close()
// if err != nil {
//  log.Printf("关闭订阅时出错: %v", err)
// }
// log.Println("订阅已关闭")

// 等待 context 超时或取消，这将自动关闭订阅
<-ctx.Done()
log.Println("Context 完成，订阅将自动关闭")
// 短暂等待 goroutine 退出
time.Sleep(1 * time.Second)

```

**关键点:**

*   `client.Subscribe` 创建一个订阅会话。`keys` 参数必须是 `[]int32`, `[]int64`, 或 `[]string`。
*   返回的 `*Subscription` 对象用于管理订阅。
*   `subscription.Events()` 返回一个只读通道 (`<-chan SubscriptionEvent`)，用于接收数据或错误。
*   `SubscriptionEvent` 包含数据 (`Data map[string]interface{}`) 或错误 (`Err error`)。
*   需要启动一个 goroutine 来循环处理来自 `Events()` 通道的数据。
*   `subscription.Close()` 用于显式关闭订阅。当创建订阅时传入的 `context` 被取消时，订阅也会自动关闭，并关闭事件通道。
*   `subscription.AddKeys()` 和 `subscription.RemoveKeys()` 可以动态修改订阅的键列表。

## 8. V3 API 操作

`Client` 还封装了 V3 风格的 API，用于直接读写实时值、历史值和统计值。这些方法通常使用特定的结构体 (`opio.Value`, `opio.Archive`, `opio.Stat`)。

### 读写实时数据

```go
pointIDs := []int32{1024, 1025}

// --- 写入 ---
writeTime := time.Now()
valuesToWrite := make([]opio.Value, len(pointIDs))
for i, id := range pointIDs {
	valuesToWrite[i] = opio.Value{
		ID: id,
		TM: int32(writeTime.Unix()),
		DS: 0,                   // 状态
		AV: float64(i+1) * 11.1, // 值
	}
}
err = client.WriteRealtime(ctx, valuesToWrite)
if err != nil {
	log.Fatalf("写入实时数据失败: %v", err)
}
log.Println("实时数据写入成功")

time.Sleep(200 * time.Millisecond) // 短暂等待

// --- 读取 ---
valuesToRead := make([]opio.Value, len(pointIDs))
for i, id := range pointIDs {
	valuesToRead[i] = opio.Value{ID: id} // 读取时只需提供 ID
}
err = client.ReadRealtime(ctx, valuesToRead)
if err != nil {
	log.Fatalf("读取实时数据失败: %v", err)
}

// 处理读取结果 (填充在 valuesToRead 中)
for _, v := range valuesToRead {
	log.Printf("读取到实时数据: ID=%d, TM=%d, DS=%d, AV=%v", v.ID, v.TM, v.DS, v.AV)
}
```

### 读写历史数据

```go
pointIDs := []int32{1024, 1025}

// --- 写入 ---
writeTime := time.Now()
archivesToWrite := make([]*opio.Archive, len(pointIDs))
for i, id := range pointIDs {
	archivesToWrite[i] = &opio.Archive{
		ID:   id,
		Type: opio.TypeR8, // 假设是浮点数类型
		Data: []opio.Value{
			{TM: int32(writeTime.Add(-2 * time.Minute).Unix()), DS: 0, AV: float64(i+1) * 10.0},
			{TM: int32(writeTime.Add(-1 * time.Minute).Unix()), DS: 0, AV: float64(i+1) * 10.5},
		},
	}
}
err = client.WriteArchive(ctx, archivesToWrite, false) // false: 不使用缓存
if err != nil {
	log.Fatalf("写入历史数据失败: %v", err)
}
log.Println("历史数据写入成功")

time.Sleep(200 * time.Millisecond)

// --- 读取 ---
beginTime := writeTime.Add(-5 * time.Minute)
endTime := writeTime.Add(1 * time.Minute)
readArchives, err := client.ReadArchive(ctx, pointIDs, opio.ModeRaw, beginTime, endTime, 0) // 读取原始值
if err != nil {
	log.Fatalf("读取历史数据失败: %v", err)
}

// 处理读取结果
for _, ar := range readArchives {
	log.Printf("读取到历史数据: ID=%d, Type=%d, 点数=%d", ar.ID, ar.Type, len(ar.Data))
	for j, v := range ar.Data {
		log.Printf("  点 %d: TM=%d, DS=%d, AV=%v", j, v.TM, v.DS, v.AV)
	}
}
```

### 读取统计数据

```go
pointIDs := []int32{1024, 1025}
beginTime := time.Now().Add(-10 * time.Minute)
endTime := time.Now()
interval := int32(60) // 1 分钟间隔

// 读取平均值
stats, err := client.ReadStat(ctx, pointIDs, opio.ModeAvg, beginTime, endTime, interval)
if err != nil {
	log.Fatalf("读取统计数据失败: %v", err)
}

// 处理读取结果
for _, st := range stats {
	log.Printf("读取到统计数据: ID=%d, Type=%d, 点数=%d", st.ID, st.Type, len(st.Data))
	for j, v := range st.Data {
		// StatValue 结构包含 Avg, Max, Min, Start, End, Total 等字段
		log.Printf("  统计值 %d: TM=%d, Avg=%v, Max=%v, Min=%v", j, v.TM, v.Avg, v.Max, v.Min)
	}
}
```

**关键点:**

*   V3 API 使用特定的结构体 (`Value`, `Archive`, `Stat`) 来传递数据。
*   `ReadArchive` 和 `ReadStat` 需要指定查询模式 (`mode`) 和时间范围 (`begin`, `end`)，以及可能的间隔 (`interval`)。模式常量定义在 `api_v3.go` 中 (例如 `opio.ModeRaw`, `opio.ModeAvg`, `opio.ModeMax` 等)。

## 9. Context 和错误处理

*   所有 `Client` 的方法都接受 `context.Context` 作为第一个参数。这允许你控制操作的超时或取消。
*   方法在出错时返回标准的 Go `error`。检查返回的 `error` 是否为 `nil` 来判断操作是否成功。可以使用 `errors.Is` 或类型断言来处理特定类型的错误（如果需要）。
