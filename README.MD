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

## 6. 常见用法示例（基于底层API，适合高级用户）

### 6.1 基础连接与UUID生成
```go
// 初始化连接
op, err := opio.Init("170.170.10.5", 8200, 60, "sis", "openplant")
if err != nil {
    log.Fatal("连接失败:", err)
}
defer op.Close()

// 生成唯一标识符
uuid := opio.MakeUUID("W3.test.test")
fmt.Println("UUID:", uuid)
```

### 6.2 基础SQL查询与复杂SQL
```go
// 基础SQL查询
op, _ := opio.Init(host, port, 60, user, pwd)
defer op.Close()
_ = op.SetCompressModel(opio.ZIP_MODEL_Frame)
sql := "select * from Realtime limit 10"
req := op.NewRequest(nil)
req.SetID(1)
req.SetService("openplant")
req.SetAction(opio.ActionExecSQL)
req.SetSQL(sql)
_ = req.WriteAndFlush()
res, _ := req.GetResponse()
rs := res.GetDataSet()
// 处理数据集 rs ...
rs.Close()

// 复杂SQL查询
sql2 := "SELECT GN FROM NODE WHERE GN='HNJT.NET';"
req2 := op.NewRequest(nil)
req2.SetID(2)
req2.SetService("openplant")
req2.SetAction(opio.ActionExecSQL)
req2.SetSQL(sql2)
_ = req2.WriteAndFlush()
res2, _ := req2.GetResponse()
rs2 := res2.GetDataSet()
// 处理数据集 rs2 ...
rs2.Close()
```

### 6.3 实时数据插入（按点名/ID/多类型）
```go
// 按点名批量插入多类型实时数据
op, _ := opio.Init(host, port, 60, user, pwd)
defer op.Close()
req := op.NewRequest(nil)
req.SetID(1)
req.SetService("openplant")
req.SetAction(opio.ActionInsert)
table := opio.NewTable("Realtime", 0)
table.AddColumn("GN", opio.VtString, 0)
table.AddColumn("AV", opio.VtObject, 0)
for i := 0; i < 100; i++ {
    gn := fmt.Sprintf("W3.AX.AX%d", i)
    _ = table.SetColumnString(0, gn)
    _ = table.SetColumnObject(1, float64(i))
    table.BindRow()
}
_ = req.SetTable(table)
_ = req.Write()
_ = req.WriteContent(table)
req.Flush()
res, _ := req.GetResponse()
rs := res.GetDataSet()
rs.Close()

// 按ID插入实时数据（支持多类型）
// 先查ID和类型，再写入
req2 := op.NewRequest(nil)
req2.SetID(2)
req2.SetService("openplant")
req2.SetAction(opio.ActionExecSQL)
req2.SetSQL("select ID,RT from Point where RT IN (0,1,2,3,4,5)")
_ = req2.Write()
req2.Flush()
res2, _ := req2.GetResponse()
rs2 := res2.GetDataSet()
idRt := make(map[int32]int8)
for {
    ok, _ := rs2.Next()
    if !ok { break }
    id, _ := rs2.GetInt32(0)
    rt, _ := rs2.GetInt8(1)
    idRt[id] = rt
}
rs2.Close()
req3 := op.NewRequest(nil)
req3.SetID(3)
req3.SetService("openplant")
req3.SetAction(opio.ActionInsert)
table2 := opio.NewTable("Realtime", 0)
table2.AddColumn("ID", opio.VtInt32, 0)
table2.AddColumn("AV", opio.VtObject, 0)
for id, rt := range idRt {
    _ = table2.SetColumnInt32(0, id, 0)
    switch rt {
    case 0, 4:
        _ = table2.SetColumnObject(1, float64(123))
    case 2, 3, 5:
        _ = table2.SetColumnObject(1, 123)
    case 6, 7:
        _ = table2.SetColumnObject(1, "字符串")
    default:
        _ = table2.SetColumnObject(1, true)
    }
    table2.BindRow()
}
_ = req3.SetTable(table2)
_ = req3.Write()
_ = req3.WriteContent(table2)
req3.Flush()
res3, _ := req3.GetResponse()
rs3 := res3.GetDataSet()
rs3.Close()
```

### 6.4 实时数据订阅
```go
// 实时数据订阅
op, _ := opio.Init(host, port, 60, user, pwd)
sub, _ := op.NewSubscribe("Realtime")
ids := []int32{1024, 1025}
_ = sub.InitSubscribe(ids, "ID", func(res *opio.Response) {
    if res.GetErrNo() == 0 {
        ds := res.GetDataSet()
        // 处理数据集 ds ...
    } else {
        fmt.Println("订阅错误:", res.GetErrNo())
    }
})
// 追加订阅
_ = sub.Subscribe([]int32{1026})
// 取消订阅
_ = sub.UnSubscribe([]int32{1025})
// 关闭订阅
sub.Close()
```

### 6.5 批量写入与性能测试
```go
// 批量写入实时数据并统计耗时
start := time.Now()
for i := 0; i < 100; i++ {
    // ... 构造table并写入 ...
}
fmt.Println("批量写入耗时:", time.Since(start))
```

### 6.6 数据查找、过滤、更新、删除
```go
// 查找
op, _ := opio.Init(host, port, 60, user, pwd)
req := op.NewRequest(nil)
req.SetID(1)
req.SetService("openplant")
req.SetAction(opio.ActionFind)
req.SetTableName("Realtime")
_ = req.WriteAndFlush()
res, _ := req.GetResponse()
rs := res.GetDataSet()
// 处理数据集 rs ...
rs.Close()

// 过滤查找
req2 := op.NewRequest(nil)
req2.SetID(2)
req2.SetService("openplant")
req2.SetAction(opio.ActionFind)
req2.SetTableName("Realtime")
req2.Set("Filters", []map[string]interface{}{{"Field": "GN", "Oper": opio.OperLike, "Value": "W3.AX.%"}})
_ = req2.WriteAndFlush()
res2, _ := req2.GetResponse()
rs2 := res2.GetDataSet()
// 处理数据集 rs2 ...
rs2.Close()

// 更新
req3 := op.NewRequest(nil)
req3.SetID(3)
req3.SetService("openplant")
req3.SetAction(opio.ActionUpdate)
req3.SetTableName("Realtime")
req3.Set("Filters", []map[string]interface{}{{"Field": "GN", "Oper": opio.OperLike, "Value": "W3.AX.%"}})
req3.Set("Values", map[string]interface{}{ "AV": 999 })
_ = req3.WriteAndFlush()
res3, _ := req3.GetResponse()
// 检查更新结果 ...

// 删除
req4 := op.NewRequest(nil)
req4.SetID(4)
req4.SetService("openplant")
req4.SetAction(opio.ActionRemove)
req4.SetTableName("Realtime")
req4.Set("Filters", []map[string]interface{}{{"Field": "GN", "Oper": opio.OperLike, "Value": "W3.AX.%"}})
_ = req4.WriteAndFlush()
res4, _ := req4.GetResponse()
// 检查删除结果 ...
```

### 6.7 多线程查找
```go
// 多线程并发查找
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func(idx int) {
        defer wg.Done()
        op, _ := opio.Init(host, port, 60, user, pwd)
        req := op.NewRequest(nil)
        req.SetID(int64(idx))
        req.SetService("openplant")
        req.SetAction(opio.ActionFind)
        req.SetTableName("Realtime")
        _ = req.WriteAndFlush()
        res, _ := req.GetResponse()
        rs := res.GetDataSet()
        // 处理数据集 rs ...
        rs.Close()
        op.Close()
    }(i)
}
wg.Wait()
```

## 7. Context 和错误处理

*   所有 `Client` 的方法都接受 `context.Context` 作为第一个参数。这允许你控制操作的超时或取消。
*   方法在出错时返回标准的 Go `error`。检查返回的 `error` 是否为 `nil` 来判断操作是否成功。可以使用 `errors.Is` 或类型断言来处理特定类型的错误（如果需要）。
