# opio 包 - Go 语言 OpenPlant 客户端库

`opio` 包提供了一个 Go 语言客户端库，用于与 OpenPlant 实时/历史数据库服务器进行交互。它支持多种 API 风格，包括通用的 V2 数据操作、直接的 V3 数据访问以及 SQL 执行和实时数据订阅。

## 1. 安装

```bash
# 假设你的项目使用了 Go Modules
# 注意：请将路径替换为实际的 opio 包路径或仓库地址
go get d:/synchronous/BaiduSyncdisk/code/opio
```

然后在你的 Go 代码中导入：

```go
import "opio"
```

## 2. 客户端初始化与管理

### 连接服务器

使用 `opio.Connect` 函数建立与 OpenPlant 服务器的连接。你需要提供服务器地址、端口、用户名、密码以及连接超时时间。建议使用 `context.Context` 来控制连接和后续操作的生命周期。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"opio" // 导入 opio 包
)

func main() {
	// --- 配置参数 (请替换为你的实际环境配置) ---
	opioHost := "10.75.39.200"   // OpenPlant 服务器主机名或 IP
	opioPort := 8200           // OpenPlant 服务器端口
	opioUser := "sis"            // 用户名
	opioPass := "openplant"      // 密码
	connectTimeout := 10 * time.Second // 连接超时
	operationTimeout := 30 * time.Second // 后续操作的默认超时

	// --- 连接 ---
	// 使用 context 控制连接和后续操作的生命周期
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel() // 确保在 main 函数退出时取消 context

	client, err := opio.Connect(ctx, opioHost, opioPort, opioUser, opioPass, connectTimeout)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	log.Println("连接成功!")

	// --- 使用 Client ---
	// ... 在这里执行 V2, V3, SQL 或订阅操作 ...
	// 示例：使用 V2 FindByID 查询点位信息
	var point opio.PointRecord // 使用 v4_models.go 中定义的结构体 (注意：实际应使用 gorm_models.go 或自定义结构)
	err = client.FindByID(ctx, &point, opio.TablePoint, "ID", 624819) // 查询 ID 为 624819 的点
	if err != nil {
		if errors.Is(err, opio.ErrRecordNotFound) {
			log.Println("未找到记录")
		} else {
			log.Printf("V2 FindByID 失败: %v", err)
		}
	} else {
		log.Printf("V2 FindByID 成功: %+v", point)
	}


	// --- 关闭 ---
	// 显式关闭连接（通常在程序结束时）
	err = client.Close()
	if err != nil && !errors.Is(err, opio.ErrConnectionClosed) {
		log.Printf("关闭客户端时出错: %v", err)
	} else {
		log.Println("客户端连接已关闭")
	}

	// 如果使用了带超时的 context，等待其完成也是一种管理方式
	// <-ctx.Done()
	// log.Println("Context 完成")
}
```

### 配置

*   `client.SetDefaultTimeout(duration time.Duration)`: 设置客户端后续操作的默认超时时间。如果操作的 `context` 没有设置截止时间，则会使用此默认值。
*   `client.SetLogger(logger *log.Logger)`: 设置一个 `log.Logger` 实例，用于客户端内部的日志记录（例如，连接错误、订阅回调中的错误等）。

## 3. V2 API (通用数据操作)

V2 API 提供了一套通用的、基于表名和过滤器的 CRUD（创建、读取、更新、删除）接口。这些方法位于 `client.go` 中。

### 创建记录 (`client.Create`)

插入一条或多条记录到指定的表中。数据以 `[]map[string]interface{}` 的形式提供。

```go
dataToInsert := []map[string]interface{}{
	{"PN": "V2_TestPoint1", "ED": "描述1", "RT": 1, "EU": "kg"},
	{"PN": "V2_TestPoint2", "ED": "描述2", "RT": 1, "EU": "m"},
}
// 假设 ctx 是已初始化的 context.Context
// 假设 client 是已连接的 *opio.OpioClient
err := client.Create(ctx, opio.TablePoint, dataToInsert)
if err != nil {
	log.Printf("V2 Create 失败: %v", err)
} else {
	log.Println("V2 Create 成功")
}
```

### 按 ID 查询 (`client.FindByID`)

根据主键 ID 查询单条记录，并将结果扫描到提供的结构体指针 `dest` 中。`dest` 结构体的字段应使用 `opio:"列名"` 标签来映射数据库列。

```go
// 假设 PointRecord 结构体已定义 (可参考 gorm_models.go 或自定义)
type PointRecord struct {
    ID int32     `opio:"ID"`
    PN string    `opio:"PN"`
    ED string    `opio:"ED"`
    // ... 其他字段 ...
}

var point PointRecord
err := client.FindByID(ctx, &point, opio.TablePoint, "ID", 624819) // 查询 ID 为 624819 的点
if err != nil {
	if errors.Is(err, opio.ErrRecordNotFound) {
		log.Println("未找到记录")
	} else {
		log.Printf("V2 FindByID 失败: %v", err)
	}
} else {
	log.Printf("V2 FindByID 成功: %+v", point)
}
```

### 条件查询 (`client.Query`)

根据过滤条件、排序和分页选项查询多条记录，并将结果扫描到提供的结构体切片指针 `dest` 中。

```go
// 假设 PointRecord 结构体已定义
var points []*PointRecord // 查询结果切片
opts := &opio.QueryOptions{
	Filters: []opio.Filter{
		*opio.NewFilter("RT", opio.OperEQ, "1", opio.RelationAnd), // 查询 RT=1 的点
		*opio.NewFilter("PN", opio.OperLike, "V2%", opio.RelationAnd), // 且 PN 以 "V2" 开头
	},
	OrderBy: "ID DESC", // 按 ID 降序
	Limit:   "10",      // 最多返回 10 条
}
// 查询所有列 (columns 传 nil)
err := client.Query(ctx, &points, opio.TablePoint, nil, opts)
if err != nil {
	log.Printf("V2 Query 失败: %v", err)
} else {
	log.Printf("V2 Query 成功，找到 %d 条记录:", len(points))
	for _, p := range points {
		log.Printf("  %+v", *p)
	}
}
```
**`Filter` 结构:**
*   `Left`: 列名
*   `Operator`: 操作符 (如 `opio.OperEQ`, `opio.OperGT`, `opio.OperLike` 等，定义在 `const.go`)
*   `Right`: 要比较的值 (字符串形式)
*   `Relation`: 与下一个过滤条件的关系 (`opio.RelationAnd` 或 `opio.RelationOr`)

### 更新记录 (`client.Update`)

根据过滤器更新匹配的记录。`updates` 参数是一个 `map[string]interface{}`，指定要更新的列和新值。**必须提供过滤器 `filters` 以确保安全。**

```go
updates := map[string]interface{}{
	"ED": "新的描述 (V2 Update)",
	"EU": "UpdatedUnit",
}
filters := []opio.Filter{
	*opio.NewFilter("PN", opio.OperEQ, "V2_TestPoint1", opio.RelationAnd),
}
err := client.Update(ctx, opio.TablePoint, updates, filters)
if err != nil {
	log.Printf("V2 Update 失败: %v", err)
} else {
	log.Println("V2 Update 成功")
}
```

### 删除记录 (`client.Delete`)

根据过滤器删除匹配的记录。**必须提供过滤器 `filters` 以确保安全。**

```go
filters := []opio.Filter{
	*opio.NewFilter("PN", opio.OperLike, "V2_TestPoint%", opio.RelationAnd),
}
err := client.Delete(ctx, opio.TablePoint, filters)
if err != nil {
	log.Printf("V2 Delete 失败: %v", err)
} else {
	log.Println("V2 Delete 成功")
}
```

## 4. V3 API (直接数据访问)

V3 API 提供针对特定数据类型（实时、历史、统计）的直接读写接口，可能具有更好的性能。这些方法位于 `client.go` 中。

### 读写实时数据 (`client.ReadRealtime`, `client.WriteRealtime`)

使用 `opio.Value` 结构体进行交互。

```go
// --- 写入 ---
writeTime := time.Now()
valuesToWrite := []opio.Value{
	{ID: 624819, TM: int32(writeTime.Unix()), DS: 0, AV: 123.45},
	// {ID: otherID, ...},
}
err = client.WriteRealtime(ctx, valuesToWrite)
if err != nil {
    log.Printf("V3 WriteRealtime 失败: %v", err)
}

// --- 读取 ---
idsToRead := []int32{624819}
valuesToRead := make([]opio.Value, len(idsToRead))
for i, id := range idsToRead {
	valuesToRead[i] = opio.Value{ID: id} // 读取时只需提供 ID
}
err = client.ReadRealtime(ctx, valuesToRead) // 读取结果会填充到 valuesToRead 中
if err != nil {
    log.Printf("V3 ReadRealtime 失败: %v", err)
} else {
	for _, v := range valuesToRead {
		log.Printf("读取到实时数据: ID=%d, Time=%d, Status=%d, Value=%f", v.ID, v.TM, v.DS, v.AV)
	}
}
```

### 读写历史数据 (`client.ReadArchive`, `client.WriteArchive`)

使用 `opio.Archive` 结构体（包含 `[]opio.Value`）进行交互。

```go
// --- 写入 ---
writeTime := time.Now()
archivesToWrite := []*opio.Archive{
	{ID: 624819, Type: opio.TypeR8, Data: []opio.Value{ // TypeR8 表示 float64
		{TM: int32(writeTime.Add(-2 * time.Minute).Unix()), DS: 0, AV: 100.1},
		{TM: int32(writeTime.Add(-1 * time.Minute).Unix()), DS: 0, AV: 100.2},
	}},
}
err = client.WriteArchive(ctx, archivesToWrite, false) // cache=false
if err != nil {
    log.Printf("V3 WriteArchive 失败: %v", err)
}

// --- 读取 ---
readIDs := []int32{624819}
beginTime := time.Now().Add(-5 * time.Minute)
endTime := time.Now()
// 读取原始值 (ModeRaw)，不指定间隔 (0)
readArchives, err := client.ReadArchive(ctx, readIDs, opio.ModeRaw, beginTime, endTime, 0)
if err != nil {
    log.Printf("V3 ReadArchive 失败: %v", err)
} else {
	for _, ar := range readArchives {
		log.Printf("读取到历史数据: ID=%d, Type=%d, 点数=%d", ar.ID, ar.Type, len(ar.Data))
		for _, v := range ar.Data {
			log.Printf("  -> Time=%d, Status=%d, Value=%f", v.TM, v.DS, v.AV)
		}
	}
}
```

### 读取统计数据 (`client.ReadStat`)

使用 `opio.Stat` 结构体（包含 `[]opio.StatVal`）接收结果。

```go
readIDs := []int32{624819}
beginTime := time.Now().Add(-10 * time.Minute)
endTime := time.Now()
interval := int32(60) // 1 分钟间隔

// 读取平均值 (ModeAvg)
stats, err := client.ReadStat(ctx, readIDs, opio.ModeAvg, beginTime, endTime, interval)
if err != nil {
    log.Printf("V3 ReadStat 失败: %v", err)
} else {
	for _, st := range stats {
		log.Printf("读取到统计数据: ID=%d, Type=%d, 点数=%d", st.ID, st.Type, len(st.Data))
		for _, sv := range st.Data { // sv 是 opio.StatVal 类型
			log.Printf("  -> Time=%d, Status=%d, Avg=%f", sv.Time, sv.Status, sv.Avg)
		}
	}
}
```
**注意**: `ReadArchive` 和 `ReadStat` 的 `mode` 参数决定了查询的数据类型（原始值、平均值、最大值等），具体常量定义在 `api_v3.go` 中 (如 `opio.ModeRaw`, `opio.ModeAvg`)。

## 5. SQL 执行

可以直接执行 SQL 语句，方法位于 `client_sql.go` 中。**强烈建议优先使用 V2 或 V3 API**，因为 SQL 执行方法**不支持参数化查询**，存在 SQL 注入风险。

### 执行非查询 SQL (`client.ExecSQL`)

用于执行 `INSERT`, `UPDATE`, `DELETE` 等不返回结果集的 SQL。

```go
// 示例：更新描述 (不推荐，优先使用 V2 Update)
sql := fmt.Sprintf("UPDATE point SET ED = 'New Desc via SQL' WHERE ID = %d", 624819)
_, err := client.ExecSQL(ctx, sql) // 注意：没有 args 参数
if err != nil {
    log.Printf("ExecSQL 失败: %v", err)
}
```

### 执行查询 SQL (`client.QuerySQL`)

用于执行 `SELECT` 语句，并将结果扫描到 `dest`（必须是结构体切片的指针）。

```go
type SimplePoint struct {
	ID int32  `opio:"ID"`
	PN string `opio:"PN"`
}
var results []*SimplePoint
sql := fmt.Sprintf("SELECT ID, PN FROM point WHERE ID = %d", 624819)
err := client.QuerySQL(ctx, &results, sql) // 注意：没有 args 参数
if err != nil {
    log.Printf("QuerySQL 失败: %v", err)
} else {
    // 处理 results
	for _, p := range results {
		log.Printf("QuerySQL Result: %+v", *p)
	}
}
```

## 6. 实时数据订阅 (`client.Subscribe`)

订阅指定表中符合条件的数据变更。方法位于 `client.go` 中。

```go
import "encoding/json" // 需要导入 json 包

// ...

tableName := opio.TableRealtime // 通常订阅实时表
keyColumn := "ID"             // 通常按 ID 订阅
initialKeys := []int32{624819} // 初始订阅的点位 ID

subOpts := &opio.SubscribeOptions{
	Snapshot: true, // 获取初始快照数据
}

// 创建订阅 (使用独立的 context 管理订阅生命周期)
subCtx, subCancel := context.WithCancel(context.Background())
defer subCancel() // 确保订阅最终被取消

subscription, err := client.Subscribe(subCtx, tableName, keyColumn, initialKeys, subOpts)
if err != nil {
    log.Fatalf("创建订阅失败: %v", err)
}
log.Println("订阅已创建")

// 定义用于接收扫描结果的结构体
type RealtimeData struct {
	ID        int32       `opio:"ID"`
	Timestamp time.Time   `opio:"TM"` // 注意：Scan 会尝试将 int32 时间戳转为 time.Time
	Value     float64     `opio:"AV"` // 假设 AV 是 float64
	Status    int16       `opio:"DS"`
	GN        string      `opio:"GN"`
}

// 启动 goroutine 处理事件
go func() {
	eventChan := subscription.Events()
	for event := range eventChan { // 通道关闭时循环会自动结束
		if event.Err != nil {
			log.Printf("订阅错误: %v", event.Err)
			// 根据错误类型决定是否需要重新订阅或退出
			if errors.Is(event.Err, opio.ErrConnectionClosed) || errors.Is(event.Err, opio.ErrSubscriptionClosed){
				log.Println("订阅连接关闭，退出处理 goroutine")
				return // 连接或订阅关闭，退出 goroutine
			}
			continue
		}

		// --- 处理方式一：直接使用 map ---
		log.Printf("收到原始数据 (map): %v", event.Data)

		// --- 处理方式二：转换为 JSON ---
		jsonData, jsonErr := json.Marshal(event.Data)
		if jsonErr == nil {
			log.Printf("收到的 JSON 数据: %s", string(jsonData))
		}

		// --- 处理方式三：扫描到结构体 ---
		var rtData RealtimeData
		scanErr := event.Scan(&rtData) // 使用 Scan 方法
		if scanErr == nil {
			log.Printf("扫描到的结构体数据: %+v", rtData)
			// 在这里处理具体的数据变更逻辑
		} else {
            log.Printf("扫描数据失败: %v, 原始数据: %v", scanErr, event.Data)
        }
	}
	log.Println("订阅处理 goroutine 退出")
}()

// --- 模拟运行和管理订阅 ---
log.Println("等待订阅事件...")
time.Sleep(30 * time.Second) // 模拟接收事件

// // 动态添加订阅点位 (示例)
// err = subscription.AddKeys([]int32{1026, 1027})
// if err != nil && !errors.Is(err, opio.ErrSubscriptionClosed) { log.Printf("添加订阅键失败: %v", err) }

// // 动态移除订阅点位 (示例)
// err = subscription.RemoveKeys([]int32{624819})
// if err != nil && !errors.Is(err, opio.ErrSubscriptionClosed) { log.Printf("移除订阅键失败: %v", err) }

// // 显式关闭订阅 (或者通过取消 subCtx)
// log.Println("正在关闭订阅...")
// err = subscription.Close()
// if err != nil && !errors.Is(err, opio.ErrSubscriptionClosed) {
//     log.Printf("关闭订阅时出错: %v", err)
// }

// 取消 context 也会导致订阅关闭和 goroutine 退出
subCancel()
log.Println("订阅 context 已取消")
// 等待 goroutine 退出
time.Sleep(1 * time.Second)

```

**关键点:**

*   `client.Subscribe` 创建订阅会话。
*   `subscription.Events()` 返回一个只读通道 `<-chan SubscriptionEvent`，用于接收事件。
*   `SubscriptionEvent` 包含 `Data map[string]interface{}` 或 `Err error`。
*   使用 `event.Scan(dest interface{})` 可以方便地将 `event.Data` 映射到你的 Go 结构体 (`dest` 必须是指针)。
*   需要在一个单独的 goroutine 中处理事件通道，以避免阻塞主线程。
*   通过取消传递给 `Subscribe` 的 `context` 或调用 `subscription.Close()` 来关闭订阅。
*   可以使用 `subscription.AddKeys()` 和 `subscription.RemoveKeys()` 动态修改订阅的键列表。

## 7. 错误处理

在调用 `opio` 包的方法时，务必检查返回的 `error`。一些常见的预定义错误包括：

*   `opio.ErrConnectionClosed`: 客户端连接已关闭。
*   `opio.ErrTimeout`: 操作超时（通常由传递的 `context` 控制）。
*   `opio.ErrOpioServer`: 底层 Opio 服务器返回了一个错误。可以通过 `errors.As` 获取更详细的 `*opio.OpioServerError`。
*   `opio.ErrRecordNotFound`: V2 API 的 `FindByID` 未找到记录时返回（其他查询方法在未找到时通常返回空切片和 `nil` 错误）。
*   `opio.ErrUpdateRequiresFilters`: V2 `Update` 操作缺少必要的过滤条件。
*   `opio.ErrSubscriptionClosed`: 尝试在已关闭的订阅上执行操作。

建议使用 `errors.Is()` 或 `errors.As()` 来检查特定的错误类型。

```go
// 假设 point 变量已定义
err := client.FindByID(ctx, &point, opio.TablePoint, "ID", 99999)
if err != nil {
    if errors.Is(err, opio.ErrRecordNotFound) {
        log.Println("记录未找到")
    } else if errors.Is(err, opio.ErrConnectionClosed) {
        log.Println("连接已关闭")
    } else {
        var serverErr *opio.OpioServerError
        if errors.As(err, &serverErr) {
            log.Printf("服务器错误: Code=%d, Message=%s", serverErr.Code, serverErr.Message)
        } else {
            log.Printf("发生未知错误: %v", err)
        }
    }
}
