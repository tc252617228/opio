# opio 包 - GormClient 使用指南 (中文)

`opio` 包提供了一个 `GormClient` 类型，旨在简化与 GORM 支持的数据库（如 MySQL）以及 OpenPlant Opio 服务的交互。它结合了 GORM 的 ORM 功能和 Opio 的实时数据订阅、V3 API 数据读写能力。

## 1. 安装

```bash
go get d:/synchronous/BaiduSyncdisk/code/opio
# 或者在你的 go.mod 文件中引用本地路径 (如果适用)
# replace opio => d:/synchronous/BaiduSyncdisk/code/opio
```
然后在你的 Go 代码中导入：
```go
import "opio"
```

## 2. 连接与关闭

使用 `opio.Connect` 函数同时建立 GORM 数据库连接和用于订阅/V3 API 的 Opio 连接。

```go
package main

import (
	"context"
	"errors" // 引入 errors 包用于错误检查
	"fmt"
	"log"
	"time"

	"opio" // 导入本地模块
)

func main() {
	// --- 配置参数 (请替换为你的实际环境配置) ---
	gormDSN := "sis:openplant@tcp(10.75.39.143:3306)/opio?charset=utf8mb4&parseTime=True&loc=Local" // GORM DSN
	opioHost := "10.75.39.143"   // OpenPlant 服务器主机名或 IP
	opioPort := 8200           // OpenPlant 服务器端口
	opioUser := "sis"            // 用户名
	opioPass := "openplant"      // 密码
	connectTimeout := 10 * time.Second // 连接超时
	operationTimeout := 30 * time.Second // 后续操作的默认超时

	// --- 连接 ---
	// 使用 context 控制连接和后续操作的生命周期
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel() // 确保在 main 函数退出时取消 context

	client, err := opio.Connect(ctx, gormDSN, opioHost, opioPort, opioUser, opioPass, connectTimeout)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	log.Println("连接成功!")

	// --- 使用 Client ---
	// ... 在这里执行数据库查询、订阅、V3 API 操作等 ...
	// 例如，查询一个点位
	var point opio.GormPoint
	err = client.FindByID(ctx, &point, 1024) // 假设查询 ID 为 1024 的点
	if err != nil {
		if errors.Is(err, opio.ErrRecordNotFound) {
			log.Println("未找到点位 1024")
		} else {
			log.Printf("查询点位失败: %v", err)
		}
	} else {
		log.Printf("查询到点位: ID=%d, 名称=%s", point.ID, point.PN)
	}


	// --- 关闭 ---
	// 通常不需要显式调用 Close，当上面的 ctx 超时或被 cancel 时，连接会自动关闭。
	// 如果需要提前关闭，可以调用 cancel() 或显式调用 client.Close()
	// err = client.Close()
	// if err != nil && !errors.Is(err, opio.ErrConnectionClosed) {
	//     log.Printf("关闭客户端时出错: %v", err)
	// }

	log.Println("操作完成或超时，等待 context 取消...")
	<-ctx.Done() // 等待 context 完成
	log.Println("Context 完成，客户端应已关闭")
	// 可能需要短暂等待后台任务（如关闭连接）完成
	time.Sleep(1 * time.Second)
}
```

**关键点:**

*   `opio.Connect` 需要 GORM DSN 和 Opio 连接参数。
*   使用 `context.Context` 管理连接生命周期和操作超时/取消。
*   `client.Close()` 用于显式关闭，但通常由 context 管理。

## 3. GORM 数据库操作

`GormClient` 封装了常见的 GORM 操作。

### 创建记录 (`client.Create`)

```go
newPoint := &opio.GormPoint{
	PN: "MyNewPoint",
	ED: "A test point created via GormClient",
	RT: 1, // 示例类型
	// ... 设置其他必要字段 ...
}
err := client.Create(ctx, newPoint)
// ... 错误处理 ...
```

### 按 ID 查询 (`client.FindByID`)

```go
var point opio.GormPoint
err := client.FindByID(ctx, &point, 1024)
// ... 错误处理 (包括 opio.ErrRecordNotFound) ...
```

### 条件查询 (`client.Query`)

```go
var points []opio.GormPoint
err := client.Query(ctx, &points, "RT = ? AND PN LIKE ?", 1, "My%")
// ... 错误处理 ...
```

### 更新记录 (`client.Update`)

默认更新所有字段（需要模型包含主键）。

```go
var pointToUpdate opio.GormPoint
err := client.FindByID(ctx, &pointToUpdate, 1024)
if err == nil {
	pointToUpdate.ED = "Updated description"
	err = client.Update(ctx, &pointToUpdate)
	// ... 错误处理 ...
}
```

### 删除记录 (`client.Delete`)

```go
err := client.Delete(ctx, &opio.GormPoint{}, 1025)
// ... 错误处理 (包括 opio.ErrRecordNotFound) ...
```

## 4. 原始 SQL 执行

### 执行非查询 SQL (`client.ExecSQL`)

```go
sql := "UPDATE point SET ED = ? WHERE ID = ?"
rowsAffected, err := client.ExecSQL(ctx, sql, "New Description via SQL", 1024)
// ... 错误处理 ...
```

### 执行查询 SQL (`client.QuerySQL`)

```go
type SimplePoint struct { ID int32; PN string }
var results []SimplePoint
sql := "SELECT ID, PN FROM point WHERE RT = ? LIMIT ?"
err := client.QuerySQL(ctx, &results, sql, 1, 5)
// ... 错误处理 ...
```

## 5. 实时数据订阅 (`client.Subscribe`)

订阅 Opio 表（通常是 `Realtime`）的数据变化。

```go
import "encoding/json" // 需要导入 json 包

// ...

tableName := "Realtime"
keyColumn := "ID"
initialKeys := []int32{1024, 1025} // 初始订阅的点位 ID

subOpts := &opio.SubscribeOptions{
	Snapshot: true, // 获取初始快照
}

// 创建订阅 (使用独立的 context 管理订阅生命周期)
subCtx, subCancel := context.WithCancel(context.Background())
defer subCancel() // 确保订阅最终被取消

subscription, err := client.Subscribe(subCtx, tableName, keyColumn, initialKeys, subOpts)
// ... 错误处理 ...
log.Println("订阅已创建")

// 定义用于接收扫描结果的结构体 (字段名或标签应与 Realtime 表列名对应)
type RealtimeData struct {
	ID int32     `opio:"ID"`
	GN string    `opio:"GN"`
	TM time.Time `opio:"TM"`
	DS int16     `opio:"DS"`
	AV float64   // 假设 AV 列可以转换为 float64
}

// 启动 goroutine 处理事件
go func() {
	eventChan := subscription.Events()
	for event := range eventChan {
		if event.Err != nil {
			log.Printf("订阅错误: %v", event.Err)
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
		}
	}
	log.Println("订阅处理 goroutine 退出")
}()

// ... 模拟运行、动态添加/移除 key、取消订阅等 ...
```

**关键点:**

*   `client.Subscribe` 创建订阅会话。
*   `subscription.Events()` 返回事件通道 `<-chan SubscriptionEvent`。
*   `SubscriptionEvent` 包含 `Data map[string]interface{}` 或 `Err error`。
*   **新增**: `event.Scan(dest interface{})` 可将数据映射到结构体。
*   `event.Data` 可用 `encoding/json.Marshal` 转为 JSON。
*   需要 goroutine 处理事件。通过取消 context 关闭订阅。
*   `AddKeys`/`RemoveKeys` 可动态修改订阅。

## 6. V3 API 操作 (直接读写实时/历史/统计)

`GormClient` 也封装了 V3 风格的 API，用于直接操作数据。

### 读写实时数据 (`client.ReadRealtime`, `client.WriteRealtime`)

```go
// --- 写入 ---
writeTime := time.Now()
valuesToWrite := []opio.Value{
	{ID: 1024, TM: int32(writeTime.Unix()), DS: 0, AV: 11.1},
	{ID: 1025, TM: int32(writeTime.Unix()), DS: 0, AV: 22.2},
}
err = client.WriteRealtime(ctx, valuesToWrite)
// ... 错误处理 ...

// --- 读取 ---
valuesToRead := []opio.Value{{ID: 1024}, {ID: 1025}} // 只需提供 ID
err = client.ReadRealtime(ctx, valuesToRead)
if err == nil {
	for _, v := range valuesToRead {
		log.Printf("读取到实时数据: %+v", v)
	}
}
// ... 错误处理 ...
```

### 读写历史数据 (`client.ReadArchive`, `client.WriteArchive`)

```go
// --- 写入 ---
writeTime := time.Now()
archivesToWrite := []*opio.Archive{
	{ID: 1024, Type: opio.TypeR8, Data: []opio.Value{
		{TM: int32(writeTime.Add(-2 * time.Minute).Unix()), DS: 0, AV: 10.0},
		{TM: int32(writeTime.Add(-1 * time.Minute).Unix()), DS: 0, AV: 10.5},
	}},
}
err = client.WriteArchive(ctx, archivesToWrite, false) // false: 不使用缓存
// ... 错误处理 ...

// --- 读取 ---
pointIDs := []int32{1024}
beginTime := time.Now().Add(-5 * time.Minute)
endTime := time.Now()
readArchives, err := client.ReadArchive(ctx, pointIDs, opio.ModeRaw, beginTime, endTime, 0) // 读取原始值
if err == nil {
	for _, ar := range readArchives {
		log.Printf("读取到历史数据: ID=%d, 点数=%d", ar.ID, len(ar.Data))
		// ... 处理 ar.Data ...
	}
}
// ... 错误处理 ...
```

### 读取统计数据 (`client.ReadStat`)

```go
pointIDs := []int32{1024}
beginTime := time.Now().Add(-10 * time.Minute)
endTime := time.Now()
interval := int32(60) // 1 分钟间隔

stats, err := client.ReadStat(ctx, pointIDs, opio.ModeAvg, beginTime, endTime, interval) // 读取平均值
if err == nil {
	for _, st := range stats {
		log.Printf("读取到统计数据: ID=%d, 点数=%d", st.ID, len(st.Data))
		// ... 处理 st.Data (包含 Avg, Max, Min 等字段) ...
	}
}
// ... 错误处理 ...
```

**关键点:**

*   V3 API 使用特定的结构体 (`Value`, `Archive`, `Stat`)。
*   `ReadArchive` 和 `ReadStat` 需要指定查询模式 (`mode`)、时间范围和可能的间隔 (`interval`)。模式常量定义在 `api_v3.go` 中。

## 7. 配置与日志

*   `client.SetDefaultTimeout(duration)`: 设置 GORM 操作的默认超时。
*   `client.SetLogger(logger)`: 设置日志记录器。
*   `client.SetGormLogLevel(level)`: 设置 GORM 的日志级别 (例如 `gormlogger.Info`, `gormlogger.Warn`)。

## 8. 错误处理

检查方法返回的 `error`。常见的特定错误包括：

*   `opio.ErrConnectionClosed`: 连接已关闭。
*   `opio.ErrRecordNotFound`: GORM 查询未找到记录。
*   `opio.ErrTimeout`: 操作超时（通常由 context 控制）。
*   `opio.ErrOpioServer` / `*opio.OpioServerError`: 订阅或 V3 API 操作时服务器返回错误。

使用 `errors.Is()` 来检查特定错误类型。
