# opio - Go 语言 OpenPlant 客户端库

[![Go Reference](https://pkg.go.dev/badge/github.com/tc252617228/opio.svg)](https://pkg.go.dev/github.com/tc252617228/opio)

`opio` 是一个用于与 OpenPlant 实时/历史数据库服务器交互的 Go 语言客户端库，支持高性能的数据读写、SQL 执行和实时订阅。适用于工业自动化、数据采集、监控等场景。

---

## 目录

- [简介](#简介)
- [安装](#安装)
- [快速开始](#快速开始)
- [功能详解](#功能详解)
  - [V2 通用数据操作](#v2-通用数据操作)
  - [V3 直接数据访问](#v3-直接数据访问)
  - [SQL 执行](#sql-执行)
  - [实时数据订阅](#实时数据订阅)
- [常见问题](#常见问题)
- [贡献指南](#贡献指南)
- [许可证](#许可证)
- [联系方式](#联系方式)

---

## 简介

opio 提供了对 OpenPlant 数据库的高效访问能力，支持：
- 通用表结构的增删改查（V2 API）
- 实时/历史/统计数据的高性能读写（V3 API）
- SQL 语句执行
- 实时数据订阅与推送

---

## 安装

```bash
go get github.com/tc252617228/opio
```

在你的 Go 代码中导入：

```go
import "github.com/tc252617228/opio"
```

---

## 快速开始

```go
package main

import (
	"log"
	"time"
	"context"
	"github.com/tc252617228/opio"
)

func main() {
	// 连接 OpenPlant 服务器
	client, err := opio.Connect(context.Background(), "127.0.0.1", 8200, "user", "password", 10*time.Second)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	// 查询点位信息（结构体字段需加 opio 标签）
	var point struct {
		ID int32  `opio:"ID"`
		PN string `opio:"PN"`
	}
	err = client.FindByID(context.Background(), &point, opio.TablePoint, "ID", 1001)
	if err != nil {
		log.Printf("查询失败: %v", err)
	} else {
		log.Printf("点位信息: %+v", point)
	}
}
```

---

## 功能详解

### V2 通用数据操作

V2 API 提供了基于表名和过滤器的通用 CRUD 接口，适合结构化数据操作。

#### 创建记录
```go
// 插入多条记录
records := []map[string]interface{}{
	{"PN": "TestPoint1", "ED": "描述1", "RT": 1, "EU": "kg"},
	{"PN": "TestPoint2", "ED": "描述2", "RT": 1, "EU": "m"},
}
err := client.Create(ctx, opio.TablePoint, records)
```

#### 按 ID 查询
```go
var point struct {
	ID int32  `opio:"ID"`
	PN string `opio:"PN"`
}
err := client.FindByID(ctx, &point, opio.TablePoint, "ID", 1001)
```

#### 条件查询
```go
var points []struct {
	ID int32  `opio:"ID"`
	PN string `opio:"PN"`
}
opts := &opio.QueryOptions{
	Filters: []opio.Filter{
		*opio.NewFilter("RT", opio.OperEQ, "1", opio.RelationAnd),
	},
	OrderBy: "ID DESC",
	Limit:   "10",
}
err := client.Query(ctx, &points, opio.TablePoint, nil, opts)
```

#### 更新/删除
```go
// 更新
updates := map[string]interface{}{"ED": "新描述"}
filters := []opio.Filter{*opio.NewFilter("PN", opio.OperEQ, "TestPoint1", opio.RelationAnd)}
err := client.Update(ctx, opio.TablePoint, updates, filters)

// 删除
err := client.Delete(ctx, opio.TablePoint, filters)
```

---

### V3 直接数据访问

V3 API 适合高性能实时/历史/统计数据读写。

#### 实时数据读写
```go
// 写入实时数据
values := []opio.Value{{ID: 1001, TM: int32(time.Now().Unix()), DS: 0, AV: 123.45}}
err := client.WriteRealtime(ctx, values)

// 读取实时数据
readVals := make([]opio.Value, len(values))
for i, v := range values { readVals[i] = opio.Value{ID: v.ID} }
err := client.ReadRealtime(ctx, readVals)
```

#### 历史数据读写
```go
// 写入历史数据
archives := []*opio.Archive{{ID: 1001, Type: opio.TypeR8, Data: []opio.Value{{TM: int32(time.Now().Unix()), DS: 0, AV: 100.1}}}}
err := client.WriteArchive(ctx, archives, false)

// 读取历史数据
begin, end := time.Now().Add(-1*time.Hour), time.Now()
result, err := client.ReadArchive(ctx, []int32{1001}, opio.ModeRaw, begin, end, 0)
```

#### 统计数据读取
```go
stats, err := client.ReadStat(ctx, []int32{1001}, opio.ModeAvg, begin, end, 60)
```

---

### SQL 执行

> **注意：SQL 执行不支持参数化，存在 SQL 注入风险，推荐优先使用 V2/V3 API。**

#### 执行非查询 SQL
```go
sql := "UPDATE point SET ED = '新描述' WHERE ID = 1001"
_, err := client.ExecSQL(ctx, sql)
```

#### 执行查询 SQL
```go
type SimplePoint struct {
	ID int32  `opio:"ID"`
	PN string `opio:"PN"`
}
var results []*SimplePoint
sql := "SELECT ID, PN FROM point WHERE ID = 1001"
err := client.QuerySQL(ctx, &results, sql)
```

---

### 实时数据订阅

支持订阅表数据变更，自动推送事件。

```go
subOpts := &opio.SubscribeOptions{Snapshot: true}
subCtx, subCancel := context.WithCancel(context.Background())
subscription, err := client.Subscribe(subCtx, opio.TableRealtime, "ID", []int32{1001}, subOpts)
if err != nil { panic(err) }

go func() {
	for event := range subscription.Events() {
		if event.Err != nil {
			// 处理错误
			continue
		}
		// 处理数据 event.Data
	}
}()
// 取消订阅
subCancel()
```

---

## 常见问题

- **Q: go get 报 module 路径不一致？**  
  A: 请确保 go.mod 中 module 路径为 `github.com/tc252617228/opio`，并用标准 go get 安装。

- **Q: 如何贡献代码？**  
  A: 欢迎提交 issue 或 pull request。

- **Q: 如何处理常见错误？**  
  A: 推荐用 `errors.Is(err, opio.ErrXXX)` 判断，详见源码注释。

---

## 贡献指南

1. Fork 本仓库，创建分支。
2. 提交你的修改并发起 Pull Request。
3. 代码需通过 go build/go test。
4. 欢迎补充文档、增加测试用例。

---

## 许可证

本项目基于 MIT License 开源。

---

## 联系方式

如有问题请通过 [GitHub Issues](https://github.com/tc252617228/opio/issues) 反馈。
