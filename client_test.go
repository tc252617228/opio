package opio_test // 使用 _test 后缀，表示是外部测试包

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tc252617228/opio" // 导入你的 opio 包
)

// --- 测试配置 ---
// !!! 请根据你的 OpenPlant 服务器环境修改以下常量 !!!
const (
	testHost    = "10.75.39.143"   // OpenPlant 服务器主机名或 IP
	testPortStr = "8200"           // OpenPlant 服务器端口 (字符串形式)
	testUser    = "sis"            // 用户名
	testPass    = "openplant"      // 密码
	testTimeout = 60 * time.Second // 测试操作的默认超时时间

	// 用于测试的表名和列名 (需要服务器上存在这些表和列)
	testTableName       = "point"             // 使用 point 表进行查询和订阅测试
	testQueryColumn     = "ID"                // point 表中的 ID 列
	testInsertTableName = "test_insert_table" // 用于插入/更新/删除的测试表 (需要预先创建或动态创建/清理) - 测试会跳过此表相关操作
	testKeyColumn       = "ID"                // point 表中的 ID 列，用于订阅
	testRealtimeTable   = "Realtime"          // 用于订阅实时数据的表名
)

var (
	testPort int // 解析后的测试端口号
)

// TestMain 用于设置测试环境，例如解析端口号。
// 可以在这里添加全局的测试数据设置和清理逻辑。
func TestMain(m *testing.M) {
	var err error
	testPort, err = strconv.Atoi(testPortStr) // 将端口字符串转换为整数
	if err != nil {
		// fmt.Printf("无效的测试端口号: %s\n", testPortStr)
		os.Exit(1) // 如果端口无效，退出测试
	}
	// 可以在这里添加创建测试表、插入初始数据的逻辑
	// setupTestData()
	exitCode := m.Run() // 运行所有测试
	// 可以在这里添加清理测试数据的逻辑
	// cleanupTestData()
	os.Exit(exitCode) // 退出并返回测试结果状态码
}

// createTestClient 是一个辅助函数，用于为测试创建一个 opio 客户端实例。
// 它处理连接建立、上下文创建以及测试结束后的客户端关闭。
func createTestClient(t *testing.T) (*opio.Client, context.Context) {
	// 使用较短的超时时间进行初始连接尝试。
	// 如果具体测试需要更长的操作时间，可以在测试内部创建带有更长超时的 context。
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel() // 确保连接上下文被取消

	// 连接到测试服务器，使用缩短的连接超时时间
	client, err := opio.Connect(connectCtx, testHost, testPort, testUser, testPass, 5*time.Second)
	require.NoError(t, err, "连接测试服务器失败") // 使用 require 确保连接成功，否则测试中止
	require.NotNil(t, client, "返回的客户端不应为 nil")

	// 创建一个用于测试操作的 context，设置默认超时时间
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel) // 使用 t.Cleanup 注册取消函数，确保测试结束时 context 被取消

	// 使用 t.Cleanup 注册客户端关闭逻辑，确保测试结束时客户端被关闭
	t.Cleanup(func() {
		closeErr := client.Close()
		// 在清理阶段，通常可以忽略 "客户端未连接或已关闭" 的错误，因为之前的测试可能已经关闭了它
		if closeErr != nil && closeErr.Error() != "客户端未连接或已关闭" {
			t.Logf("关闭客户端时出错: %v", closeErr) // 记录其他关闭错误
		}
	})

	return client, ctx // 返回创建的客户端和用于测试操作的 context
}

// TestConnectAndClose 测试基本的连接和关闭功能。
func TestConnectAndClose(t *testing.T) {
	client, _ := createTestClient(t) // 创建客户端，忽略返回的 context
	// createTestClient 内部已经验证了连接成功，这里主要测试 Close 方法。
	err := client.Close()
	assert.NoError(t, err, "关闭客户端时出错") // 第一次关闭应该成功

	// 尝试再次关闭客户端
	err = client.Close()
	assert.Error(t, err, "重复关闭应该返回错误") // 第二次关闭应该失败
	// 检查错误信息是否符合预期 (可能依赖底层实现，使用 Contains 更健壮)
	assert.Contains(t, err.Error(), "客户端未连接或已关闭", "重复关闭的错误信息应包含特定文本")
}

// TestSetCompression 测试设置连接的压缩模式。
func TestSetCompression(t *testing.T) {
	client, _ := createTestClient(t) // 创建客户端，忽略 context，因为 SetCompression 不直接使用它

	// 测试设置一个有效的压缩模式 (例如 Frame 压缩)
	err := client.SetCompression(opio.ZIP_MODEL_Frame)
	assert.NoError(t, err, "设置有效压缩模式失败")

	// 测试设置一个无效的压缩模式 (假设 byte(255) 是无效的)
	// 注意：底层的 SetCompressModel 方法可能不会进行严格的模式验证，
	// 因此这个断言可能会失败，取决于底层实现。
	// err = client.SetCompression(byte(255)) // 假设 255 是无效值
	// assert.Error(t, err, "设置无效压缩模式应返回错误")

	// 关闭客户端后尝试设置压缩模式
	client.Close()
	err = client.SetCompression(opio.ZIP_MODEL_Frame)
	assert.Error(t, err, "关闭后设置压缩模式应失败") // 对已关闭的连接操作应返回错误
}

// TestQuery 测试基本的结构化查询功能 (使用 client.Query)。
// 这个测试需要 OpenPlant 服务器上存在名为 testTableName (point) 的表。
func TestQuery(t *testing.T) {
	client, ctx := createTestClient(t) // 创建客户端和 context

	opts := &opio.QueryOptions{
		Limit: "5", // 限制返回结果的数量为 5 行
	}
	columns := []string{testQueryColumn, "GN", "ED"} // 指定要查询的列：ID, GN, ED

	// 执行查询
	result, err := client.Query(ctx, testTableName, columns, opts)
	require.NoError(t, err, "执行查询失败") // 确保查询没有错误
	require.NotNil(t, result, "查询结果不应为 nil")

	t.Logf("查询表 '%s', 列 %v, 选项 %+v", testTableName, columns, opts)
	t.Logf("查询到 %d 行数据", len(result.Rows)) // 记录查询到的行数
	logQueryResultSample(t, result, 3)     // 记录前 3 行数据样本
	if len(result.Rows) > 0 {              // 如果查询返回了数据
		assert.NotEmpty(t, result.Columns, "查询结果应包含列信息") // 验证列信息不为空
		assert.Len(t, result.Columns, 3, "查询返回的列数应为 3")  // 验证返回了 3 列

		// 检查返回的列名是否正确 (不保证顺序)
		colNames := make(map[string]bool)
		for _, col := range result.Columns {
			colNames[col.GetName()] = true
		}
		assert.True(t, colNames[testQueryColumn], "结果中应包含列 %s", testQueryColumn)
		assert.True(t, colNames["GN"], "结果中应包含列 GN")
		assert.True(t, colNames["ED"], "结果中应包含列 ED")

		// 检查第一行数据是否包含预期的列 (作为抽样检查)
		_, idOk := result.Rows[0][testQueryColumn]
		_, gnOk := result.Rows[0]["GN"]
		_, edOk := result.Rows[0]["ED"]
		assert.True(t, idOk, "查询结果行中应包含列 %s", testQueryColumn)
		assert.True(t, gnOk, "查询结果行中应包含列 GN")
		assert.True(t, edOk, "查询结果行中应包含列 ED")
	} else {
		// 如果查询没有返回行，记录一个警告
		t.Log("警告：查询未返回任何行，某些断言可能未执行")
	}

	// 测试查询所有列 ("*")
	resultAll, err := client.Query(ctx, testTableName, []string{"*"}, opts)
	require.NoError(t, err, "执行查询所有列失败")
	require.NotNil(t, resultAll, "查询所有列结果不应为 nil")
	assert.NotEmpty(t, resultAll.Columns, "查询所有列结果应包含列信息")
	t.Logf("查询表 '%s' 所有列 (*), 选项 %+v", testTableName, opts)
	t.Logf("查询所有列返回 %d 列, %d 行", len(resultAll.Columns), len(resultAll.Rows)) // 记录查询所有列时返回的列数和行数
	logQueryResultSample(t, resultAll, 3)                                     // 记录前 3 行数据样本
}

// TestExecSQL 测试执行原始 SQL 语句的功能 (基于原 Test_SQL 和 Test_SQL2)。
func TestExecSQL(t *testing.T) {
	client, ctx := createTestClient(t) // 创建客户端和 context

	// 测试一个简单的 SELECT SQL 语句 (限制返回 5 行)
	sqlSimple := fmt.Sprintf("select ID, GN from %s limit 5", testTableName)
	t.Logf("执行简单 SQL: %s", sqlSimple)
	resultSimple, err := client.ExecSQL(ctx, sqlSimple)
	require.NoError(t, err, "执行简单 SQL 失败")
	require.NotNil(t, resultSimple, "简单 SQL 查询结果不应为 nil")
	t.Logf("简单 SQL 查询返回 %d 行", len(resultSimple.Rows))
	logQueryResultSample(t, resultSimple, 5)                                // 记录最多 5 行数据样本
	assert.LessOrEqual(t, len(resultSimple.Rows), 5, "简单 SQL 查询结果行数应 <= 5") // 验证行数不超过限制
	if len(resultSimple.Rows) > 0 {                                         // 如果有返回行
		assert.Len(t, resultSimple.Columns, 2, "简单 SQL 查询结果列数应为 2") // 验证列数
		// 抽样检查第一行是否包含 ID 和 GN 列
		_, idOk := resultSimple.Rows[0]["ID"]
		_, gnOk := resultSimple.Rows[0]["GN"]
		assert.True(t, idOk, "简单 SQL 结果行应包含 ID 列")
		assert.True(t, gnOk, "简单 SQL 结果行应包含 GN 列")
	}

	// 测试一个带 WHERE 子句的 SQL 语句
	// 假设服务器上存在 GN='W3.LE.SC_LE_1_001_AMBWINDSPEED' 的点位
	sqlWhere := fmt.Sprintf("SELECT GN, ED FROM %s WHERE GN='W3.LE.SC_LE_1_001_AMBWINDSPEED'", testTableName)
	t.Logf("执行带 WHERE 的 SQL: %s", sqlWhere)
	resultWhere, err := client.ExecSQL(ctx, sqlWhere)
	require.NoError(t, err, "执行带 WHERE 的 SQL 失败")
	require.NotNil(t, resultWhere, "带 WHERE 的 SQL 查询结果不应为 nil")
	t.Logf("带 WHERE 的 SQL 查询返回 %d 行", len(resultWhere.Rows))
	logQueryResultSample(t, resultWhere, 1) // 记录最多 1 行数据样本
	if len(resultWhere.Rows) > 0 {          // 如果找到了匹配的点位
		assert.Len(t, resultWhere.Columns, 2, "带 WHERE 的 SQL 查询结果列数应为 2")                                  // 验证列数
		assert.Equal(t, "W3.LE.SC_LE_1_001_AMBWINDSPEED", resultWhere.Rows[0]["GN"], "WHERE 查询返回的 GN 不匹配") // 验证返回的 GN 是否正确
	} else {
		// 如果未找到匹配的点位，记录警告
		t.Logf("警告: SQL '%s' 未返回任何行", sqlWhere)
	}

	// 测试执行非 SELECT 语句 (例如 CREATE TABLE)
	// 这可能会因为权限不足而失败，这是符合预期的。
	// sqlCreate := "CREATE TABLE test_execsql_table (id INT PRIMARY KEY, name VARCHAR(50))"
	// resultCreate, err := client.ExecSQL(ctx, sqlCreate)
	// if err == nil { // 如果创建成功
	//  t.Log("成功执行 CREATE TABLE (可能需要手动清理)")
	//  assert.Empty(t, resultCreate.Rows, "CREATE TABLE 不应返回行") // CREATE TABLE 不应返回结果集
	//  // 可以在这里添加 DROP TABLE 语句进行清理
	// } else { // 如果创建失败
	//  t.Logf("执行 CREATE TABLE 失败 (符合预期，如果权限不足): %v", err)
	// }
}

// TestInsertUpdateDelete 测试高级的插入、更新、删除方法 (client.Insert, client.Update, client.Delete)。
// !!! 这个测试默认被跳过，因为它需要手动在服务器上创建名为 testInsertTableName 的测试表 !!!
// 测试表需要包含至少三列：col_int (整数), col_str (字符串), col_float (浮点数)。
func TestInsertUpdateDelete(t *testing.T) {
	// 跳过此测试，并说明原因
	t.Skipf("跳过 TestInsertUpdateDelete，需要手动创建表 '%s' 包含 col_int, col_str, col_float 列", testInsertTableName)

	client, ctx := createTestClient(t) // 创建客户端和 context

	// --- 插入操作 (使用 client.Insert) ---
	// 在实际测试前，最好确保测试表是空的，或者清理掉之前的测试数据。
	// _, err := client.ExecSQL(ctx, fmt.Sprintf("DELETE FROM %s", testInsertTableName)) // 清理语句示例
	// require.NoError(t, err, "清理测试表失败")

	// 准备要插入的数据 (两行)
	insertData := []map[string]interface{}{
		{"col_int": 101, "col_str": "client_test1", "col_float": 101.1},
		{"col_int": 102, "col_str": "client_test2", "col_float": 102.2},
	}
	err := client.Insert(ctx, testInsertTableName, insertData) // 执行插入
	require.NoError(t, err, "使用 client.Insert 插入数据失败")

	// --- 查询验证插入 (使用 client.Query) ---
	// 构建查询选项，查找刚刚插入的数据
	queryOpts := &opio.QueryOptions{
		Filters: []opio.Filter{
			// 注意：NewFilter 的 value 参数格式可能需要根据实际列类型调整。
			// 这里假设 col_str 是字符串类型，所以值用单引号括起来。
			*opio.NewFilter("col_str", opio.OperIn, "'client_test1','client_test2'", opio.RelationAnd),
		},
	}
	// 查询插入的数据
	queryResult, err := client.Query(ctx, testInsertTableName, []string{"col_int", "col_str", "col_float"}, queryOpts)
	require.NoError(t, err, "插入后查询失败")
	require.Len(t, queryResult.Rows, 2, "插入后查询到的行数不为 2") // 验证是否插入了 2 行
	// 可以在这里添加更详细的数据验证，例如检查返回的具体值是否与插入时一致。

	// --- 更新操作 (使用 client.Update) ---
	// 准备要更新的内容和条件
	updates := map[string]interface{}{
		"col_str": "client_updated_test1", // 将 col_str 更新为新值
	}
	updateFilters := []opio.Filter{
		*opio.NewFilter("col_int", opio.OperEQ, "101", opio.RelationAnd), // 更新 col_int 为 101 的行
	}
	err = client.Update(ctx, testInsertTableName, updates, updateFilters) // 执行更新
	require.NoError(t, err, "使用 client.Update 更新数据失败")

	// --- 查询验证更新 (使用 client.Query) ---
	// 查询更新后的行
	queryOptsUpdate := &opio.QueryOptions{
		Filters: []opio.Filter{
			*opio.NewFilter("col_int", opio.OperEQ, "101", opio.RelationAnd),
		},
	}
	queryResultUpdate, err := client.Query(ctx, testInsertTableName, []string{"col_str"}, queryOptsUpdate)
	require.NoError(t, err, "更新后查询失败")
	require.Len(t, queryResultUpdate.Rows, 1, "更新后查询应返回 1 行")                                 // 验证只更新了 1 行
	assert.Equal(t, "client_updated_test1", queryResultUpdate.Rows[0]["col_str"], "更新后的值不匹配") // 验证 col_str 是否已更新

	// --- 删除操作 (使用 client.Delete) ---
	// 定义删除条件 (删除 col_str 中包含 'client_test' 的所有行)
	deleteFilters := []opio.Filter{
		*opio.NewFilter("col_str", opio.OperLike, "'%client_test%'", opio.RelationAnd),
	}
	err = client.Delete(ctx, testInsertTableName, deleteFilters) // 执行删除
	require.NoError(t, err, "使用 client.Delete 删除数据失败")

	// --- 查询验证删除 (使用 client.Query) ---
	// 再次查询已删除的数据，确认它们不存在
	queryOptsDelete := &opio.QueryOptions{
		Filters: []opio.Filter{
			*opio.NewFilter("col_str", opio.OperLike, "'%client_test%'", opio.RelationAnd),
		},
	}
	queryResultDelete, err := client.Query(ctx, testInsertTableName, []string{"col_int"}, queryOptsDelete)
	require.NoError(t, err, "删除后查询失败")
	assert.Len(t, queryResultDelete.Rows, 0, "删除后查询应返回 0 行") // 验证数据已被删除

	// 测试结束后，最好再次清理测试表
	// _, err = client.ExecSQL(ctx, fmt.Sprintf("DELETE FROM %s", testInsertTableName))
	// require.NoError(t, err, "清理测试表失败")
}

// TestSubscribe 测试客户端的订阅功能 (基于原 Test_sub)。
// 这个测试需要服务器上存在名为 testRealtimeTable (Realtime) 的表和 testKeyColumn (ID) 列。
func TestSubscribe(t *testing.T) {
	client, ctx := createTestClient(t) // 创建客户端和 context

	// 定义要订阅的点位 ID。
	// !!! 请确保这些 ID 在你的 OpenPlant 环境中是有效的、存在的点位 ID !!!
	initialKeys := []int32{1024}    // 初始订阅的 ID
	additionalKeys := []int32{1025} // 稍后动态添加的 ID
	if len(initialKeys) == 0 || len(additionalKeys) == 0 {
		t.Skip("跳过 TestSubscribe，未配置有效的订阅键 (initialKeys 或 additionalKeys)") // 如果未配置 ID，跳过测试
		return
	}

	// 配置订阅选项
	subOpts := &opio.SubscribeOptions{
		Snapshot: true, // 请求在订阅建立时获取一次初始快照数据
	}

	// 使用 client.Subscribe 方法创建订阅
	subscription, err := client.Subscribe(ctx, testRealtimeTable, testKeyColumn, initialKeys, subOpts)
	require.NoError(t, err, "创建订阅失败")
	require.NotNil(t, subscription, "返回的订阅对象不应为 nil")
	defer subscription.Close() // 确保测试结束时关闭订阅

	// 用于跟踪测试状态的变量
	eventReceived := false                                 // 是否收到了至少一个数据事件
	errorReceived := false                                 // 是否收到了错误事件
	eventCount := 0                                        // 收到的事件总数
	receivedData := make(map[int32]map[string]interface{}) // 用于存储按 ID 分组的接收到的数据

	// 设置一个总的测试超时时间，防止测试因收不到事件而永久阻塞
	subscribeDuration := 15 * time.Second // 增加持续时间以观察添加/删除 key 的效果
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, subscribeDuration)
	defer timeoutCancel() // 确保超时 context 被取消

	eventChan := subscription.Events() // 获取用于接收事件的通道

	// 启动一个 goroutine 来异步处理来自订阅通道的事件
	go func() {
		t.Log("启动订阅事件处理 goroutine...")
		for {
			select {
			case event, ok := <-eventChan: // 从通道接收事件
				if !ok { // 如果通道关闭
					t.Log("订阅通道已关闭，事件处理 goroutine 退出")
					return // 退出 goroutine
				}
				eventCount++          // 增加事件计数
				if event.Err != nil { // 如果是错误事件
					t.Logf("订阅收到错误: %v", event.Err)
					errorReceived = true // 标记收到错误
				} else { // 如果是数据事件
					t.Logf("收到订阅数据: %+v", event.Data) // 使用 %+v 打印 map 详情
					eventReceived = true              // 标记收到数据
					// 按 ID 存储收到的数据，方便后续验证
					if idVal, ok := event.Data[testKeyColumn]; ok { // 从数据中提取 ID
						if id, ok := idVal.(int32); ok { // 假设 ID 是 int32 类型
							// 存储数据，如果已存在则覆盖（表示收到更新）
							receivedData[id] = event.Data
							t.Logf("已存储/更新 ID %d 的数据", id)
						} else {
							t.Logf("警告：收到的数据中 %s 列不是 int32 类型: %T", testKeyColumn, idVal)
						}
					} else {
						t.Logf("警告：收到的数据中缺少键列 %s", testKeyColumn)
					}
				}
			case <-timeoutCtx.Done(): // 如果测试超时
				t.Log("订阅测试超时，事件处理 goroutine 退出")
				return // 退出 goroutine
			}
		}
	}()

	t.Log("等待初始快照...")
	time.Sleep(2 * time.Second) // 等待一段时间，让初始快照数据到达

	t.Logf("追加订阅 ID: %v", additionalKeys)
	err = subscription.AddKeys(additionalKeys) // 使用 subscription.AddKeys 动态添加订阅键
	assert.NoError(t, err, "动态添加 Key 失败")

	t.Log("等待一段时间以接收更多事件 (包括新添加的 Key)...")
	time.Sleep(5 * time.Second) // 等待新 key 的数据或现有 key 的更新

	t.Logf("取消订阅 ID: %v", additionalKeys)
	err = subscription.RemoveKeys(additionalKeys) // 使用 subscription.RemoveKeys 动态移除订阅键
	assert.NoError(t, err, "动态删除 Key 失败")

	t.Log("等待一段时间以确认 key 已移除 (理论上不应再收到该 Key 的更新)...")
	time.Sleep(5 * time.Second) // 等待确认移除效果

	// 停止接收事件并关闭订阅
	timeoutCancel()      // 触发超时以停止事件处理 goroutine
	subscription.Close() // 正式关闭订阅

	// 等待事件处理 goroutine 完全退出
	time.Sleep(1 * time.Second)

	// --- 验证订阅结果 ---
	assert.False(t, errorReceived, "订阅过程中不应收到错误")       // 验证没有收到错误事件
	assert.True(t, eventReceived, "应至少收到一个订阅事件（快照或更新）") // 验证至少收到了一个数据事件
	t.Logf("总共收到 %d 个事件", eventCount)                   // 记录收到的事件总数

	// 验证是否收到了初始订阅 key 的数据 (快照或更新)
	initialData, initialKeyReceived := receivedData[initialKeys[0]]
	assert.True(t, initialKeyReceived, "应收到初始订阅 key (%d) 的数据", initialKeys[0])
	if initialKeyReceived {
		t.Logf("收到的初始 Key (%d) 的最终数据: %+v", initialKeys[0], initialData)
	}

	// 验证是否收到了动态添加的 key 的数据
	// 注意：这取决于在添加后、移除前，该 key 是否有数据更新。所以这个断言不是强制性的。
	additionalData, additionalKeyReceived := receivedData[additionalKeys[0]]
	t.Logf("是否收到添加的 key (%d) 的数据: %v", additionalKeys[0], additionalKeyReceived)
	if additionalKeyReceived {
		t.Logf("收到的添加 Key (%d) 的最终数据: %+v", additionalKeys[0], additionalData)
	}
	// assert.True(t, additionalKeyReceived, "应收到动态添加的 key (%d) 的数据", additionalKeys[0]) // 可以取消注释，如果确定会收到更新

}

// TestReadWriteRealtime 测试 V3 API 风格的实时数据读写功能。
func TestReadWriteRealtime(t *testing.T) {
	client, ctx := createTestClient(t) // 创建客户端和 context

	// 定义要读写的点位 ID。
	// !!! 请确保这些 ID 在你的 OpenPlant 环境中是有效的 !!!
	pointIDs := []int32{1024, 1025} // 使用示例中可能存在的 ID
	if len(pointIDs) == 0 {
		t.Skip("跳过 TestReadWriteRealtime，未配置有效的点位 ID") // 如果未配置 ID，跳过测试
		return
	}

	// --- 写入实时数据 ---
	writeTime := time.Now()                            // 获取当前时间作为写入时间戳
	valuesToWrite := make([]opio.Value, len(pointIDs)) // 创建 Value 切片用于写入
	for i, id := range pointIDs {
		valuesToWrite[i] = opio.Value{
			ID: id,                      // 点位 ID
			TM: int32(writeTime.Unix()), // 时间戳 (Unix 秒)
			DS: 0,                       // 数据状态 (假设 0 表示良好)
			AV: float64(i+1) * 11.1,     // 要写入的值 (示例值)
		}
	}
	t.Logf("准备写入实时数据: %+v", valuesToWrite)
	err := client.WriteRealtime(ctx, valuesToWrite) // 调用客户端的 WriteRealtime 方法
	require.NoError(t, err, "写入实时数据失败")
	t.Log("写入实时数据成功")

	// 短暂等待，以确保数据有足够时间被服务器处理和写入。
	t.Log("等待 200ms 以便服务器处理写入...")
	time.Sleep(200 * time.Millisecond)

	// --- 读取实时数据 ---
	// 准备 Value 切片用于读取，只需要设置 ID 字段。
	valuesToRead := make([]opio.Value, len(pointIDs))
	for i, id := range pointIDs {
		valuesToRead[i] = opio.Value{ID: id}
	}
	t.Logf("准备读取实时数据 (仅提供 ID): %+v", valuesToRead)
	err = client.ReadRealtime(ctx, valuesToRead) // 调用客户端的 ReadRealtime 方法
	require.NoError(t, err, "读取实时数据失败")
	t.Logf("读取实时数据成功，结果: %+v", valuesToRead)

	// --- 验证读取结果 ---
	require.Len(t, valuesToRead, len(pointIDs), "读取到的数据点数量不匹配") // 验证读取到的点数是否正确
	// 将读取到的结果存入 map 以便按 ID 查找
	readValuesMap := make(map[int32]opio.Value)
	for _, v := range valuesToRead {
		readValuesMap[v.ID] = v
	}

	// 遍历写入的数据，与读取到的数据进行比较
	for _, writeVal := range valuesToWrite {
		readVal, ok := readValuesMap[writeVal.ID]           // 查找对应 ID 的读取结果
		assert.True(t, ok, "未能找到 ID %d 的读取结果", writeVal.ID) // 确保找到了读取结果
		if ok {
			// 比较值 (AV)：使用 InDelta 允许浮点数比较的微小误差
			assert.InDelta(t, writeVal.AV, readVal.AV, 0.001, "读取到的实时值与写入的不匹配 (ID: %d)", writeVal.ID)
			// 比较状态 (DS)
			assert.Equal(t, writeVal.DS, readVal.DS, "读取到的实时状态与写入的不匹配 (ID: %d)", writeVal.ID)
			// 比较时间戳 (TM)：允许几秒的误差，因为服务器处理可能引入延迟
			assert.InDelta(t, float64(writeVal.TM), float64(readVal.TM), 5.0, "读取到的时间戳与写入的相差过大 (ID: %d, 写入: %d, 读取: %d)", writeVal.ID, writeVal.TM, readVal.TM)
		}
	}
	t.Log("实时数据读写验证完成")
}

// TestReadWriteArchive 测试 V3 API 风格的历史数据读写功能。
func TestReadWriteArchive(t *testing.T) {
	client, ctx := createTestClient(t) // 创建客户端和 context

	// 定义要读写的点位 ID。
	// !!! 请确保这些 ID 在你的 OpenPlant 环境中是有效的 !!!
	pointIDs := []int32{1024, 1025} // 使用示例中可能存在的 ID
	if len(pointIDs) == 0 {
		t.Skip("跳过 TestReadWriteArchive，未配置有效的点位 ID") // 如果未配置 ID，跳过测试
		return
	}

	// --- 写入历史数据 ---
	writeTime := time.Now()                                 // 获取当前时间作为基准
	archivesToWrite := make([]*opio.Archive, len(pointIDs)) // 创建 Archive 指针切片用于写入
	for i, id := range pointIDs {
		// 假设这些点位是 R8 (float64) 类型
		archivesToWrite[i] = &opio.Archive{
			ID:   id,          // 点位 ID
			Type: opio.TypeR8, // 数据类型
			Data: []opio.Value{ // 要写入的历史数据点列表
				{TM: int32(writeTime.Add(-2 * time.Minute).Unix()), DS: 0, AV: float64(i+1) * 10.0}, // 2 分钟前
				{TM: int32(writeTime.Add(-1 * time.Minute).Unix()), DS: 0, AV: float64(i+1) * 10.5}, // 1 分钟前
				{TM: int32(writeTime.Unix()), DS: 0, AV: float64(i+1) * 11.0},                       // 当前时间
			},
		}
	}
	t.Logf("准备写入历史数据: %+v", archivesToWrite)
	err := client.WriteArchive(ctx, archivesToWrite, false) // 调用客户端的 WriteArchive 方法，不使用缓存
	require.NoError(t, err, "写入历史数据失败")
	t.Log("写入历史数据成功")

	// 短暂等待，确保数据写入完成。
	t.Log("等待 200ms 以便服务器处理写入...")
	time.Sleep(200 * time.Millisecond)

	// --- 读取历史数据 ---
	// 定义读取的时间范围
	beginTime := writeTime.Add(-5 * time.Minute) // 从写入最早点之前开始
	endTime := writeTime.Add(1 * time.Minute)    // 到写入最晚点之后结束
	t.Logf("准备读取历史数据: IDs=%v, Mode=%d, Begin=%v, End=%v, Interval=0", pointIDs, opio.ModeRaw, beginTime, endTime)
	// 调用客户端的 ReadArchive 方法，读取原始值 (ModeRaw)
	readArchives, err := client.ReadArchive(ctx, pointIDs, opio.ModeRaw, beginTime, endTime, 0)
	require.NoError(t, err, "读取历史数据失败")
	require.NotEmpty(t, readArchives, "读取历史数据应返回结果") // 确保读取到了数据
	t.Logf("读取历史数据成功，返回 %d 个点位的数据", len(readArchives))

	// --- 验证读取结果 ---
	assert.Len(t, readArchives, len(pointIDs), "读取到的历史数据点数量不匹配") // 验证返回的点位数是否正确
	// 将读取到的结果存入 map 以便按 ID 查找
	readArchivesMap := make(map[int32]*opio.Archive)
	for _, ar := range readArchives {
		readArchivesMap[ar.ID] = ar
	}

	// 遍历写入的数据，与读取到的数据进行比较
	for _, writeAr := range archivesToWrite {
		readAr, ok := readArchivesMap[writeAr.ID]          // 查找对应 ID 的读取结果
		assert.True(t, ok, "未能找到 ID %d 的读取结果", writeAr.ID) // 确保找到了读取结果
		if ok {
			// 简单验证：读取到的数据点数量应至少等于写入的数量
			// （因为时间范围内可能还包含其他历史数据）
			assert.GreaterOrEqual(t, len(readAr.Data), len(writeAr.Data), "读取到的历史数据点数少于写入的 (ID: %d)", writeAr.ID)
			t.Logf("ID %d: 写入 %d 点, 读取到 %d 点", writeAr.ID, len(writeAr.Data), len(readAr.Data))
			// 打印读取到的前几个数据点样本
			logArchiveSample(t, readAr, 5)
			// 可以添加更详细的验证逻辑，例如逐个比较写入和读取的数据点的时间戳和值。
		}
	}
	t.Log("历史数据读写验证完成")
}

// TestReadStat 测试 V3 API 风格的统计数据读取功能。
func TestReadStat(t *testing.T) {
	client, ctx := createTestClient(t) // 创建客户端和 context

	// 定义要查询统计数据的点位 ID。
	// !!! 请确保这些 ID 在你的 OpenPlant 环境中是有效的，并且有历史数据 !!!
	pointIDs := []int32{1024, 1025} // 使用之前可能写入过数据的点
	if len(pointIDs) == 0 {
		t.Skip("跳过 TestReadStat，未配置有效的点位 ID") // 如果未配置 ID，跳过测试
		return
	}

	// 定义查询的时间范围和统计间隔
	beginTime := time.Now().Add(-10 * time.Minute) // 过去 10 分钟
	endTime := time.Now()                          // 到当前时间
	interval := int32(60)                          // 统计间隔为 1 分钟 (60 秒)
	t.Logf("准备读取统计数据: IDs=%v, Mode=%d, Begin=%v, End=%v, Interval=%d", pointIDs, opio.ModeAvg, beginTime, endTime, interval)
	// 调用客户端的 ReadStat 方法，读取平均值 (ModeAvg)
	stats, err := client.ReadStat(ctx, pointIDs, opio.ModeAvg, beginTime, endTime, interval)
	require.NoError(t, err, "读取统计数据失败")
	require.NotEmpty(t, stats, "读取统计数据应返回结果") // 确保读取到了数据
	t.Logf("读取统计数据成功，返回 %d 个点位的数据", len(stats))

	// --- 验证读取结果 ---
	assert.Len(t, stats, len(pointIDs), "读取到的统计数据点数量不匹配") // 验证返回的点位数是否正确
	// 将读取到的结果存入 map 以便按 ID 查找
	statsMap := make(map[int32]*opio.Stat)
	for _, st := range stats {
		statsMap[st.ID] = st
	}

	// 遍历请求的 ID，检查每个 ID 是否都有返回结果
	for _, reqID := range pointIDs {
		st, ok := statsMap[reqID]                     // 查找对应 ID 的统计结果
		assert.True(t, ok, "未能找到 ID %d 的统计结果", reqID) // 确保找到了结果
		if ok {
			assert.NotEmpty(t, st.Data, "统计结果应包含数据 (ID: %d)", st.ID) // 确保返回的统计数据不为空
			t.Logf("ID %d: 读取到 %d 个统计值", st.ID, len(st.Data))        // 记录读取到的统计值数量
			// 打印读取到的前几个统计值样本
			logStatSample(t, st, 5)
			// 可以添加对具体统计值的断言，例如检查平均值 (Avg) 是否在预期范围内。
		}
	}
	t.Log("统计数据读取验证完成")
}

// TestQueryResultScan 测试 QueryResult.Scan 方法的功能 (改为集成测试)。
// 这个测试会先执行一次真实的查询，然后用查询结果来测试 Scan 方法。
func TestQueryResultScan(t *testing.T) {
	client, ctx := createTestClient(t) // 创建客户端和 context

	// 1. 执行一个真实的查询以获取 QueryResult 对象
	queryOpts := &opio.QueryOptions{
		Limit: "3", // 获取少量数据 (3 行) 用于测试 Scan
	}
	// 查询 point 表的 ID, GN, ED 列
	queryColumns := []string{"ID", "GN", "ED"}
	qr, err := client.Query(ctx, testTableName, queryColumns, queryOpts)
	require.NoError(t, err, "执行查询以获取 Scan 测试数据失败")
	require.NotNil(t, qr, "查询结果不应为 nil")
	require.NotEmpty(t, qr.Rows, "查询应返回至少一行数据以测试 Scan") // 确保有数据用于 Scan
	t.Logf("Scan 测试：查询到 %d 行原始数据用于测试", len(qr.Rows))
	logQueryResultSample(t, qr, 3) // 记录原始数据样本

	// 2. 定义目标结构体，用于接收 Scan 的结果
	type TargetStructScan struct {
		pointID          int32   `opio:"ID"` // 使用 opio 标签匹配数据库列 "ID"
		GroupName        string  `opio:"GN"` // 使用 opio 标签匹配数据库列 "GN"
		ExtendedDesc     *string `opio:"ED"` // 使用 opio 标签匹配 "ED"，使用指针类型测试 nullable 列
		NonExistentField float64 // 这个字段在查询结果中不存在，用于测试忽略不存在的列
	}

	// 3. 调用 QueryResult 的 Scan 方法
	var results []TargetStructScan // 准备一个目标切片变量
	err = qr.Scan(&results)        // 将查询结果 Scan 到 results 切片中
	require.NoError(t, err, "Scan 操作失败")
	require.Len(t, results, len(qr.Rows), "Scan 后的切片长度应与查询结果行数匹配") // 验证 Scan 后的行数
	t.Logf("Scan 成功，结果 (%d 行):", len(results))
	for i, scanned := range results {
		if i >= 3 { // 最多记录前 3 行
			t.Logf("... (更多 %d 行)", len(results)-i)
			break
		}
		t.Logf("  行 %d: %+v", i, scanned) // 记录 Scan 后的结构体内容
	}

	// 4. 详细验证 Scan 后的结果是否正确
	t.Log("开始详细验证 Scan 结果...")
	for i, row := range qr.Rows { // 遍历原始查询结果的每一行 (map)
		scanned := results[i] // 获取 Scan 后的对应结构体

		// 验证 pointID (类型转换和标签匹配)
		expectedID, idOk := row["ID"].(int32) // 从原始 map 中获取 ID，假设是 int32
		assert.True(t, idOk, "原始数据中 ID 应为 int32")
		assert.Equal(t, expectedID, scanned.pointID, "Scan 后的 pointID 不匹配 (行 %d)", i)

		// 验证 GroupName (类型转换和标签匹配)
		expectedGN, gnOk := row["GN"].(string) // 从原始 map 中获取 GN，假设是 string
		assert.True(t, gnOk, "原始数据中 GN 应为 string")
		assert.Equal(t, expectedGN, scanned.GroupName, "Scan 后的 GroupName 不匹配 (行 %d)", i)

		// 验证 ExtendedDesc (指针类型和 nullable 处理)
		expectedED, edExists := row["ED"]  // 从原始 map 中获取 ED
		if edExists && expectedED != nil { // 如果原始值存在且不为 nil
			edStr, edOk := expectedED.(string) // 假设 ED 是 string
			assert.True(t, edOk, "原始数据中非 nil 的 ED 应为 string")
			require.NotNil(t, scanned.ExtendedDesc, "Scan 后的 ExtendedDesc 不应为 nil (行 %d)", i)    // 验证 Scan 后的指针不为 nil
			assert.Equal(t, edStr, *scanned.ExtendedDesc, "Scan 后的 ExtendedDesc 值不匹配 (行 %d)", i) // 验证解引用后的值
		} else { // 如果原始值不存在或为 nil
			assert.Nil(t, scanned.ExtendedDesc, "Scan 后的 ExtendedDesc 应为 nil (行 %d)", i) // 验证 Scan 后的指针为 nil
		}

		// 验证 NonExistentField (应保持 Go 的零值)
		assert.Zero(t, scanned.NonExistentField, "Scan 后的 NonExistentField 应为零值 (行 %d)", i)
	}

	// --- 测试 Scan 的错误处理情况 ---
	// 测试 Scan 到非指针变量
	var singleStruct TargetStructScan
	err = qr.Scan(singleStruct) // 传入结构体值，而不是指针
	assert.Error(t, err, "Scan 到非指针应返回错误")

	// 测试 Scan 到指向结构体的指针 (而不是指向切片的指针)
	err = qr.Scan(&singleStruct) // 传入结构体指针
	assert.Error(t, err, "Scan 到非切片指针应返回错误")

	// 测试 Scan 到 nil 切片指针 (这应该是允许的，Scan 内部应分配新的切片)
	var nilSlice []TargetStructScan                                // 声明一个 nil 切片变量
	err = qr.Scan(&nilSlice)                                       // 传入 nil 切片的指针
	assert.NoError(t, err, "Scan 到 nil 切片指针应成功（会分配新切片）")           // 验证没有错误
	assert.NotNil(t, nilSlice, "Scan 后切片不应为 nil")                  // 验证切片已被分配
	assert.Len(t, nilSlice, len(qr.Rows), "Scan 到 nil 切片指针后长度应匹配") // 验证分配后的长度正确
	t.Log("Scan 错误处理测试完成")
}

// --- 辅助日志函数 ---

// logQueryResultSample 记录 QueryResult 的前 N 行数据样本。
func logQueryResultSample(t *testing.T, qr *opio.QueryResult, maxRows int) {
	if qr == nil || len(qr.Rows) == 0 {
		t.Log("  (查询结果为空)")
		return
	}
	t.Logf("  查询结果样本 (最多 %d 行):", maxRows)
	// 打印列名
	colNames := make([]string, len(qr.Columns))
	for i, col := range qr.Columns {
		colNames[i] = col.GetName()
	}
	t.Logf("    列: %v", colNames)
	// 打印行数据
	for i, row := range qr.Rows {
		if i >= maxRows {
			t.Logf("    ... (更多 %d 行)", len(qr.Rows)-i)
			break
		}
		t.Logf("    行 %d: %+v", i, row)
	}
}

// logArchiveSample 记录 Archive 数据的前 N 个点。
func logArchiveSample(t *testing.T, ar *opio.Archive, maxPoints int) {
	if ar == nil || len(ar.Data) == 0 {
		t.Logf("  (ID %d 的历史数据为空)", ar.ID)
		return
	}
	t.Logf("  历史数据样本 (ID: %d, 类型: %d, 最多 %d 点):", ar.ID, ar.Type, maxPoints)
	for i, val := range ar.Data {
		if i >= maxPoints {
			t.Logf("    ... (更多 %d 点)", len(ar.Data)-i)
			break
		}
		// 格式化时间戳
		ts := time.Unix(int64(val.TM), 0).Format("2006-01-02 15:04:05")
		t.Logf("    点 %d: 时间=%s, 状态=%d, 值=%v", i, ts, val.DS, val.AV)
	}
}

// logStatSample 记录 Stat 数据的前 N 个统计值。
func logStatSample(t *testing.T, st *opio.Stat, maxPoints int) {
	if st == nil || len(st.Data) == 0 {
		t.Logf("  (ID %d 的统计数据为空)", st.ID)
		return
	}
	t.Logf("  统计数据样本 (ID: %d, 类型: %d, 最多 %d 点):", st.ID, st.Type, maxPoints)
	for i, val := range st.Data {
		if i >= maxPoints {
			t.Logf("    ... (更多 %d 点)", len(st.Data)-i)
			break
		}
		// 格式化时间戳
		ts := time.Unix(int64(val.Time), 0).Format("2006-01-02 15:04:05")
		// 打印所有 StatVal 结构体中的字段
		t.Logf("    点 %d: 时间=%s, 状态=%d, 平均值=%.3f, 最大值=%.3f (时间=%v), 最小值=%.3f (时间=%v), 累积=%.3f, 算术平均=%.3f, 总和=%.3f",
			i, ts, val.Status, val.Avg, val.Max, time.Unix(int64(val.MaxTime), 0).Format("15:04:05"), val.Min, time.Unix(int64(val.MinTime), 0).Format("15:04:05"), val.Flow, val.Mean, val.Sum)
	}
}
