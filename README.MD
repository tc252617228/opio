### 在新锐代码的基础上增加了部分接口功能的注释
### opio的连接


    opio.MakeUUID  
    生成一个基于输入字符串的唯一标识符（UUID）。
    这个函数是用于创建识别数据记录或对象的唯一键值。

    opio.Init
    用于初始化与服务的连接。
    这个函数接受主机地址、端口、超时时间、用户名和密码作为参数，并返回一个连接对象和错误信息。

    opio.NewSubscribe
    创建一个新的数据订阅。
    这个方法用于设置实时数据流或事件的监听。

    sub.InitSubscribe
    初始化订阅，设置订阅的数据标识和回调函数。
    这个函数用于配置订阅的特定数据点和接收数据时的处理函数。

    sub.Subscribe
    sub.UnSubscribe
    这两个方法分别用于添加新的订阅标识和取消订阅。

    op.NewRequest
    创建一个新的请求。这通常用于执行SQL查询或数据操作请求。

    req.SetID(int)
    req.SetService(string)
    req.SetAction(int)
    req.SetSQL(string)
    这些方法用于配置请求的不同参数，包括请求ID、服务名称、操作类型和SQL语句。

    req.WriteAndFlush()
    req.GetResponse()
    这些方法用于发送请求并获取响应。WriteAndFlush可能是写入并发送请求，而GetResponse用于接收服务器的响应。

    opio.NewTable()
    创建新表。用于定义数据结构，可能用于插入或更新操作。

    table.AddColumn()
    添加列到表。这个方法定义了表的结构，包括列的名称、数据类型和长度。

    table.BindRow()
    将当前行的数据绑定到表中。这可能是在插入或更新数据之前的一个步骤。

    req.SetTable()
    将表绑定到请求。这用于在执行插入或更新操作时，将数据表与请求关联起来。

    op.Close()
    sub.Close()
    用于关闭连接或订阅。这些方法释放资源并关闭打开的连接或数据订阅。


```go
package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	opio "github.com/tc252617228/opio/driver"
)

func main() {
	host := "127.0.0.1"
	port := 8200
	user := "sis"
	pwd := "openplant"
	gn := "W3.AX.AX1" // 假设的GN
	value := 123.45   // 假设的要写入的值

	// 1. 初始化连接
	op, err := opio.Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	defer op.Close()

	// 2. 创建请求
	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(opio.ActionInsert)

	// 3. 设置请求参数
	table := opio.NewTable("Realtime", 0)
	table.AddColumn("GN", opio.VtString, 0)
	table.AddColumn("AV", opio.VtObject, 0)

	// 4. 插入数据
	_ = table.SetColumnString(0, gn)
	_ = table.SetColumnObject(1, value)
	table.BindRow()

	// 5. 发送请求并获取响应
	_ = req.SetTable(table)
	err = req.Write()
	if err != nil {
		fmt.Println(err)
		return
	}
	err = req.WriteContent(table)
	if err != nil {
		fmt.Println(err)
		return
	}
	req.Flush()

	res, err := req.GetResponse()
	if err != nil {
		fmt.Println(err)
		return
	}
	rs := res.GetDataSet()
	fmt.Println("Data written successfully")
	rs.Close()

	// 6. 关闭连接
	_ = op.Close()
}

```


```
opio.Init(host, port, 60, user, pwd)
Init函数负责建立与服务端的TCP连接，并进行登录验证，返回一个IOConnect实例用于后续的通信。
这个函数是与服务端交互的基础，为进一步的数据操作（如查询、插入等）提供了网络连接基础。


req := op.NewRequest(nil)
主要作用是创建并初始化一个新的请求对象。这个请求对象随后可以用于执行各种操作，如数据库查询、数据插入等。通过传递一个属性映射，可以灵活地配置请求的行为和参数。
nil表示不需要立即为请求设置任何特定的属性或参数


op.NewRequest(nil).SetID(1)
SetID方法的作用是设置请求的唯一标识符（ID）。这个ID在请求处理过程中可能被用于追踪、日志记录或其他目的。通过加锁和解锁，该方法还确保了在多线程环境下对请求ID进行设置的线程安全性。

opio.NewTable("Realtime", 0)
创建一个新的 Table 实例的函数。它接受两个参数：表的名称（name）和容量（capacity）。这个函数初始化一个 Table 结构体实例，设定表名和预分配的行数（通过 capacity 参数）。
Table 结构体包含多个字段，用于管理表的数据和结构，如表名、列的数量、列的信息、行数据等。


opio.NewTable("Realtime", 0).AddColumn("GN", opio.VtString, 0)
AddColumn 用于向 Table 实例添加一个新列。它接受三个参数：列名（name）、列的数据类型（typ）和列的长度（length）。这个函数首先调用 AddColumnEx 方法来完成列的添加。
AddColumnEx 实现了列的详细配置，包括列名、数据类型、长度等，并将列信息追加到 Table 结构的列集合中。


table.SetColumnString(0, gn)
用于将一个字符串值设置到指定的列中。这个函数接收列的索引（col）和字符串值（value）作为参数。
函数首先检查列索引是否有效，然后确认列的数据类型是否为 VtString（字符串类型）。如果这些检查通过，它会将字符串值编码并存储在表的内部缓冲区中。如果列的类型与期望不匹配，函数会记录一个错误。

table.SetColumnObject(1, value)
用于为表中的指定列设置一个通用的对象值。这个函数处理多种不同的数据类型，包括基础数据类型（如整数、浮点数）、复杂数据类型（如时间对象）以及二进制和字符串数据。
这个函数基于列的数据类型和提供的值，执行相应的处理和转换。例如，对于基础数据类型，它会直接将值转换为合适的格式并存储在缓冲区中。对于字符串和二进制数据，它会执行额外的编码步骤。如果提供的值类型与列的数据类型不匹配，函数会记录一个错误。

table.BindRow()
用于将当前的列数据作为一个新的行绑定到表中。这个函数在所有必需的列值被设置之后调用，以完成一行数据的构建。
在 BindRow 被调用时，它首先检查是否有任何错误发生在设置列值的过程中。如果没有错误，它将把当前缓冲区中的数据（代表一行）复制到表的行集合中，并递增行数计数器。如果有错误，这些错误会被记录下来，并且可能影响数据绑定的结果。

req.SetTable(table)
这个方法用于将一个Table对象设置到请求(Request)中。它首先检查Table中是否有未处理的异常，然后将Table的名称和列数据添加到请求的属性集合中。这个方法通常用于准备请求数据，例如在插入或更新数据库时设置要发送的数据表。

req.Write()
将请求的属性（props）和一些其他数据写入到内部的缓冲区（buff）。它通过遍历props中的每个键值对，并将它们编码（序列化）到缓冲区中。这是发送请求到服务器前的一个重要步骤，因为它实际上准备了请求的数据内容。

req.WriteContent(table)
将一个Table对象的内容写入请求的缓冲区。它遍历Table中的每一行数据，并将这些数据序列化到缓冲区中。这个方法通常与Write()方法一起使用，用于准备包含数据表内容的请求。

req.Flush()
将请求的缓冲区（buff）中的内容发送到服务器。具体来说，它首先将一个nil值编码到缓冲区的末尾，这可能用作消息结束的标记。然后调用 buff.Flush(true)，这个调用实际上会将缓冲区中的数据发送出去。
在网络编程中，缓冲区通常用于临时存储数据，直到缓冲区满或显式调用发送操作（如此处的Flush）。在Flush()被调用之前，即使已经写入了数据（如使用Write()或WriteContent()方法），这些数据也不会被发送到服务器。只有在Flush()被调用之后，这些数据才真正地发送到网络上。
简而言之，Flush()方法在请求的数据准备完毕后，负责实际的数据发送操作，确保所有已经编码到缓冲区的数据被发送到服务器。

req.GetResponse()
从请求的缓冲区读取响应数据，并创建一个Response对象。它调用MakeResponse来创建一个新的Response对象，然后调用该对象的Read()方法来填充响应数据。这个方法通常在发送请求并等待响应时使用。

res.GetDataSet()
获取与请求相关联的OPDataSet对象。OPDataSet可能是请求的结果，例如在查询数据库后获取的数据集。这个方法允许您访问和处理这些数据。
```

