package opio

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

var host = "10.75.39.200"
var port = 8200
var user = "sis"
var pwd = "openplant"

// var timeout = 60
// var min = 2
// var m = 20
var count = 1000

// 生成唯一标识符
func Test_MakeUUID(t *testing.T) {
	v := MakeUUID("W3.test.test")
	if v != 6236676836603809660 {
		t.Errorf("Test MakeUUID error ")
	}
	fmt.Println(v)
	return
	//v := int32(1024)
	//fmt.Println(float64(v))
	//return
}

// 开启订阅
func Test_sub(t *testing.T) {
	// 用于初始化与服务的连接。需要提供服务器地址、端口、超时时间、用户名和密码。
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	// 创建一个新的订阅。这通常用于实时数据流或事件监听。
	sub, err := op.NewSubscribe("Realtime")

	if err != nil {
		fmt.Println("error", err)
		return
	}

	_ = op.Close()

	//ids := make([]int32, 1)
	//for i := int32(0); i < 17; i++ {
	//	ids[i] = 1024 + i
	//}
	ids := []int32{198713}
	ids2 := []int32{198714}
	//ids2 := make([]int32, 1)
	//ids2[0] = 9031
	log.Println("开始发起订阅")
	// 初始化订阅，设置需要订阅的数据和回调函数以处理接收到的数据。
	_ = sub.InitSubscribe(ids, "ID", func(res *Response) {
		errno := res.GetErrNo()
		if errno == 0 {
			format(res.GetDataSet())
		} else {
			fmt.Println("error", errno)
		}
	})

	log.Println("追加订阅ids2")
	_ = sub.Subscribe(ids2)
	time.Sleep(30 * time.Second)

	_ = sub.UnSubscribe(ids2)
	time.Sleep(10 * time.Minute)
	log.Println("停止订阅ids2")

	sub.Close()

	log.Println("关闭订阅")
}

// 基础sql查询
func Test_SQL(t *testing.T) {
	// 用于初始化与服务的连接。需要提供服务器地址、端口、超时时间、用户名和密码。
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	_ = op.SetCompressModel(ZIP_MODEL_Frame)
	sql := "select * from Realtime limit 10"
	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionExecSQL)
	req.SetSQL(sql)
	_ = req.WriteAndFlush()

	res, err := req.GetResponse()
	if err != nil {
		log.Fatal("get response error:", err)
	}
	rs := res.GetDataSet()
	format(rs)
	rs.Close()
	_ = op.Close()
}

// 复杂情况下的sql查询
func Test_SQL2(t *testing.T) {
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	_ = op.SetCompressModel(ZIP_MODEL_Frame)
	sql := "SELECT GN FROM NODE;"
	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionExecSQL)
	req.SetSQL(sql)
	_ = req.WriteAndFlush()

	res, err := req.GetResponse()
	if err != nil {
		log.Fatal("get response error:", err)
	}
	rs := res.GetDataSet()
	format(rs)
	rs.Close()
	_ = op.SetCompressModel(ZIP_MODEL_Frame)
	sql = "SELECT GN FROM NODE WHERE GN='HNJT.NET';"
	req = op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionExecSQL)
	req.SetSQL(sql)
	_ = req.WriteAndFlush()

	res, err = req.GetResponse()
	if err != nil {
		log.Fatal("get response error:", err)
	}
	rs = res.GetDataSet()
	format(rs)
	rs.Close()
	_ = op.Close()
}

// 自定义字段(GN)进行插入
func Test_Insert_Realtime(t *testing.T) {
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		fmt.Println(err)
		return
	}

	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionInsert)

	table := NewTable("Realtime", 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("AV", VtObject, 0)
	w_count := 5
	for {
		table.Clear()
		start := time.Now()
		w_count++
		for i := 0; i < count; i++ {
			fix := strconv.Itoa(i)
			err := table.SetColumnString(0, "W3.AX.AX"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			if w_count%5 == 0 {
				err = table.SetColumnObject(1, float64(w_count))
			} else {
				err = table.SetColumnObject(1, float64(w_count))
			}
			if err != nil {
				fmt.Println(err)
				break
			}
			table.BindRow()

			err = table.SetColumnString(0, "W3.DX.DX"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			if w_count%5 == 0 {
				err = table.SetColumnObject(1, float64(w_count))
			} else {
				err = table.SetColumnObject(1, float64(w_count))
			}
			if err != nil {
				fmt.Println(err)
				break

			}
			table.BindRow()

			err = table.SetColumnString(0, "W3.I2.I2"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			err = table.SetColumnObject(1, float64(w_count))
			if err != nil {
				fmt.Println(err)
				break
			}
			table.BindRow()

			err = table.SetColumnString(0, "W3.I4.I4"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			err = table.SetColumnObject(1, float64(w_count))
			if err != nil {
				fmt.Println(err)
				break
			}
			table.BindRow()

			err = table.SetColumnString(0, "W3.R8.R8"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			err = table.SetColumnObject(1, float64(w_count))
			if err != nil {
				fmt.Println(err)
				break
			}
			table.BindRow()

			err = table.SetColumnString(0, "W3.LONG.LONG"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			err = table.SetColumnObject(1, float64(w_count))
			if err != nil {
				fmt.Println(err)
				break
			}
			table.BindRow()

			err = table.SetColumnString(0, "W3.TEXT.TEXT"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			err = table.SetColumnObject(1, "W3.TEXT Test"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			table.BindRow()

			err = table.SetColumnString(0, "W3.BLOB.BLOB"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			err = table.SetColumnObject(1, "W3.BLOB Test"+fix)
			if err != nil {
				fmt.Println(err)
				break
			}
			table.BindRow()

		}
		_ = req.SetTable(table)
		fmt.Println("construct request use time:", time.Since(start))
		start = time.Now()
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
		fmt.Println("write use time:", time.Since(start))
		start = time.Now()
		res, err := req.GetResponse()
		if err != nil {
			fmt.Println(err)
			return
		}
		rs := res.GetDataSet()
		rs.Close()
		fmt.Println("read use time:", time.Since(start))
		//format(rs)
		time.Sleep(time.Second)
	}
	_ = op.Close()
}

// 根据ID进行实时数据写入
func Test_Insert_Realtime_ID(t *testing.T) {
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		fmt.Println(err)
		return
	}

	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionExecSQL)
	sql := "select ID,RT from Point where  RT IN (0,1,2,3,4,5)"
	req.SetSQL(sql)
	_ = req.Write()
	req.Flush()
	res, err := req.GetResponse()
	if err != nil {
		log.Fatal("get response error:", err)
	}
	rs := res.GetDataSet()
	idRt := make(map[int32]int8)
	for {
		ok, err := rs.Next()
		if ok && err == nil {
			id, _ := rs.GetInt32(0)
			rt, _ := rs.GetInt8(1)
			idRt[id] = rt
		} else {
			fmt.Println("release dataset error:", err)
			break
		}
	}
	rs.Close()
	req = op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionInsert)
	table := NewTable("Realtime", 0)
	table.AddColumn("ID", VtInt32, 0)
	table.AddColumn("AV", VtObject, 0)
	w_count := 0
	for {
		table.Clear()
		start := time.Now()
		w_count++
		fix := strconv.Itoa(w_count)
		for id, rt := range idRt {
			_ = table.SetColumnInt32(0, id, 0)
			if w_count%5 == 0 {
				err = table.SetColumnObject(1, nil)
			} else {
				switch rt {
				case 0, 4:
					err = table.SetColumnObject(1, float64(w_count))
				case 2, 3, 5:
					err = table.SetColumnObject(1, w_count)
				case 6, 7:
					err = table.SetColumnObject(1, "W3.TEXT Test"+fix)
				default:
					if w_count%2 == 0 {
						err = table.SetColumnObject(1, true)
					} else {
						err = table.SetColumnObject(1, false)
					}
				}
			}
			if err != nil {
				fmt.Println("=========", err)
				break
			}
			table.BindRow()
		}
		_ = req.SetTable(table)
		//fmt.Println("construct request use time:", time.Since(start))
		//start = time.Now()
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
		//fmt.Println("write use time:", time.Since(start))
		//start = time.Now()
		res, err := req.GetResponse()
		if err != nil {
			fmt.Println(err)
			return
		}
		rs = res.GetDataSet()
		fmt.Println("write use time:", time.Since(start))
		//format(rs)
		rs.Close()
		//time.Sleep(time.Second)
	}
	//noinspection GoUnreachableCode
	_ = op.Close()
}

// 根据ID进行实时数据写入，并且专注于双精度浮点型实时数据写入
func Test_Insert_Realtime_ID_DOUBLE(t *testing.T) {
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		fmt.Println(err)
		return
	}

	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionExecSQL)
	sql := "select ID,RT from Point where  RT IN (0,1,2,3,4,5)"
	req.SetSQL(sql)
	_ = req.Write()
	req.Flush()
	res, err := req.GetResponse()
	if err != nil {
		log.Fatal("get response error:", err)
	}
	rs := res.GetDataSet()
	idRt := make(map[int32]int8, 0)
	for {
		ok, err := rs.Next()
		if ok && err == nil {
			id, _ := rs.GetInt32(0)
			rt, _ := rs.GetInt8(1)
			idRt[id] = rt
		} else {
			fmt.Println("release dataset error:", err)
			break
		}
	}
	rs.Close()
	req = op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionInsert)

	table := NewTable("Realtime", 0)
	table.AddColumn("ID", VtInt32, 0)
	table.AddColumn("AV", VtDouble, 0)
	w_count := 0
	for {
		table.Clear()
		start := time.Now()
		w_count++
		fix := strconv.Itoa(w_count)
		for id, rt := range idRt {
			_ = table.SetColumnInt32(0, id, 0)
			switch rt {
			case 0, 4:
				err = table.SetColumnDouble(1, float64(w_count))
			case 2, 3, 5:
				err = table.SetColumnDouble(1, float64(w_count))
			case 6, 7:
				err = table.SetColumnObject(1, "W3.TEXT Test"+fix)
			default:
				err = table.SetColumnDouble(1, float64(w_count))
			}
			if err != nil {
				fmt.Println("=========", err)
				break
			}
			table.BindRow()
		}
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
		rs = res.GetDataSet()
		fmt.Println("write use time:", time.Since(start))
		//format(rs)
		rs.Close()
		//time.Sleep(time.Second)
	}
	_ = op.Close()
}

// 创建点位并写入node节点
func Test_Insert(t *testing.T) {
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	table := NewTable("Node", 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("ED", VtString, 0)

	_ = table.SetColumnString(0, "W3.AX")
	_ = table.SetColumnString(1, "W3.AX Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.DX")
	_ = table.SetColumnString(1, "W3.DX Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.I2")
	_ = table.SetColumnString(1, "W3.I2 Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.I4")
	_ = table.SetColumnString(1, "W3.I4 Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.R8")
	_ = table.SetColumnString(1, "W3.R8 Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.LONG")
	_ = table.SetColumnString(1, "W3.LONG Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.TEXT")
	_ = table.SetColumnString(1, "W3.TEXT Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.BLOB")
	_ = table.SetColumnString(1, "W3.BLOB Test")
	table.BindRow()

	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionInsert)
	_ = req.SetTable(table)
	_ = req.Write()
	_ = req.WriteContent(table)
	req.Flush()

	res, err := req.GetResponse()
	rs := res.GetDataSet()
	format(rs)
	table = NewTable("Point", 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("ED", VtString, 0)
	table.AddColumn("RT", VtInt32, 0)
	for i := 0; i < count; i++ {
		_ = table.SetColumnString(0, "W3.AX.AX"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.AX Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 0, 0)
		table.BindRow()

		_ = table.SetColumnString(0, "W3.DX.DX"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.DX Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 1, 0)
		table.BindRow()

		_ = table.SetColumnString(0, "W3.I2.I2"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.I2 Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 2, 0)
		table.BindRow()

		_ = table.SetColumnString(0, "W3.I4.I4"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.I4 Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 3, 0)
		table.BindRow()

		_ = table.SetColumnString(0, "W3.R8.R8"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.R8 Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 4, 0)
		table.BindRow()

		_ = table.SetColumnString(0, "W3.LONG.LONG"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.LONG Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 5, 0)
		table.BindRow()

		_ = table.SetColumnString(0, "W3.TEXT.TEXT"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.TEXT Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 6, 0)
		table.BindRow()

		_ = table.SetColumnString(0, "W3.BLOB.BLOB"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.BLOB Test"+strconv.Itoa(i))
		_ = table.SetColumnInt32(2, 7, 0)
		table.BindRow()

	}
	_ = req.SetTable(table)
	_ = req.Write()
	_ = req.WriteContent(table)
	req.Flush()
	res, err = req.GetResponse()
	rs = res.GetDataSet()
	format(rs)
	_ = op.Close()

}

// 更新node节点的基础信息
func Test_Update(t *testing.T) {
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	table := NewTable("Node", 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("ED", VtString, 0)

	_ = table.SetColumnString(0, "W3.AX")
	_ = table.SetColumnString(1, "W3.AX Test Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.DX")
	_ = table.SetColumnString(1, "W3.DX Test Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.I2")
	_ = table.SetColumnString(1, "W3.I2 Test Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.I4")
	_ = table.SetColumnString(1, "W3.I4 Test Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.R8")
	_ = table.SetColumnString(1, "W3.R8 Test Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.LONG")
	_ = table.SetColumnString(1, "W3.LONG Test Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.TEXT")
	_ = table.SetColumnString(1, "W3.TEXT Test Test")
	table.BindRow()

	_ = table.SetColumnString(0, "W3.BLOB")
	_ = table.SetColumnString(1, "W3.BLOB Test Test")
	table.BindRow()

	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionUpdate)
	_ = req.SetTable(table)
	_ = req.Write()
	_ = req.WriteContent(table)
	req.Flush()

	res, err := req.GetResponse()
	rs := res.GetDataSet()
	format(rs)
	table = NewTable("Point", 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("ED", VtString, 0)
	for i := 0; i < count; i++ {

		_ = table.SetColumnString(0, "W3.AX.AX"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.AX Test Test"+strconv.Itoa(i))
		table.BindRow()

		_ = table.SetColumnString(0, "W3.DX.DX"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.DX Test Test"+strconv.Itoa(i))
		table.BindRow()

		_ = table.SetColumnString(0, "W3.I2.I2"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.I2 Test Test"+strconv.Itoa(i))
		table.BindRow()

		_ = table.SetColumnString(0, "W3.I4.I4"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.I4 Test Test"+strconv.Itoa(i))
		table.BindRow()

		_ = table.SetColumnString(0, "W3.R8.R8"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.R8 Test Test"+strconv.Itoa(i))
		table.BindRow()

		_ = table.SetColumnString(0, "W3.LONG.LONG"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.LONG Test Test"+strconv.Itoa(i))
		table.BindRow()

		_ = table.SetColumnString(0, "W3.TEXT.TEXT"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.TEXT Test Test"+strconv.Itoa(i))
		table.BindRow()

		_ = table.SetColumnString(0, "W3.BLOB.BLOB"+strconv.Itoa(i))
		_ = table.SetColumnString(1, "W3.BLOB Test Test"+strconv.Itoa(i))
		table.BindRow()
	}
	_ = req.SetTable(table)
	_ = req.Write()
	_ = req.WriteContent(table)
	req.Flush()
	res, err = req.GetResponse()
	rs = res.GetDataSet()
	format(rs)
	_ = op.Close()
}

// 根据ID删除node节点的点位
func Test_Remove(t *testing.T) {
	fmt.Println("host:", host, "port :", port)
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionDelete)
	req.SetTableName("Node")
	count := 100
	ids := make([]int32, count)
	for i := 0; i < count; i++ {
		ids[i] = int32(i)
	}
	req.SetIndexesInt32("ID", ids)
	_ = req.Write()
	req.Flush()
	res, err := req.GetResponse()
	fmt.Println(err)
	rs := res.GetDataSet()
	format(rs)
	_ = op.Close()
}

// 根据条件返回对应的列
func Test_Find(t *testing.T) {
	fmt.Println("host:", host, "port :", port)
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionSelect)
	table := NewTable("Point", 0)
	//table.AddColumn("*", VtNull, 0)
	// 设置要返回的列
	table.AddColumn("EC", VtInt8, 0)
	table.AddColumn("ID", VtInt64, 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("EX", VtString, 0)
	table.AddColumn("ED", VtString, 0)
	table.AddColumn("ND", VtInt32, 0)
	table.AddColumn("CP", VtInt32, 0)
	table.AddColumn("PT", VtInt32, 0)
	table.AddColumn("RT", VtInt32, 0)

	//table.AddColumn("TM", VtDateTime, 0)
	//table.AddColumn("DS", VtInt16, 0)
	//table.AddColumn("AV", VtObject, 0)
	_ = req.SetTable(table)
	//count := 1000
	//ids := make([]int32, count)
	//for i := 0; i < count; i++ {
	//	ids[i] = int32(i + 2106176)
	//}
	//ids := []int32{1024, 1025, 1026}
	//// 设置筛选条件， 如果不设置就返回所有内容
	//req.SetIndexesInt32("ID", ids)

	now := time.Now()

	_ = req.WriteAndFlush()
	res, err := req.GetResponse()
	fmt.Println(err)
	rs := res.GetDataSet()
	format(rs)
	rs.Close()
	fmt.Println(time.Since(now))
	_ = op.Close()
}

// 根据筛选条件返回对应的列和满足条件的行
func Test_FindFilter(t *testing.T) {
	fmt.Println("host:", host, "port :", port)
	op, err := Init(host, port, 60, user, pwd)
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionSelect)
	table := NewTable("Point", 0)
	//table.AddColumn("*", VtNull, 0)
	// 设置要返回的列
	table.AddColumn("EC", VtInt8, 0)
	table.AddColumn("ID", VtInt64, 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("EX", VtString, 0)
	table.AddColumn("ED", VtString, 0)
	table.AddColumn("ND", VtInt32, 0)
	table.AddColumn("CP", VtInt32, 0)
	table.AddColumn("PT", VtInt32, 0)
	table.AddColumn("RT", VtInt32, 0)

	table.AddColumn("TM", VtDateTime, 0)
	table.AddColumn("DS", VtInt16, 0)
	table.AddColumn("AV", VtObject, 0)
	_ = req.SetTable(table)
	//count := 1000
	//ids := make([]int32, count)
	//for i := 0; i < count; i++ {
	//	ids[i] = int32(i + 2106176)
	//}
	//ids := []int32{1024, 1025, 1026}
	// 设置筛选条件， 如果不设置就返回所有内容
	//req.SetIndexesInt32("ID", []int32{1024, 1025, 1026})
	req.SetIndexesString("GN", []string{"W3.AX.AX0", "W3.DX.DX0"})

	now := time.Now()

	_ = req.WriteAndFlush()
	res, err := req.GetResponse()
	fmt.Println(err)
	rs := res.GetDataSet()
	format(rs)
	rs.Close()
	fmt.Println(time.Since(now))
	_ = op.Close()
}

// 多线程查询  通过多端口实现
func Test_MultiThreadFind(t *testing.T) {
	syncWait := sync.WaitGroup{}
	host := "192.168.4.236"
	ports := []int{8200, 8300, 8400}
	//ports := []int{8200}
	for i := 0; i < len(ports); i++ {
		syncWait.Add(1)
		go func(i int) {
			findByNewProtocol(host, ports[i])
			syncWait.Done()
		}(i)
	}
	syncWait.Wait()

	//host := "192.168.4.236"
	////ports := []int{8200, 8300, 8400}
	//
	//ports := []int{8200}
	//for i := 0; i < len(ports); i++ {
	//	findByNewProtocol(host, ports[i])
	//	fmt.Println("ok ????")
	//}

}

// 多线程环境下执行具体查询的操作
func findByNewProtocol(host string, port int) {
	//fmt.Println("host:", host, "port :", port)
	op, err := Init(host, port, 1, "sis", "openplant")
	if err != nil {
		log.Fatal("init conn error:", err)
		return
	}
	req := op.NewRequest(nil)
	req.SetID(1)
	req.SetService("openplant")
	req.SetAction(ActionSelect)
	tableName := "Point"
	req.SetTableName(tableName)
	table := NewTable(tableName, 0)
	table.AddColumn("EC", VtInt32, 0)
	table.AddColumn("GN", VtString, 0)
	table.AddColumn("ID", VtInt32, 0)
	_ = req.SetTable(table)

	_ = req.Write()
	req.Flush()

	res, err := req.GetResponse()
	//fmt.Println("res :>>>", res)
	rs := res.GetDataSet()
	format2(op, rs)
	rs.Close()
	_ = op.Close()

}

func format2(op *IOConnect, rs *OPDataSet) {

	//count := 0
	//for {
	//	ok, err := rs.Next()
	//	if ok && err == nil {
	//		for i, col := range rs.GetColumns() {
	//			value, _ := rs.GetValue(uint32(i))
	//			fmt.Print(col.GetName(), ":", value, "\t")
	//		}
	//		fmt.Println()
	//		count++
	//	} else {
	//		fmt.Println("ok >>> ", ok, " err >>", err)
	//		break
	//	}
	//}
	//fmt.Println("resultset row count ", count)

	count := 0
	for {
		ok, err := rs.Next()
		if ok && err == nil {
			count++
		} else {
			break
		}
	}
	fmt.Println(op.GetAddress(), " --resultset row count ", count)
}

func format(rs *OPDataSet) {
	fmt.Println("---------------------------------")
	//count := 0
	for {
		ok, err := rs.Next()
		if ok && err == nil {
			for i, col := range rs.GetColumns() {
				value, _ := rs.GetValue(uint32(i))
				fmt.Print(col.GetName(), ":", value, "\t")
			}
			fmt.Println()
			count++
		} else {
			//fmt.Println("error", err)
			break
		}
	}
	//fmt.Println("resultset row count ", count)
}
