package opio

import (
	"fmt"
	"opio/driver/internal/utils"
	"testing"
	"time"
	"unsafe"
)

type student struct {
	ID     int32             `json:"id"`
	Name   string            `json:"name"`
	Class  string            `json:"class"`
	Grade  string            `json:"grade"`
	Scores []int16           `json:"scores"`
	Family map[string]string `json:"family"` // first:relationship, second:first name
}

type classTool struct {
	Tool1 string `json:"tool_1"`
	Tool2 string `json:"tool_2"`
	Tool3 string `json:"tool_3"`
}

type class struct {
	Name     string            `json:"name"`
	Titles   []string          `json:"titles"`
	Students map[int32]student `json:"students"`
	Tool     classTool         `json:"tool"`
}

// 驱动电机 MotorState
type motor struct {
	Mt_seq int16   `thrift:"A_TAG,1"` // 驱动电机序号           short     MotorSeq
	Mt_s   int16   `thrift:"A_TAG,2"` // 驱动电机状态           String    Status
	Mc_t   int16   `thrift:"A_TAG,3"` // 驱动电机控制器温度      String    ControllerTemperature
	Mt_n   int32   `thrift:"A_TAG,4"` // 驱动电机转速           String    MotorSpeed
	M_tq   float32 `thrift:"A_TAG,5"` // 驱动电机转矩           String    MotorTorque
	Mt_t   int16   `thrift:"A_TAG,6"` // 驱动电机温度           String    MotorTemperature
	Mt_v   float32 `thrift:"A_TAG,7"` // 电机控制器输入电压      String    ControllerVoltage
	Mt_i   float32 `thrift:"A_TAG,8"` // 电机控制器直流母线电流   String    ControllerCurrent
}

func EncodeMotor(m motor) (int, []byte) {
	data := make([]byte, unsafe.Sizeof(m))
	utils.PutInt16(data[:2], m.Mt_seq)
	utils.PutInt16(data[2:4], m.Mt_s)
	utils.PutInt16(data[4:6], m.Mc_t)
	utils.PutFloat32(data[6:10], m.M_tq)
	utils.PutInt16(data[6:10], m.Mt_t)
	utils.PutFloat32(data[6:10], m.Mt_v)
	utils.PutFloat32(data[6:10], m.Mt_i)
	return 0, data
}

// mt_seq int16   // 驱动电机序号           short     MotorSeq
// mt_s   int16   // 驱动电机状态           String    Status
// mc_t   int16   // 驱动电机控制器温度      String    ControllerTemperature
// mt_n   int32   // 驱动电机转速           String    MotorSpeed
// m_tq   float32 // 驱动电机转矩           String    MotorTorque
// mt_t   int16   // 驱动电机温度           String    MotorTemperature
// mt_v   float32 // 电机控制器输入电压      String    ControllerVoltage
// mt_i   float32 // 电机控制器直流母线电流   String    ControllerCurrent

func TestStructure(t *testing.T) {
	stu := &student{
		ID:     1,
		Name:   "Tom",
		Class:  "Class1",
		Grade:  "Grade1",
		Scores: []int16{100, 99, 98},
		Family: map[string]string{"father": "Jack", "mother": "Lily"},
	}
	encodeES := time.Now()

	headLen, raw := EncodeStructure(stu)

	fmt.Println("struct encode elapsed:", time.Since(encodeES))

	fmt.Println("struct head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opStructure := DecodeStructure(raw)
	if nil == opStructure {
		fmt.Println("decode struct failed")
		return
	}
	fmt.Println("struct decode elapsed:", time.Since(decodeES))

	opSlice := opStructure.GetSlice("Scores")
	iter := opSlice.Iterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Println("scores 's element:", opSlice.Get())
	}

	opMap := opStructure.GetMap("Family")
	if nil == opMap {
		fmt.Println("get family failed")
		return
	}
	opMap.Range(func(key, value interface{}) bool {
		fmt.Println("family key:", key, "val:", value)
		return true
	})
}

func TestCompoundStructure(t *testing.T) {
	stu := student{
		ID:     1,
		Name:   "Tom",
		Class:  "Class1",
		Grade:  "Grade1",
		Scores: []int16{100, 100, 100},
		Family: map[string]string{"father": "Jack", "mother": "Lily"},
	}
	stu1 := student{
		ID:     2,
		Name:   "Green",
		Class:  "Class1",
		Grade:  "Grade1",
		Scores: []int16{99, 99, 99},
		Family: map[string]string{"father": "Jack", "mother": "Lily"},
	}
	stu2 := student{
		ID:     3,
		Name:   "Jea",
		Class:  "Class1",
		Grade:  "Grade1",
		Scores: []int16{98, 98, 98},
		Family: map[string]string{"father": "Jack", "mother": "Lily"},
	}
	stuMap := map[int32]student{1: stu, 2: stu1, 3: stu2}

	titles := []string{"t1", "t2", "t3"}

	tool := classTool{"tool1", "tool2", "tool3"}

	cl := class{
		Name:     "Class1",
		Titles:   titles,
		Students: stuMap,
		Tool:     tool,
	}

	encodeES := time.Now()

	headLen, raw := EncodeStructure(cl)

	fmt.Println("struct encode elapsed:", time.Since(encodeES))

	fmt.Println("struct head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opStructure := DecodeStructure(raw)
	if nil == opStructure {
		fmt.Println("decode struct failed")
		return
	}
	fmt.Println("struct decode elapsed:", time.Since(decodeES))

	fmt.Println("class name:", opStructure.GetString("Name"))

	titleList := opStructure.GetSlice("Titles")
	titleIter := titleList.Iterator()
	for titleIter.SeekToFirst(); titleIter.Valid(); titleIter.Next() {
		fmt.Println("titles element:", titleList.Get())
	}

	tools := opStructure.GetStructure("Tool")
	tools.Range(func(key string, value interface{}) bool {
		fmt.Println("tool field, key:", key, "val:", value)
		return true
	})

	opMap := opStructure.GetMap("Students")
	if nil == opMap {
		fmt.Println("student map was empty")
		return
	}
	opMap.Range(func(key, value interface{}) bool {
		fmt.Println("student key:", key)

		student := value.(*OPStructure)
		fields := student.GetAllField()
		if 0 == len(fields) {
			return false
		}

		for _, field := range fields {
			switch field.Type {
			case VtBool:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetBool(field.Name))

			case VtInt8:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetInt8(field.Name))

			case VtInt16:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetInt16(field.Name))

			case VtInt32:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetInt32(field.Name))

			case VtInt64:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetInt64(field.Name))

			case VtFloat:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetFloat(field.Name))

			case VtDouble:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetDouble(field.Name))

			case VtString:
				fmt.Println(field.Name, field.Tag.Get("json"), field.Type, student.GetString(field.Name))

			case VtSlice:
				opSlice := student.GetSlice(field.Name)
				iter := opSlice.Iterator()
				for iter.SeekToFirst(); iter.Valid(); iter.Next() {
					fmt.Println(field.Name, field.Tag.Get("json"), field.Type, opSlice.GetDataType(), opSlice.Get())
				}

			case VtMap:
				subMap := student.GetMap(field.Name)
				subMap.Range(func(subMapKey, subMapVal interface{}) bool {
					fmt.Println(field.Name, field.Tag.Get("json"), field.Type, subMap.GetKeyType(), subMap.GetValType(), subMapKey, subMapVal)
					return true
				})

			case VtStructure:
				subStruct := student.GetStructure(field.Name)
				subStruct.Range(func(subStructKey string, subStructVal interface{}) bool {
					fmt.Println(field.Name, field.Tag.Get("json"), field.Type, subStructKey, subStructVal)
					return true
				})
			}
		}
		return true
	})
}

func TestEmptyStructure(t *testing.T) {
	stu := student{}

	encodeES := time.Now()

	headLen, raw := EncodeStructure(stu)

	fmt.Println("struct encode elapsed:", time.Since(encodeES))

	fmt.Println("struct head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	decodeES := time.Now()

	opStructure := DecodeStructure(raw)
	if nil == opStructure {
		fmt.Println("decode struct failed")
		return
	}
	fmt.Println("struct decode elapsed:", time.Since(decodeES))

	fields := opStructure.GetAllField()
	for _, field := range fields {
		fmt.Println("fieldName:", field.Name, "tag:", field.Tag, "type:", field.Type, "pos:", field.pos, "dataLen:", field.dataLen)
	}

	id := opStructure.GetInt32("ID")
	fmt.Println("id:", id)

	name := opStructure.GetString("Name")
	fmt.Println("name:", name)

	class := opStructure.GetString("Class")
	fmt.Println("class:", class)

	scores := opStructure.GetSlice("Scores")
	if scores.IsEmpty() {
		fmt.Println("Scores is empty")
	} else {
		iter := scores.Iterator()
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			fmt.Println("score:", scores.Get())
		}
	}

	family := opStructure.GetMap("Family")
	if family.IsEmpty() {
		fmt.Println("family is empty")
	} else {
		family.Range(func(key, value interface{}) bool {
			fmt.Println("family key:", key, "value:", value)
			return true
		})
	}
}
