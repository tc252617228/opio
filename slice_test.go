package opio

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert" // 确保 assert 被导入
)

type teacher struct {
	ID    int32  `json:"id"`
	Name  string `json:"name"`
	Class string `json:"class"`
	Grade string `json:"grade"`
}

const dataLimit = 10

func TestFixedSlice(t *testing.T) {
	testSlice := make([]int64, dataLimit)
	for i := int64(0); i < dataLimit; i++ {
		testSlice[i] = i
	}
	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestFixedSlice: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		t.Fatalf("TestFixedSlice: DecodeSlice failed: %v", err)
	}
	if nil == opSlice {
		// DecodeSlice 返回 nil, nil 表示空二进制输入，这里不应发生
		t.Fatalf("TestFixedSlice: DecodeSlice returned nil slice for non-empty input")
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	iter := opSlice.Iterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		data := opSlice.Get()
		fmt.Println("slice element value:", data)
	}
}

func TestVarSlice(t *testing.T) {
	testSlice := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}

	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestVarSlice: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		t.Fatalf("TestVarSlice: DecodeSlice failed: %v", err)
	}
	if nil == opSlice {
		t.Fatalf("TestVarSlice: DecodeSlice returned nil slice for non-empty input")
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	iter := opSlice.Iterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		data := opSlice.Get()
		fmt.Println("slice element value:", data)
	}
}

func TestArraySlice(t *testing.T) {
	testSlice := [3][5]string{{"str1", "str2", "str3", "str4", "str5"},
		{"strA", "strB", "strC", "strD", "strE"},
		{"str_a", "str_b", "str_c", "str_d", "str_e"}}

	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestArraySlice: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		t.Fatalf("TestArraySlice: DecodeSlice failed: %v", err)
	}
	if nil == opSlice {
		t.Fatalf("TestArraySlice: DecodeSlice returned nil slice for non-empty input")
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	iter := opSlice.Iterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		val := opSlice.Get()
		subList := val.(*OPSlice)

		subIter := subList.Iterator()
		for subIter.SeekToFirst(); subIter.Valid(); subIter.Next() {
			fmt.Println("slice element value:", subList.Get())
		}
	}
}

func TestMapSlice(t *testing.T) {
	testSlice := []map[string]string{
		{"1": "a", "2": "b", "3": "c"},
		{"11": "aa", "22": "bb", "33": "cc"},
		{"111": "aaa", "222": "bbb", "333": "ccc"},
	}
	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestMapSlice: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		t.Fatalf("TestMapSlice: DecodeSlice failed: %v", err)
	}
	if nil == opSlice {
		t.Fatalf("TestMapSlice: DecodeSlice returned nil slice for non-empty input")
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	iter := opSlice.Iterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		val := opSlice.Get()
		subMap := val.(*OPMap)

		subMap.Range(func(key, value interface{}) bool {
			fmt.Println("slice element value:", key, value)
			return true
		})
	}
}

func TestStructureSlice(t *testing.T) {
	testSlice := []teacher{{1, "jack", "Class1", "Grade1"},
		{2, "jay", "Class2", "Grade1"},
		{3, "tom", "Class3", "Grade1"}}

	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestStructureSlice: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		t.Fatalf("TestStructureSlice: DecodeSlice failed: %v", err)
	}
	if nil == opSlice {
		t.Fatalf("TestStructureSlice: DecodeSlice returned nil slice for non-empty input")
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	iter := opSlice.Iterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		val := opSlice.Get()
		t := val.(*OPStructure)
		t.Range(func(key string, value interface{}) bool {
			fmt.Println("slice element value, key:", key, "val:", value)
			return true
		})
	}
}

func TestEmptySlice(t *testing.T) {
	testSlice := make([]int, 0)

	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestEmptySlice: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		// 对于空 slice 输入，DecodeSlice 应该返回 nil, nil
		// 如果返回错误，说明解码逻辑有问题
		t.Fatalf("TestEmptySlice: DecodeSlice failed for empty slice input: %v", err)
	}
	// 对于空 slice 输入，DecodeSlice 应该返回 opSlice == nil
	// if nil == opSlice { // 这行是错误的，空 slice 解码后 opSlice 不为 nil，但 IsEmpty() 为 true
	// 	fmt.Println("decode slice failed") // 移除旧的打印
	// 	return
	// }
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	// 对于空 slice 的二进制表示，DecodeSlice 应返回 nil
	// 使用 testify/assert 来检查 opSlice 是否为 nil
	assert.Nil(t, opSlice, "DecodeSlice for an empty slice should return nil")
	if opSlice == nil {
		fmt.Println("opSlice is correctly nil for an empty slice representation")
	}
	// 移除多余的括号
}

func TestFixedSliceAt(t *testing.T) {
	testSlice := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestFixedSliceAt: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		t.Fatalf("TestFixedSliceAt: DecodeSlice failed: %v", err)
	}
	if nil == opSlice {
		t.Fatalf("TestFixedSliceAt: DecodeSlice returned nil slice for non-empty input")
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	num := opSlice.Number()
	fmt.Println("slice number:", num)

	iter := opSlice.Iterator()
	for i := 0; i < num; i++ {
		iter.At(i)
		fmt.Println("opSlice.Get():", opSlice.Get())
	}
}

func TestFixedSliceIntX(t *testing.T) {

	testSlice_int8 := []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	testSlice_int16 := []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	testSlice_int32 := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	testSlice_int64 := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	encodeES := time.Now()
	var headLen int
	var raw []byte
	var maxSize = 10000 * 1000

	encodeES = time.Now()
	for i := 0; i < maxSize; i++ {
		headLen, raw = EncodeSliceInt8(testSlice_int8)
	}

	fmt.Println("slice EncodeSliceInt8 elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	encodeES = time.Now()
	for i := 0; i < maxSize; i++ {
		headLen, raw = EncodeSliceInt16(testSlice_int16)
	}

	fmt.Println("slice EncodeSliceInt16 elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	encodeES = time.Now()
	for i := 0; i < maxSize; i++ {
		headLen, raw = EncodeSliceInt32(testSlice_int32)
	}
	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	encodeES = time.Now()
	for i := 0; i < maxSize; i++ {
		headLen, raw = EncodeSliceInt64(testSlice_int64)
	}
	fmt.Println("slice EncodeSliceInt64 elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	//encodeES = time.Now()
	//for i := 0; i < maxSize; i++ {
	//	headLen, raw = EncodeSlice(testSlice_int16)
	//}
	//fmt.Println("slice encode elapsed:", time.Since(encodeES))
	//fmt.Println("slice head len:", headLen)
	//fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)
}

func TestVarSliceAt(t *testing.T) {
	testSlice := []string{"a", "b", "c", "d", "e", "f", "g"}
	encodeES := time.Now()

	headLen, raw, err := EncodeSlice(testSlice) // 添加 err
	if err != nil {
		t.Fatalf("TestVarSliceAt: EncodeSlice failed: %v", err)
	}

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice, err := DecodeSlice(raw) // 添加 err
	if err != nil {
		t.Fatalf("TestVarSliceAt: DecodeSlice failed: %v", err)
	}
	if nil == opSlice {
		t.Fatalf("TestVarSliceAt: DecodeSlice returned nil slice for non-empty input")
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	num := opSlice.Number()
	fmt.Println("slice number:", num)

	iter := opSlice.Iterator()
	for i := 0; i < num; i++ {
		iter.At(i)
		fmt.Println("opSlice.Get():", opSlice.Get())
	}
}
