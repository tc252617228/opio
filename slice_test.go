package opio

import (
	"fmt"
	"testing"
	"time"
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

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
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

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
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

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
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

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
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

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
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

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
	}
	fmt.Println("slice decode elapsed:", time.Since(decodeES))

	if opSlice.IsEmpty() {
		fmt.Println("slice was empty")
	} else {
		iter := opSlice.Iterator()
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			fmt.Println("slice element value:", opSlice.Get())
		}
	}
}

func TestFixedSliceAt(t *testing.T) {
	testSlice := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	encodeES := time.Now()

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
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

	headLen, raw := EncodeSlice(testSlice)

	fmt.Println("slice encode elapsed:", time.Since(encodeES))
	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opSlice := DecodeSlice(raw)
	if nil == opSlice {
		fmt.Println("decode slice failed")
		return
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
