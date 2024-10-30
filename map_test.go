package opio

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestSimpleMap(t *testing.T) {
	testMap := make(map[int]string, 10)
	for i := 0; i < dataLimit; i++ {
		testMap[i] = "ele>>>>>" + strconv.FormatInt(int64(i), 10)
	}

	encodeES := time.Now()

	headLen, raw := EncodeMap(testMap)

	fmt.Println("map encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opMap := DecodeMap(raw)
	if nil == opMap {
		fmt.Println("decode map failed")
		return
	}
	fmt.Println("map decode elapsed:", time.Since(decodeES))
	for i := 0; i < dataLimit; i++ {
		fmt.Println("find opMap's value:", opMap.Find(i))
	}

	opMap.Range(func(key, value interface{}) bool {
		fmt.Println("map element, key:", key, "value:", value)
		return true
	})
}

func TestVarKeyMap(t *testing.T) {
	testMap := make(map[string]int, 10)
	for i := 0; i < dataLimit; i++ {
		key := "key>>>" + strconv.FormatInt(int64(i), 10)
		testMap[key] = i
	}

	encodeES := time.Now()

	headLen, raw := EncodeMap(testMap)

	fmt.Println("map encode elapsed:", time.Since(encodeES))

	fmt.Println("map head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opMap := DecodeMap(raw)
	if nil == opMap {
		fmt.Println("decode map failed")
		return
	}
	fmt.Println("map decode elapsed:", time.Since(decodeES))
	opMap.Range(func(key, val interface{}) bool {
		fmt.Println("key:", key, "val:", val)
		return true
	})
}

func TestSliceMap(t *testing.T) {
	testMap := map[string][]string{
		"test1":  {"aa1", "bb1", "cc1"},
		"test2":  {"dd2", "ee2", "ff2"},
		"test3":  {"dd3", "ee3", "ff3"},
		"test4":  {"dd4", "ee4", "ff4"},
		"test5":  {"dd5", "ee5", "ff5"},
		"test6":  {"dd6", "ee6", "ff6"},
		"test7":  {"dd7", "ee7", "ff7"},
		"test8":  {"dd8", "ee8", "ff8"},
		"test9":  {"dd9", "ee9", "ff9"},
		"test10": {"dd10", "ee10", "ff10"},
	}
	encodeES := time.Now()

	headLen, raw := EncodeMap(testMap)

	fmt.Println("map encode elapsed:", time.Since(encodeES))

	fmt.Println("map head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opMap := DecodeMap(raw)
	if nil == opMap {
		fmt.Println("decode map failed")
		return
	}
	fmt.Println("map decode elapsed:", time.Since(decodeES))

	opMap.Range(func(key, val interface{}) bool {
		fmt.Println("key:", key)

		opSlice := val.(*OPSlice)
		iter := opSlice.Iterator()
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			fmt.Println("val:", opSlice.Get())
		}
		return true
	})
}

func TestMapMap(t *testing.T) {
	testMap := map[string]map[string]string{
		"test1": {"aa1": "11", "aa2": "22", "aa3": "33"},
		"test2": {"aa1": "11", "aa2": "22", "aa3": "33"},
		"test3": {"aa1": "11", "aa2": "22", "aa3": "33"},
	}

	encodeES := time.Now()

	headLen, raw := EncodeMap(testMap)

	fmt.Println("map encode elapsed:", time.Since(encodeES))

	fmt.Println("map head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opMap := DecodeMap(raw)
	if nil == opMap {
		fmt.Println("decode map failed")
		return
	}
	fmt.Println("map decode elapsed:", time.Since(decodeES))

	key := "test1"
	val := opMap.Find(key)
	subMap := val.(*OPMap)
	for i := 1; i <= 3; i++ {
		subKey := "aa" + strconv.FormatInt(int64(i), 10)
		fmt.Println("sub map value:", subMap.Find(subKey))
	}
}

func TestStructureMap(t *testing.T) {
	testMap := map[int32]teacher{
		1: {1, "jack", "Class1", "Grade1"},
		2: {2, "jay", "Class2", "Grade1"},
		3: {3, "tom", "Class3", "Grade1"}}
	encodeES := time.Now()

	headLen, raw := EncodeMap(testMap)

	fmt.Println("map encode elapsed:", time.Since(encodeES))

	fmt.Println("slice head len:", headLen)
	fmt.Println("raw len:", len(raw))
	//fmt.Println("raw:", raw)

	decodeES := time.Now()

	opMap := DecodeMap(raw)
	if nil == opMap {
		fmt.Println("decode map failed")
		return
	}
	fmt.Println("map decode elapsed:", time.Since(decodeES))

	opMap.Range(func(key, value interface{}) bool {
		t := value.(*OPStructure)
		t.Range(func(tKey string, tVal interface{}) bool {
			fmt.Println("map key:", key, "subKey:", tKey, "tVal:", tVal)
			return true
		})
		return true
	})
}

func TestEmptyMap(t *testing.T) {
	testMap := make(map[int]string)

	encodeES := time.Now()

	headLen, raw := EncodeMap(testMap)

	fmt.Println("map encode elapsed:", time.Since(encodeES))

	fmt.Println("map head len:", headLen)
	fmt.Println("raw len:", len(raw))
	fmt.Println("raw:", raw)

	decodeES := time.Now()

	opMap := DecodeMap(raw)
	if nil == opMap {
		fmt.Println("decode map failed")
		return
	}
	fmt.Println("map decode elapsed:", time.Since(decodeES))

	if opMap.IsEmpty() {
		fmt.Println("map is empty")
	} else {
		opMap.Range(func(key, val interface{}) bool {
			fmt.Println("key:", key, "val:", val)
			return true
		})
	}
}
