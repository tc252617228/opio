package opio

type Iterator interface {
	// reset vernier
	SeekToFirst()

	// returns, current vernier pos
	curr() (int, int)

	// is end
	Valid() bool

	// to next block
	Next()

	// special position
	At(pos int)

	// element number
	Number() int
}

type byteIterator struct {
	start   int // 起始点
	vernier int // 游标
	total   int // 总长
}

// 定长iterator -
type fixByteIterator struct {
	byteIterator
	step int // 步长
}

func newFixByteIterator(start, step, total int) *fixByteIterator {
	res := &fixByteIterator{
		step: step,
	}
	res.start = start
	res.total = total

	return res
}

func (iter *fixByteIterator) SeekToFirst() {
	iter.vernier = iter.start
}

func (iter *fixByteIterator) curr() (int, int) {
	return iter.vernier, iter.vernier + iter.step
}

func (iter *fixByteIterator) Valid() bool {
	return iter.vernier != iter.total
}

func (iter *fixByteIterator) Next() {
	iter.vernier += iter.step
}

func (iter *fixByteIterator) At(pos int) {
	curr := iter.start + iter.step*pos
	if curr >= iter.total || curr+iter.step > iter.total {
		// reset vernier
		iter.vernier = iter.start
		return
	}
	iter.vernier = curr
}

func (iter *fixByteIterator) Number() int {
	return (iter.total - iter.start) / iter.step
}

// 变长iterator -
type varByteIterator struct {
	byteIterator
	indexVernier int   // 索引游标
	steps        []int // 长度列表
}

func newVarByteIterator(steps []int, start, total int) *varByteIterator {
	res := &varByteIterator{
		steps: steps,
	}
	res.start = start
	res.total = total

	return res
}

func (iter *varByteIterator) SeekToFirst() {
	iter.vernier = iter.start
	iter.indexVernier = 0
}

func (iter *varByteIterator) curr() (int, int) {
	step := iter.steps[iter.indexVernier]
	return iter.vernier, iter.vernier + step
}

func (iter *varByteIterator) Valid() bool {
	return iter.indexVernier != len(iter.steps)
}

func (iter *varByteIterator) Next() {
	step := iter.steps[iter.indexVernier]
	iter.vernier += step
	iter.indexVernier++
}

func (iter *varByteIterator) At(pos int) {
	steps := iter.steps
	stepLen := len(steps)
	if pos >= stepLen || pos+steps[stepLen-1] > iter.total {
		// reset
		iter.vernier = iter.start
		iter.indexVernier = 0
		return
	}

	curr := iter.start
	i := 0
	for ; i < stepLen; i++ {
		if i == pos {
			break
		}
		curr += steps[i]
	}
	iter.vernier = curr
	iter.indexVernier = i
}

func (iter *varByteIterator) Number() int {
	return len(iter.steps)
}
