package tsm1

import "sync"

// 各种类型的内存池实现

var (
	bufPool          sync.Pool
	float64ValuePool sync.Pool
	integerValuePool sync.Pool
	booleanValuePool sync.Pool
	stringValuePool  sync.Pool
)

// getBuf returns a buffer with length size from the buffer pool.
// 从内存池中获取一个指定大小的 buff 对象
func getBuf(size int) []byte {
	x := bufPool.Get()
	if x == nil {
		return make([]byte, size)
	}
	buf := x.([]byte)
	if cap(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
// 将 []byte 对象放入内存池中
func putBuf(buf []byte) {
	bufPool.Put(buf)
}

// getBuf returns a buffer with length size from the buffer pool.
func getFloat64Values(size int) []Value {
	var buf []Value
	x := float64ValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = &FloatValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putFloat64Values(buf []Value) {
	float64ValuePool.Put(buf)
}

// getBuf returns a buffer with length size from the buffer pool.
func getIntegerValues(size int) []Value {
	var buf []Value
	x := integerValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = &IntegerValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putIntegerValues(buf []Value) {
	integerValuePool.Put(buf)
}

// getBuf returns a buffer with length size from the buffer pool.
func getBooleanValues(size int) []Value {
	var buf []Value
	x := booleanValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = &BooleanValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putStringValues(buf []Value) {
	stringValuePool.Put(buf)
}

// getBuf returns a buffer with length size from the buffer pool.
func getStringValues(size int) []Value {
	var buf []Value
	x := stringValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = &StringValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putBooleanValues(buf []Value) {
	booleanValuePool.Put(buf)
}
func putValue(buf []Value) {
	if len(buf) > 0 {
		switch buf[0].(type) {
		case *FloatValue:
			putFloat64Values(buf)
		case *IntegerValue:
			putIntegerValues(buf)
		case *BooleanValue:
			putBooleanValues(buf)
		case *StringValue:
			putStringValues(buf)
		}
	}
}
