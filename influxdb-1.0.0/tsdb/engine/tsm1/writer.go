package tsm1

/*
A TSM file is composed for four sections: header, blocks, index and the footer.

┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
│ Header │               Blocks               │    Index    │    Footer    │
│5 bytes │              N bytes               │   N bytes   │   4 bytes    │
└────────┴────────────────────────────────────┴─────────────┴──────────────┘

Header is composed of a magic number to identify the file type and a version
number.

┌───────────────────┐
│      Header       │
├─────────┬─────────┤
│  Magic  │ Version │
│ 4 bytes │ 1 byte  │
└─────────┴─────────┘

Blocks are sequences of pairs of CRC32 and data.  The block data is opaque to the
file.  The CRC32 is used for block level error detection.  The length of the blocks
is stored in the index.

┌───────────────────────────────────────────────────────────┐
│                          Blocks                           │
├───────────────────┬───────────────────┬───────────────────┤
│      Block 1      │      Block 2      │      Block N      │
├─────────┬─────────┼─────────┬─────────┼─────────┬─────────┤
│  CRC    │  Data   │  CRC    │  Data   │  CRC    │  Data   │
│ 4 bytes │ N bytes │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
└─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘

Following the blocks is the index for the blocks in the file.  The index is
composed of a sequence of index entries ordered lexicographically by key and
then by time.  Each index entry starts with a key length and key followed by a
count of the number of blocks in the file.  Each block entry is composed of
the min and max time for the block, the offset into the file where the block
is located and the the size of the block.

The index structure can provide efficient access to all blocks as well as the
ability to determine the cost associated with acessing a given key.  Given a key
and timestamp, we can determine whether a file contains the block for that
timestamp as well as where that block resides and how much data to read to
retrieve the block.  If we know we need to read all or multiple blocks in a
file, we can use the size to determine how much to read in a given IO.

┌────────────────────────────────────────────────────────────────────────────┐
│                                   Index                                    │
├─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───┤
│ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
│ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
└─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───┘

The last section is the footer that stores the offset of the start of the index.

┌─────────┐
│ Footer  │
├─────────┤
│Index Ofs│
│ 8 bytes │
└─────────┘
*/

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	// MagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 formatted file
	// 用于表示这是一个 tsm1 的文件
	MagicNumber uint32 = 0x16D116D1

	// 用于区分 tsm 文件版本号
	Version byte = 1

	// Size in bytes of an index entry
	indexEntrySize = 28

	// Size in bytes used to store the count of index entries for a key
	// Index 条目的数量
	indexCountSize = 2

	// Size in bytes used to store the type of block encoded
	// block 存储类型
	indexTypeSize = 1

	// Max number of blocks for a given key that can exist in a single file
	// 一个 key 在一个 tsm 文件中最多存在的 blocks 数量
	maxIndexEntries = (1 << (indexCountSize * 8)) - 1

	// max length of a key in an index entry (measurement + tags)
	// key 的长度最大值， key 为 measurement + tags
	maxKeyLength = (1 << (2 * 8)) - 1
)

var (
	ErrNoValues             = fmt.Errorf("no values written")
	ErrTSMClosed            = fmt.Errorf("tsm file closed")
	ErrMaxKeyLengthExceeded = fmt.Errorf("max key length exceeded")
	ErrMaxBlocksExceeded    = fmt.Errorf("max blocks exceeded")
)

// TSMWriter writes TSM formatted key and values.
type TSMWriter interface {
	// Write writes a new block for key containing and values.  Writes append
	// blocks in the order that the Write function is called.  The caller is
	// responsible for ensuring keys and blocks or sorted appropriately.
	// Values are encoded as a full block.  The caller is responsible for
	// ensuring a fixed number of values are encoded in each block as wells as
	// ensuring the Values are sorted. The first and last timestamp values are
	// used as the minimum and maximum values for the index entry.
	Write(key string, values Values) error

	// WriteBlock writes a new block for key containing the bytes in block.  WriteBlock appends
	// blocks in the order that the WriteBlock function is called.  The caller is
	// responsible for ensuring keys and blocks are sorted appropriately, and that the
	// block and index information is correct for the block.  The minTime and maxTime
	// timestamp values are used as the minimum and maximum values for the index entry.
	WriteBlock(key string, minTime, maxTime int64, block []byte) error

	// WriteIndex finishes the TSM write streams and writes the index.
	WriteIndex() error

	// Closes any underlying file resources.
	Close() error

	// Size returns the current size in bytes of the file
	Size() uint32
}

// IndexWriter writes a TSMIndex
type IndexWriter interface {
	// Add records a new block entry for a key in the index.
	Add(key string, blockType byte, minTime, maxTime int64, offset int64, size uint32)

	// Entries returns all index entries for a key.
	Entries(key string) []IndexEntry

	// Keys returns the unique set of keys in the index.
	Keys() []string

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Size returns the size of a the current index in bytes
	Size() uint32

	// MarshalBinary returns a byte slice encoded version of the index.
	MarshalBinary() ([]byte, error)

	// WriteTo writes the index contents to a writer
	WriteTo(w io.Writer) (int64, error)
}

// IndexEntry is the index information for a given block in a TSM file.
// 一个 block 在 tsm 文件中的索引信息
type IndexEntry struct {
	// The min and max time of all points stored in the block.
	// 一个 block 中的 point 都在这个最小和最大的时间范围内
	MinTime, MaxTime int64

	// The absolute position in the file where this block is located.
	// block 在 tsm 文件中偏移量
	Offset int64

	// The size in bytes of the block in the file.
	// block 的具体大小
	Size uint32
}

// UnmarshalBinary decodes an IndexEntry from a byte slice
// 反序列化一个 block 的 index
func (e *IndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) != indexEntrySize {
		return fmt.Errorf("unmarshalBinary: short buf: %v != %v", indexEntrySize, len(b))
	}
	e.MinTime = int64(binary.BigEndian.Uint64(b[:8]))
	e.MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))
	e.Offset = int64(binary.BigEndian.Uint64(b[16:24]))
	e.Size = binary.BigEndian.Uint32(b[24:28])
	return nil
}

// AppendTo will write a binary-encoded version of IndexEntry to b, allocating
// and returning a new slice, if necessary
func (e *IndexEntry) AppendTo(b []byte) []byte {
	if len(b) < indexEntrySize {
		if cap(b) < indexEntrySize {
			b = make([]byte, indexEntrySize)
		} else {
			b = b[:indexEntrySize]
		}
	}

	binary.BigEndian.PutUint64(b[:8], uint64(e.MinTime))
	binary.BigEndian.PutUint64(b[8:16], uint64(e.MaxTime))
	binary.BigEndian.PutUint64(b[16:24], uint64(e.Offset))
	binary.BigEndian.PutUint32(b[24:28], uint32(e.Size))

	return b
}

// Returns true if this IndexEntry may contain values for the given time.  The min and max
// times are inclusive.
func (e *IndexEntry) Contains(t int64) bool {
	return e.MinTime <= t && e.MaxTime >= t
}

func (e *IndexEntry) OverlapsTimeRange(min, max int64) bool {
	return e.MinTime <= max && e.MaxTime >= min
}

func (e *IndexEntry) String() string {
	return fmt.Sprintf("min=%s max=%s ofs=%d siz=%d",
		time.Unix(0, e.MinTime).UTC(), time.Unix(0, e.MaxTime).UTC(), e.Offset, e.Size)
}

func NewIndexWriter() IndexWriter {
	return &directIndex{
		blocks: map[string]*indexEntries{},
	}
}

// directIndex is a simple in-memory index implementation for a TSM file.  The full index
// must fit in memory.
// 单个 tsm 文件的索引信息在内存中的存储结构
type directIndex struct {
	mu     sync.RWMutex
	size   uint32						// Index 部分所占用的空间大小
	blocks map[string]*indexEntries
}

// 增加一个指定 key 的 block 索引信息
func (d *directIndex) Add(key string, blockType byte, minTime, maxTime int64, offset int64, size uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entries := d.blocks[key]
	if entries == nil {
		entries = &indexEntries{
			Type: blockType,
		}
		d.blocks[key] = entries
		// size of the key stored in the index
		d.size += uint32(2 + len(key))

		// size of the count of entries stored in the index
		d.size += indexCountSize
	}
	entries.entries = append(entries.entries, IndexEntry{
		MinTime: minTime,
		MaxTime: maxTime,
		Offset:  offset,
		Size:    size,
	})

	// size of the encoded index entry
	// 一个 block 的 index 的大小，28字节
	d.size += indexEntrySize
}

// 获取指定 key 的索引信息，需要注意加锁
func (d *directIndex) entries(key string) []IndexEntry {
	entries := d.blocks[key]
	if entries == nil {
		return nil
	}
	return entries.entries
}

// 获取指定 key 的索引信息
func (d *directIndex) Entries(key string) []IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.entries(key)
}

// 根据指定 key 和 时间 获取所在 block 的索引信息，不存在返回 nil
func (d *directIndex) Entry(key string, t int64) *IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entries := d.entries(key)
	for _, entry := range entries {
		if entry.Contains(t) {
			return &entry
		}
	}
	return nil
}

// 获取当前文件中所有的 key，key = measurement + tags
func (d *directIndex) Keys() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var keys []string
	for k := range d.blocks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// 获取当前 tsm 文件中 key 的数量，key = measurement + tags
func (d *directIndex) KeyCount() int {
	d.mu.RLock()
	n := len(d.blocks)
	d.mu.RUnlock()
	return n
}

// 批量增加索引信息
func (d *directIndex) addEntries(key string, entries *indexEntries) {
	existing := d.blocks[key]
	if existing == nil {
		d.blocks[key] = entries
		return
	}
	existing.entries = append(existing.entries, entries.entries...)
}

// 将当前内存中的所有索引信息写入到 tsm 文件中，返回总计写入的字节数
func (d *directIndex) WriteTo(w io.Writer) (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Index blocks are writtens sorted by key
	// 在文件中的索引信息需要按照 key 的字典序存储
	keys := make([]string, 0, len(d.blocks))
	for k := range d.blocks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var (
		n   int
		err error
		buf [5]byte
		N   int64
	)

	// For each key, individual entries are sorted by time
	// 在每一个 key 中，每一个条目按照时间排序
	for _, key := range keys {
		entries := d.blocks[key]

		if entries.Len() > maxIndexEntries {
			return N, fmt.Errorf("key '%s' exceeds max index entries: %d > %d", key, entries.Len(), maxIndexEntries)
		}
		sort.Sort(entries)

		// 2 字节 key 的长度
		binary.BigEndian.PutUint16(buf[0:2], uint16(len(key)))
		// 1 字节 block 的类型
		buf[2] = entries.Type
		// 2 字节属于这个 key 的 block 的索引数量
		binary.BigEndian.PutUint16(buf[3:5], uint16(entries.Len()))

		// Append the key length and key
		if n, err = w.Write(buf[0:2]); err != nil {
			return int64(n) + N, fmt.Errorf("write: writer key length error: %v", err)
		}
		N += int64(n)

		if n, err = io.WriteString(w, key); err != nil {
			return int64(n) + N, fmt.Errorf("write: writer key error: %v", err)
		}
		N += int64(n)

		// Append the block type and count
		if n, err = w.Write(buf[2:5]); err != nil {
			return int64(n) + N, fmt.Errorf("write: writer block type and count error: %v", err)
		}
		N += int64(n)

		// Append each index entry for all blocks for this key
		// 最后依次遍历该 key 中每一个 block 的索引条目，写入文件中
		// 8字节 MinTime, 8字节 MaxTime, 8字节 文件偏移量，4字节 block 长度
		var n64 int64
		if n64, err = entries.WriteTo(w); err != nil {
			return n64 + N, fmt.Errorf("write: writer entries error: %v", err)
		}
		N += n64

	}
	return N, nil
}

// 将索引内容序列化到 []byte 中
func (d *directIndex) MarshalBinary() ([]byte, error) {
	var b bytes.Buffer
	if _, err := d.WriteTo(&b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// 将 []byte 中的内容反序列化到内存的索引对象中
func (d *directIndex) UnmarshalBinary(b []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.size = uint32(len(b))

	var pos int
	for pos < len(b) {
		n, key, err := readKey(b[pos:])
		if err != nil {
			return fmt.Errorf("readIndex: read key error: %v", err)
		}
		pos += n

		var entries indexEntries
		n, err = readEntries(b[pos:], &entries)
		if err != nil {
			return fmt.Errorf("readIndex: read entries error: %v", err)
		}

		pos += n
		d.addEntries(string(key), &entries)
	}
	return nil
}

// 返回索引总计占用的大小信息
func (d *directIndex) Size() uint32 {
	return d.size
}

// tsmWriter writes keys and values in the TSM format
type tsmWriter struct {
	wrapped io.Writer
	w       *bufio.Writer
	index   IndexWriter
	n       int64
}

func NewTSMWriter(w io.Writer) (TSMWriter, error) {
	index := &directIndex{
		blocks: map[string]*indexEntries{},
	}

	return &tsmWriter{wrapped: w, w: bufio.NewWriterSize(w, 4*1024*1024), index: index}, nil
}

// 写入 Header 部分数据
func (t *tsmWriter) writeHeader() error {
	var buf [5]byte
	// 写入 MagicNumber 和 version 信息
	binary.BigEndian.PutUint32(buf[0:4], MagicNumber)
	buf[4] = Version

	n, err := t.w.Write(buf[:])
	if err != nil {
		return err
	}
	t.n = int64(n)
	return nil
}

// 将一个 block 的数据写入 tsm file 文件
func (t *tsmWriter) Write(key string, values Values) error {
	// Nothing to write
	if len(values) == 0 {
		return nil
	}

	if len(key) > maxKeyLength {
		return ErrMaxKeyLengthExceeded
	}

	// Write header only after we have some data to write.
	// 如果当前文件第一次写入数据，先写入 Header 部分
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	block, err := values.Encode(nil)
	if err != nil {
		return err
	}

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}

	// 写入 crc32 值
	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

	_, err = t.w.Write(checksum[:])
	if err != nil {
		return err
	}

	// 写入 block 内容
	n, err := t.w.Write(block)
	if err != nil {
		return err
	}
	n += len(checksum)

	// Record this block in index
	t.index.Add(key, blockType, values[0].UnixNano(), values[len(values)-1].UnixNano(), t.n, uint32(n))

	// Increment file position pointer
	t.n += int64(n)
	return nil
}

// WriteBlock writes block for the given key and time range to the TSM file.  If the write
// exceeds max entries for a given key, ErrMaxBlocksExceeded is returned.  This indicates
// that the index is now full for this key and no future writes to this key will succeed.
func (t *tsmWriter) WriteBlock(key string, minTime, maxTime int64, block []byte) error {
	// Nothing to write
	if len(block) == 0 {
		return nil
	}

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}

	// Write header only after we have some data to write.
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

	_, err = t.w.Write(checksum[:])
	if err != nil {
		return err
	}

	n, err := t.w.Write(block)
	if err != nil {
		return err
	}
	n += len(checksum)

	// Record this block in index
	t.index.Add(key, blockType, minTime, maxTime, t.n, uint32(n))

	// Increment file position pointer (checksum + block len)
	t.n += int64(n)

	if len(t.index.Entries(key)) >= maxIndexEntries {
		return ErrMaxBlocksExceeded
	}

	return nil
}

// WriteIndex writes the index section of the file.  If there are no index entries to write,
// this returns ErrNoValues
func (t *tsmWriter) WriteIndex() error {
	indexPos := t.n

	if t.index.KeyCount() == 0 {
		return ErrNoValues
	}

	// Write the index
	if _, err := t.index.WriteTo(t.w); err != nil {
		return err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(indexPos))

	// Write the index index position
	_, err := t.w.Write(buf[:])
	return err
}

func (t *tsmWriter) Close() error {
	if err := t.w.Flush(); err != nil {
		return err
	}

	if f, ok := t.wrapped.(*os.File); ok {
		if err := f.Sync(); err != nil {
			return err
		}
	}

	if c, ok := t.wrapped.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (t *tsmWriter) Size() uint32 {
	return uint32(t.n) + t.index.Size()
}

// verifyVersion will verify that the reader's bytes are a TSM byte
// stream of the correct version (1)
// 检查 reader 对象对应的是否是一个 tsm 文件流对应的格式，主要是 MagicNumber 和 version 的比对
func verifyVersion(r io.ReadSeeker) error {
	_, err := r.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("init: failed to seek: %v", err)
	}
	var b [4]byte
	_, err = io.ReadFull(r, b[:])
	if err != nil {
		return fmt.Errorf("init: error reading magic number of file: %v", err)
	}
	// 检查文件头4个字节是否是 MagicNumber
	if binary.BigEndian.Uint32(b[:]) != MagicNumber {
		return fmt.Errorf("can only read from tsm file")
	}
	_, err = io.ReadFull(r, b[:1])
	if err != nil {
		return fmt.Errorf("init: error reading version: %v", err)
	}
	// 检查 tsm 文件版本是否符合当前要求
	if b[0] != Version {
		return fmt.Errorf("init: file is version %b. expected %b", b[0], Version)
	}

	return nil
}
