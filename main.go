package robin

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/maphash"
	"io"
	"log/slog"
	"math"
	"math/bits"
	"os"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

func main() {
	flag.Parse()

	switch flag.Arg(0) {
	case "index":
		indexCommand(flag.Arg(1), flag.Arg(2))
	}
}

func indexCommand(dataPath string, measurementsPath string) {
	db := die2(pebble.Open(dataPath, nil))("creating idex database path=%q", dataPath)
	defer db.Close()

	ba := newBatch(db)

	file := die2(os.Open(measurementsPath))("opening measument file")
	defer file.Close()

	buf := make([]byte, 1024*1024)
	readStart := 0
	sep := []byte(":")
	var id uint64
	for {
		id++
		n, err := file.Read(buf[readStart:])
		if err != nil && err != io.EOF {
			die(err)("reading data")
		}
		if readStart+n == 0 {
			break
		}
		chunk := buf[:readStart+n]

		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			break
		}
		remaining := chunk[newline+1:]
		chunk = chunk[:newline+1]

		station, tempBytes, hasSemi := bytes.Cut(chunk, sep)
		if !hasSemi {
			continue
		}
		negative := false
		index := 0
		if tempBytes[index] == '-' {
			index++
			negative = true
		}
		temp := float64(tempBytes[index] - '0')
		index++
		if tempBytes[index] != '.' {
			temp = temp*10 + float64(tempBytes[index]-'0')
			index++
		}
		index++ // skip '.'
		temp += float64(tempBytes[index]-'0') / 10
		if negative {
			temp = -temp
		}
		if id == 5 {
			os.Exit(1)
		} else {
			fmt.Println(string(station), temp)
			continue
		}
		ba.Add(id, station, int64(temp))
		readStart = copy(buf, remaining)
	}
	ba.save()
	ba.ba.Close()
}

// FromFloat64 converts a float into a Decimal.
func FromFloat64(f float64) int64 {
	us := int64(f * math.Pow(10, 2))
	return us
}

func toStr(b []byte) string {
	return unsafe.String(&b[0], len(b))
}

type prefix byte

const (
	data prefix = 1 + iota
	translateID
	translateKey
	shardCount
)

type column byte

const (
	weather column = 1 + iota
	measure
)

const (
	prefixOffset    = 0
	columnOffset    = prefixOffset + 1
	shardOffset     = columnOffset + 1
	containerOffset = shardOffset + 8
	dataKeySize     = containerOffset + 8
)

type dataKey [dataKeySize]byte

func (da *dataKey) Set(col column, shard, container uint64) {
	da[prefixOffset] = byte(data)
	da[columnOffset] = byte(col)
	binary.BigEndian.PutUint64(da[shardOffset:], shard)
	binary.BigEndian.PutUint64(da[containerOffset:], container)
}

func makeTranslationKey(value []byte) []byte {
	return append([]byte{byte(translateKey)}, value...)
}

func makeTranslationID(value uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{byte(translateID)}, value)
}

var (
	seed = maphash.MakeSeed()
)

type batch struct {
	tr      *tr
	columns struct {
		weather *roaring.Bitmap
		measure *roaring.Bitmap
	}
	db    *pebble.DB
	ba    *pebble.Batch
	key   dataKey
	buf   bytes.Buffer
	shard uint64
}

const zeroSHard = ^uint64(0)

func newBatch(db *pebble.DB) *batch {
	b := &batch{
		shard: zeroSHard,
		db:    db,
		ba:    db.NewBatch(),
		tr:    newTr(),
	}
	b.columns.weather = roaring.NewBitmap()
	b.columns.measure = roaring.NewBitmap()
	return b
}

func (ba *batch) Add(id uint64, station []byte, measure int64) {
	shard := id / shardwidth.ShardWidth
	if shard != ba.shard {
		ba.shard = shard
	}
	ba.writeWeather(id, station)
	ba.writeMeasure(id, measure)
}

func (ba *batch) save() {
	die(ba.saveColumn(weather, ba.columns.weather))("saving weather")
	die(ba.saveColumn(measure, ba.columns.measure))("saving measure")
	die(ba.ba.Commit(nil))("commit batch")
	ba.ba = ba.db.NewBatch()
}

func (ba *batch) saveColumn(col column, ra *roaring.Bitmap) error {
	if !ra.Any() {
		return nil
	}
	ba.key.Set(col, ba.shard, 0)
	itr, _ := ra.Containers.Iterator(0)
	for itr.Next() {
		key, value := itr.Value()
		binary.BigEndian.PutUint64(ba.key[containerOffset:], key)
		err := ba.ba.Set(ba.key[:], value.Encode(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ba *batch) writeWeather(id uint64, station []byte) {
	value := ba.tr.Translate(ba.ba, station)
	ba.columns.weather.DirectAdd(
		value*shardwidth.ShardWidth +
			(id % shardwidth.ShardWidth),
	)
}

func (ba *batch) writeMeasure(id uint64, svalue int64) {
	ra := ba.columns.measure
	fragmentColumn := id % shardwidth.ShardWidth
	ra.DirectAdd(fragmentColumn)
	negative := svalue < 0
	var value uint64
	if negative {
		ra.DirectAdd(shardwidth.ShardWidth + fragmentColumn) // set sign bit
		value = uint64(svalue * -1)
	} else {
		value = uint64(svalue)
	}
	lz := bits.LeadingZeros64(value)
	row := uint64(2)
	for mask := uint64(0x1); mask <= 1<<(64-lz) && mask != 0; mask = mask << 1 {
		if value&mask > 0 {
			ra.DirectAdd(row*shardwidth.ShardWidth + fragmentColumn)
		}
		row++
	}
}

type tr struct {
	values map[uint64]uint64
	buf    struct {
		key []byte
		id  [9]byte
	}
	id uint64
}

func newTr() *tr {
	a := &tr{values: make(map[uint64]uint64)}
	a.buf.id[0] = byte(translateID)
	a.buf.key = make([]byte, 0, 1<<10)
	a.buf.key = append(a.buf.key, byte(translateKey))
	return a
}

func (tr *tr) Translate(ba *pebble.Batch, value []byte) uint64 {
	hash := maphash.Bytes(seed, value)
	if v, ok := tr.values[hash]; ok {
		return v
	}
	tr.id++
	tr.keys(value)

	die(ba.Set(tr.buf.id[:], value, nil))("creating translation id")
	die(ba.Set(tr.buf.key, value, nil))("creating translation key")
	tr.values[hash] = tr.id
	return tr.id
}

func (tr *tr) keys(value []byte) {
	binary.BigEndian.PutUint64(tr.buf.id[1:], tr.id)
	tr.buf.key = append(tr.buf.key[:1], value...)
}

func die(err error) func(msg string, args ...any) {
	return func(msg string, args ...any) {
		if err != nil {
			reason := fmt.Sprintf(msg, args...)
			slog.Error(reason, "err", err)
			os.Exit(1)
		}
	}
}

func die2[T any](v T, err error) func(msg string, args ...any) T {
	return func(msg string, args ...any) T {
		if err != nil {
			reason := fmt.Sprintf(msg, args...)
			slog.Error(reason, "err", err)
			os.Exit(1)
		}
		return v
	}
}
