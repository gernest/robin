package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/maphash"
	"log/slog"
	"math"
	"math/bits"
	"os"

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

type iter struct {
	lo    dataKey
	hi    dataKey
	it    *pebble.Iterator
	shard uint64
}

func (i *iter) reset(co column) bool {
	i.lo.Set(co, i.shard, 0)
	i.hi.Set(co, i.shard, math.MaxUint64)
	return i.it.SeekGE(i.lo[:])
}

func (i *iter) Valid() bool {
	return i.it.Valid() &&
		bytes.Compare(i.it.Key(), i.hi[:]) == -1
}

func (i *iter) Next() bool {
	return i.it.Next() && i.Valid()
}

func (i *iter) BitLen() uint64 {
	return i.Max() / shardwidth.ShardWidth
}

func (i *iter) Value() (uint64, *roaring.Container) {
	key := i.it.Key()
	return binary.BigEndian.Uint64(key[len(key)-8:]),
		roaring.DecodeContainer(i.it.Value())
}

func (i *iter) Max() uint64 {
	if !i.it.SeekLT(i.hi[:]) {
		return 0
	}
	key := i.it.Key()
	if bytes.Compare(key, i.lo[:]) == -1 {
		return 0
	}
	ck := binary.BigEndian.Uint64(key[len(key)-8:])
	value := roaring.LastValueFromEncodedContainer(i.it.Value())
	return uint64((ck << 16) | uint64(value))
}

func (i *iter) Sum(filter *roaring.Bitmap) (count int32, total int64) {
	fs := roaring.NewBitmapBSICountFilter(filter)
	i.ApplyFilter(0, fs)
	return fs.Total()
}

func (i *iter) Row(rowID uint64) *roaring.Bitmap {
	return i.OffsetRange(
		shardwidth.ShardWidth*i.shard,
		shardwidth.ShardWidth*rowID,
		shardwidth.ShardWidth*(rowID+1),
	)
}

func (i *iter) Seek(key uint64) bool {
	ls := i.lo[:]
	binary.BigEndian.PutUint64(ls[len(ls)-8:], key)
	return i.it.SeekGE(ls) && i.Valid()
}

func (i *iter) ApplyFilter(key uint64, filter roaring.BitmapFilter) (err error) {
	if !i.Seek(key) {
		return
	}
	var minKey roaring.FilterKey

	for ; i.Valid(); i.it.Next() {
		dk := i.it.Key()
		ckey := binary.BigEndian.Uint64(dk[len(dk)-8:])
		key := roaring.FilterKey(ckey)
		if key < minKey {
			continue
		}
		// Because ne never delete, we are sure that no empty container is ever
		// stored. We pass 1 as cardinality to signal that there is bits ina container.
		//
		// Filters only use this to check for empty containers.
		res := filter.ConsiderKey(key, 1)
		if res.Err != nil {
			return res.Err
		}
		if res.YesKey <= key && res.NoKey <= key {
			data := roaring.DecodeContainer(i.it.Value())
			res = filter.ConsiderData(key, data)
			if res.Err != nil {
				return res.Err
			}
		}
		minKey = res.NoKey
		if minKey > key+1 {
			if !i.Seek(uint64(minKey)) {
				return nil
			}
		}
	}
	return nil

}

func (i *iter) OffsetRange(offset, start, endx uint64) *roaring.Bitmap {
	dieFalse(lowbits(offset) == 0)("low bits set")
	dieFalse(lowbits(start) == 0)("low bits set")
	dieFalse(lowbits(endx) == 0)("low bits set")

	other := roaring.NewSliceBitmap()
	off := highbits(offset)
	hi0, hi1 := highbits(start), highbits(endx)
	if !i.Seek(hi0) {
		return other
	}
	for ; i.Valid(); i.it.Next() {
		key := i.it.Key()
		ckey := binary.BigEndian.Uint64(key[len(key)-8:])
		if ckey >= hi1 {
			break
		}
		other.Containers.Put(off+(ckey-hi0), roaring.DecodeContainer(i.it.Value()).Clone())
	}
	return other
}

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

func indexCommand(dataPath string, measurementsPath string) {
	db := die2(pebble.Open(dataPath, nil))("creating idex database path=%q", dataPath)
	defer db.Close()

	ba := newBatch(db)

	file := die2(os.Open(measurementsPath))("opening measument file")
	defer file.Close()

	var id uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		id++
		line := scanner.Bytes()

		end := len(line)
		tenths := int32(line[end-1] - '0')
		ones := int32(line[end-3] - '0') // line[end-2] is '.'
		var temp int32
		var semicolon int
		if line[end-4] == ';' {
			temp = ones*10 + tenths
			semicolon = end - 4
		} else if line[end-4] == '-' {
			temp = -(ones*10 + tenths)
			semicolon = end - 5
		} else {
			tens := int32(line[end-4] - '0')
			if line[end-5] == ';' {
				temp = tens*100 + ones*10 + tenths
				semicolon = end - 5
			} else { // '-'
				temp = -(tens*100 + ones*10 + tenths)
				semicolon = end - 6
			}
		}

		station := line[:semicolon]
		ba.Add(id, station, int64(temp))
	}
	ba.Finish()
	die(db.Compact([]byte{0}, []byte{byte(translateKey + 1)}, true))("running compaction")
}

type prefix byte

const (
	data prefix = 1 + iota
	translateID
	translateKey
)

type column byte

const (
	weather column = 1 + iota
	measure
	shards
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
	shards *roaring.Bitmap
	db     *pebble.DB
	ba     *pebble.Batch
	key    dataKey
	buf    bytes.Buffer
	shard  uint64
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
	b.shards = roaring.NewBitmap()
	return b
}

func (ba *batch) Add(id uint64, station []byte, measure int64) {
	shard := id / shardwidth.ShardWidth
	if shard != ba.shard {
		if ba.shard != zeroSHard {
			ba.save()
		}
		ba.shard = shard
	}
	ba.writeWeather(id, station)
	ba.writeMeasure(id, measure)
}

func (ba *batch) Finish() {
	ba.saveColumn(shards, ba.shards, 0)
	ba.save()
	ba.ba.Close()
}

func (ba *batch) save() {
	die(ba.saveColumn(weather, ba.columns.weather, ba.shard))("saving weather")
	die(ba.saveColumn(measure, ba.columns.measure, ba.shard))("saving measure")
	die(ba.ba.Commit(nil))("commit batch")
	ba.columns.weather.Containers.Reset()
	ba.columns.measure.Containers.Reset()
	ba.ba = ba.db.NewBatch()
}

func (ba *batch) saveColumn(col column, ra *roaring.Bitmap, shard uint64) error {
	if !ra.Any() {
		return nil
	}
	ra.Optimize()
	ba.key.Set(col, shard, 0)
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
	encodeCard(ba.columns.weather, id, value)

	// track which shards the weather station was found
	encodeCard(ba.shards, ba.shard, value)
}

func encodeCard(ra *roaring.Bitmap, id uint64, value uint64) {
	ra.DirectAdd(
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

func dieFalse(ok bool) func(msg string, args ...any) {
	return func(msg string, args ...any) {
		if !ok {
			slog.Error(msg, args...)
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
