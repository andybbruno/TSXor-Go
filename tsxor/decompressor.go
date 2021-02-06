package tsxor

import (
	"math"
)

// const firstDeltaBits = 32

const MaxUint32 = uint64(4294967295)

type Decompressor struct {
	window          []Window
	bstream         BitStream
	bytes           []byte
	blockTimestamp  int64
	storedDelta     int64
	endOfStream     bool
	nCols           uint64
	currentID       uint64
	StoredTimestamp int64
	StoredValues    []float64
}

func (d *Decompressor) New(bs *BitStream, data *[]byte, nCols uint64) {
	d.bstream = *bs
	d.bytes = *data
	d.blockTimestamp = int64(d.bstream.Get(64))
	d.nCols = nCols
	d.window = make([]Window, nCols)
	d.StoredValues = make([]float64, nCols)
	for i := range d.window {
		d.window[i].Init()
	}
}

func (d *Decompressor) HasNext() bool {
	d.next()
	return !d.endOfStream
}

func (d *Decompressor) readBytes(len uint64) uint64 {
	var val uint64

	for i := 0; i < int(len); i++ {
		val |= uint64(d.bytes[d.currentID])
		d.currentID++
		if i != int(len-1) {
			val <<= 8
		}
		if d.currentID == 17379095 {
			return 0
		}
	}
	return val
}

func (d *Decompressor) bitsToRead() uint64 {
	val := d.bstream.nextZeroWithin(4)
	toRead := 0

	switch val {
	case 0x00:
		break
	case 0x02:
		toRead = 7
		break
	case 0x06:
		toRead = 9
		break
	case 0x0e:
		toRead = 12
		break
	case 0x0F:
		toRead = 32
		break
	}

	return uint64(toRead)

}

func (d *Decompressor) zzDecode(i uint64) int64 {
	v := (i >> 1) ^ (-(i & 1))
	return int64(v)
}

func (d *Decompressor) next() {
	if d.StoredTimestamp == 0 {
		d.storedDelta = int64(d.bstream.Get(firstDeltaBits))
		if d.storedDelta == ((1 << 14) - 1) {
			d.endOfStream = true
			return
		}
		for i := 0; i < int(d.nCols); i++ {
			read := d.readBytes(8)
			d.window[i].Add(read)
			d.StoredValues[i] = math.Float64frombits(read)
		}
		d.StoredTimestamp = d.blockTimestamp + d.storedDelta
	} else {
		d.nextTimestamp()
	}
}

func (d *Decompressor) nextTimestamp() {
	var dDelta int64
	toRead := d.bitsToRead()
	if toRead > 0 {
		deltaDelta := d.bstream.Get(toRead)
		if toRead == 32 {
			if deltaDelta == MaxUint32 {
				d.endOfStream = true
				return
			}
		}
		dDelta = d.zzDecode(deltaDelta)
	}
	d.storedDelta += dDelta
	d.StoredTimestamp += d.storedDelta

	d.nextValue()
}

func (d *Decompressor) nextValue() {
	var finalVal uint64
	for i := 0; i < int(d.nCols); i++ {
		head := d.readBytes(1)
		if head < 0x80 {
			finalVal = d.window[i].At(head)
		} else if head == 0xFF {
			finalVal = d.readBytes(8)
		} else {
			offset := head & 0x7F
			info := d.readBytes(1)
			trailZeroBytes := info >> 4
			xorBytes := info & 0xF
			xorValue := d.readBytes(xorBytes) << (8 * trailZeroBytes)
			finalVal = xorValue ^ d.window[i].At(offset)
		}
		d.window[i].Add(finalVal)
		d.StoredValues[i] = math.Float64frombits(finalVal)
	}
}
