package tsxor

// MaxUint64 is the max unit64 value
const MaxUint64 = uint64(18446744073709551615)

// BitStream class
type BitStream struct {
	mSize         uint64
	mFreeSlots    uint64
	mUsedSlots    uint64
	closed        bool
	currentBucket *uint64
	data          []uint64
}

func assert(b bool) {
	if !b {
		panic("Assert failed")
	}
}
func min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b

}

//New initializes the BitStream
func (b *BitStream) New() {
	b.closed = false
	b.mSize = 0
	b.mFreeSlots = 64
	b.mUsedSlots = 0
}

//ReadFromRawData allows to read data from a raw slice of uint64, require 'm' (the size of the BitStream)
func (b *BitStream) ReadFromRawData(data *[]uint64, m uint64) {
	b.data = *data
	b.mSize = m
	b.closed = true
	b.currentBucket = &b.data[0]
	b.mUsedSlots = 64
}

// Add allows to add 'length' bits to the BitStream taken from 'bits'
func (b *BitStream) Add(bits uint64, length uint64) {
	assert(!b.closed)
	assert(length == 64 || (bits>>length) == 0)
	b.mSize += length
	if b.mFreeSlots == 64 {
		b.data = append(b.data, bits)
	} else {
		var shift = min(b.mFreeSlots, length)
		*b.currentBucket = (*b.currentBucket << shift) ^ (bits >> (length - shift))
		if length > b.mFreeSlots {
			bits &= ^(MaxUint64 << (length - shift))
			b.data = append(b.data, bits)
		}
	}
	b.mFreeSlots = 64 - (b.mSize % 64)
	b.currentBucket = &b.data[len(b.data)-1]
}

//Close will close the BitStream. Once closed, a BitStream is set in consume-only mode.
func (b *BitStream) Close() {
	b.Add(0x0F, 4)
	b.Add(MaxUint64, 64)
	b.Add(MaxUint64, 64)
	b.Add(0, 1)
	b.closed = true
	if b.mSize < 64 {
		b.mUsedSlots = b.mSize
	} else {
		b.mUsedSlots = 64
	}
	b.currentBucket = &b.data[0]
}

//Size of the BitStream
func (b *BitStream) Size() uint64 {
	return b.mSize
}

//Get 'length' bits from the BitStream
func (b *BitStream) Get(length uint64) uint64 {
	assert(b.closed)
	assert(length <= 64)
	assert(len(b.data) > 0)

	tBits := uint64(0)

	if length == b.mUsedSlots {
		tBits = *b.currentBucket
		b.data = b.data[1:]
		if len(b.data) > 0 {
			b.currentBucket = &b.data[0]
			if b.mSize < 64 {
				b.mUsedSlots = b.mSize
			} else {
				b.mUsedSlots = 64
			}
		} else {
			b.mUsedSlots = 0
		}

	} else if length < b.mUsedSlots {
		tBits = *b.currentBucket >> (b.mUsedSlots - length)
		mask := MaxUint64 << (b.mUsedSlots - length)
		*b.currentBucket &= (^mask)
		b.mUsedSlots -= length
	} else {
		tBits = *b.currentBucket
		b.data = b.data[1:]
		b.currentBucket = &b.data[0]
		tBits <<= length - b.mUsedSlots

		if len(b.data) > 1 {
			tBits ^= (*b.currentBucket >> (64 - length + b.mUsedSlots))
			mask := MaxUint64 << (64 - length + b.mUsedSlots)
			*b.currentBucket &= (^mask)
			b.mUsedSlots = 64 - length + b.mUsedSlots
		} else {
			tBits ^= (*b.currentBucket >> (b.mSize - length))
			mask := MaxUint64 << (b.mSize - length)
			*b.currentBucket &= (^mask)
			b.mUsedSlots = b.mSize - length
		}
	}
	b.mSize -= length
	return tBits
}

//nextZeroWithin returns the number of bits before seeing a zero in the BitStream
func (b *BitStream) nextZeroWithin(length uint64) uint64 {
	t := b.Get(1)
	res := t
	length--
	for (t != 0) && (length > 0) {
		t = b.Get(1)
		res = (res << 1) ^ t
		length--
	}
	return res
}
