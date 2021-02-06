package tsxor

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

const NotANum = 0x7FF8000000000000

const firstDeltaBits = 32

const mask7 = 0x02 << 7
const mask9 = 0x06 << 9
const mask12 = 0x0E << 12

// Compressor class
type Compressor struct {
	window          []Window
	bstream         BitStream
	bytes           []byte
	blockTimestamp  int64
	storedTimestamp int64
	storedDelta     int64
}

//New initializes the Compressor
func (c *Compressor) New(timestamp int64) {
	c.bstream.New()
	c.blockTimestamp = timestamp
	c.bstream.Add(uint64(timestamp), 64)
}

func (c *Compressor) AddValue(timestamp int64, vals *[]float64) {
	if c.storedTimestamp == 0 {
		c.window = make([]Window, len(*vals))
		for i := range c.window {
			c.window[i].Init()
		}

		c.writeFirst(timestamp, vals)
	} else {
		c.compressTimestamp(timestamp)
		c.compressValues(vals)
	}
}

func (c *Compressor) writeFirst(timestamp int64, vals *[]float64) {
	c.storedDelta = timestamp - c.blockTimestamp
	c.storedTimestamp = timestamp
	c.bstream.Add(uint64(c.storedDelta), firstDeltaBits)
	for i, v := range *vals {
		x := math.Float64bits(v)
		c.append64(x)
		c.window[i].Add(x)
	}
}

func (c *Compressor) Close() {
	c.bstream.Close()
}

func (c *Compressor) CompressedSize() uint64 {
	return c.bstream.Size() + uint64(8*len(c.bytes))
}

func (c *Compressor) append8(x uint64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, x)
	c.bytes = append(c.bytes, b[0])
}

func (c *Compressor) append64(x uint64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, x)
	for i := 7; i >= 0; i-- {
		c.bytes = append(c.bytes, b[i])
	}
}

func zzEncode(i int64) uint64 {
	v := (i >> 63) ^ (i << 1)
	return uint64(v)
}

func (c *Compressor) compressTimestamp(timestamp int64) {
	newDelta := timestamp - c.storedTimestamp
	deltaOfDelta := newDelta - c.storedDelta

	if deltaOfDelta == 0 {
		c.bstream.Add(0, 1)
	} else {
		d := zzEncode(deltaOfDelta)
		length := 64 - bits.LeadingZeros64(d)

		if length >= 1 && length <= 7 {
			//mask7 adds '10' to d
			d |= mask7
			c.bstream.Add(d, 9)
		} else if length >= 8 && length <= 9 {
			//mask9 adds '110' to d
			d |= mask9
			c.bstream.Add(d, 12)
		} else if length >= 10 && length <= 12 {
			//mask12 adds '1110' to d
			d |= mask12
			c.bstream.Add(d, 16)
		} else {
			// Append '1111'
			c.bstream.Add(0xF, 4)
			c.bstream.Add(d, 32)
		}
	}
	c.storedDelta = newDelta
	c.storedTimestamp = timestamp

}

func (c *Compressor) GetByteSize() int {
	return len(c.bytes)
}
func (c *Compressor) compressValues(vals *[]float64) {
	b := make([]byte, 8)

	for i, v := range *vals {
		var val uint64
		if !math.IsNaN(v) {
			val = math.Float64bits(v)
		} else {
			val = NotANum
		}
		yes, offset := c.window[i].Contains(val)
		if val == uint64(math.NaN()) {
			fmt.Println(val)
		}
		if yes {
			// fmt.Println(offset)
			c.append8(offset)
		} else {
			candidate := c.window[i].GetCandidate(val)
			// fmt.Println(candidate)
			xor := candidate ^ val
			leadZeroBytes := bits.LeadingZeros64(xor) / 8
			trailZeroBytes := bits.TrailingZeros64(xor) / 8

			if (leadZeroBytes + trailZeroBytes) > 1 {
				_, offset := c.window[i].Contains(candidate)
				// fmt.Println(offset)
				offset |= 0x80
				c.append8(offset)

				xorByteLength := 8 - leadZeroBytes - trailZeroBytes
				xor >>= (trailZeroBytes * 8)

				head := (trailZeroBytes << 4) | xorByteLength
				c.append8(uint64(head))

				binary.LittleEndian.PutUint64(b, uint64(xor))
				for k := (xorByteLength - 1); k >= 0; k-- {
					c.append8(uint64(b[k]))
				}

			} else {
				c.append8(0xFF)
				c.append64(val)
			}
		}
		c.window[i].Add(val)
	}
}

// ExportData return the bitstream and the raw data of the compressor
func (c *Compressor) ExportData() (*BitStream, *[]byte) {
	return &c.bstream, &c.bytes
}

// switch length {
// case 1:
// case 2:
// case 3:
// case 4:
// case 5:
// case 6:
// case 7:
// 	//mask7 adds '10' to d
// 	d |= mask7
// 	c.bstream.Add(d, 9)
// case 8:
// case 9:
// 	//mask9 adds '110' to d
// 	d |= mask9
// 	c.bstream.Add(d, 12)
// case 10:
// case 11:
// case 12:
// 	//mask12 adds '1110' to d
// 	d |= mask12
// 	c.bstream.Add(d, 16)
// default:
// 	// Append '1111'
// 	c.bstream.Add(0, 4)
// 	c.bstream.Add(d, 32)
// }
