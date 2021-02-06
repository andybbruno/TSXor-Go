package tsxor

import (
	"math/bits"
)

const windowSize = 127

// Window class
type Window struct {
	tmp    []uint64
	buffer []uint64
}

//Init initializes the window
func (w *Window) Init() {
	w.buffer = make([]uint64, windowSize)
	w.tmp = make([]uint64, windowSize)
}

//Add will add 'val' to the window
func (w *Window) Add(val uint64) {
	w.buffer = w.buffer[:windowSize-1]
	w.buffer = append([]uint64{val}, w.buffer...)
}

//Contains : if 'val' is in the window it returns the couple <true,indexOf('val')>
func (w *Window) Contains(val uint64) (bool, uint64) {
	for i, v := range w.buffer {
		if v == val {
			return true, uint64(i)
		}
	}
	return false, 0
}

//GetCandidate returns the element with most bytes in common with 'val'
func (w *Window) GetCandidate(val uint64) uint64 {

	for i := 0; i < windowSize; i++ {
		w.tmp[i] = val ^ w.buffer[i]
	}

	for i := 0; i < windowSize; i++ {
		w.tmp[i] = uint64(bits.LeadingZeros64(w.tmp[i]) + bits.TrailingZeros64(w.tmp[i]))
	}

	max := w.tmp[0]
	maxID := 0
	for i := range w.tmp {
		if w.tmp[i] > max {
			max = w.tmp[i]
			maxID = i
		}
	}

	return w.buffer[maxID]

}

//At returns the element at position 'pos'
func (w *Window) At(pos uint64) uint64 {
	return w.buffer[pos]
}
