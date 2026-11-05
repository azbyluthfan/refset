package refset

import "container/heap"

// waiter is a read waiting for a version to be applied.
type waiter struct {
	min uint64
	// needSeq, if non-zero, is the first poll sequence number that started
	// after this request arrived. Only a poll with seq >= needSeq can prove
	// the requested version is beyond the source's head.
	needSeq uint64
	ch      chan error // buffered; nil means the version was applied
	index   int        // position in the heap, -1 once removed
}

// waiterHeap is a min-heap of waiters ordered by requested version.
type waiterHeap []*waiter

func (h waiterHeap) Len() int           { return len(h) }
func (h waiterHeap) Less(i, j int) bool { return h[i].min < h[j].min }
func (h waiterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *waiterHeap) Push(x any) {
	w := x.(*waiter)
	w.index = len(*h)
	*h = append(*h, w)
}

func (h *waiterHeap) Pop() any {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	w.index = -1
	*h = old[:n-1]
	return w
}

// releaseApplied wakes every waiter whose version is at or below applied.
func (h *waiterHeap) releaseApplied(applied uint64) {
	for h.Len() > 0 && (*h)[0].min <= applied {
		w := heap.Pop(h).(*waiter)
		w.ch <- nil
	}
}

// releaseAhead fails every waiter that a completed poll proves is asking for a
// version beyond head.
func (h *waiterHeap) releaseAhead(seq, head uint64) {
	var ahead []*waiter
	for _, w := range *h {
		if w.needSeq != 0 && w.needSeq <= seq && w.min > head {
			ahead = append(ahead, w)
		}
	}
	for _, w := range ahead {
		heap.Remove(h, w.index)
		w.ch <- ErrVersionAhead
	}
}

// remove takes w out of the heap. It reports false if w was already released.
func (h *waiterHeap) remove(w *waiter) bool {
	if w.index < 0 {
		return false
	}
	heap.Remove(h, w.index)
	return true
}

// releaseAll fails every waiter with err.
func (h *waiterHeap) releaseAll(err error) {
	for h.Len() > 0 {
		w := heap.Pop(h).(*waiter)
		w.ch <- err
	}
}
