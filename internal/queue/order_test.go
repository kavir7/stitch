package queue

import (
	"testing"
)

// The comparator is the whole ordering design, so it gets tested on its own
// before anything is built on top of it.
func TestLessIsATotalOrder(t *testing.T) {
	cases := []struct {
		name string
		a, b Key
		lifo bool
		want bool
	}{
		{"fifo: earlier sequence wins", Key{0, 1}, Key{0, 2}, false, true},
		{"fifo: later sequence loses", Key{0, 2}, Key{0, 1}, false, false},
		{"lifo: later sequence wins", Key{0, 2}, Key{0, 1}, true, true},
		{"lifo: earlier sequence loses", Key{0, 1}, Key{0, 2}, true, false},
		{"priority outranks sequence in fifo", Key{5, 99}, Key{1, 1}, false, true},
		{"priority outranks sequence in lifo", Key{5, 1}, Key{1, 99}, true, true},
		{"equal priority falls through to sequence", Key{3, 1}, Key{3, 2}, false, true},
		{"lower priority loses regardless of order", Key{1, 1}, Key{2, 99}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := less(tc.a, tc.b, tc.lifo); got != tc.want {
				t.Fatalf("less(%v, %v, lifo=%v) = %v, want %v", tc.a, tc.b, tc.lifo, got, tc.want)
			}
		})
	}
}

func TestLessIsIrreflexiveAndAntisymmetric(t *testing.T) {
	keys := []Key{{0, 1}, {0, 2}, {1, 1}, {1, 2}, {-1, 5}, {3, 3}}
	for _, lifo := range []bool{false, true} {
		for _, a := range keys {
			if less(a, a, lifo) {
				t.Fatalf("less(%v, %v, lifo=%v) is true; a key must not precede itself", a, a, lifo)
			}
			for _, b := range keys {
				if a == b {
					continue
				}
				if less(a, b, lifo) == less(b, a, lifo) {
					t.Fatalf("less(%v,%v)=%v and less(%v,%v)=%v with lifo=%v; the order is not antisymmetric",
						a, b, less(a, b, lifo), b, a, less(b, a, lifo), lifo)
				}
			}
		}
	}
}

// LIFO is FIFO with the sequence comparator inverted, and nothing else. If
// that ever stops being true this test fails.
func TestLIFOIsInvertedFIFO(t *testing.T) {
	keys := []Key{{0, 1}, {0, 2}, {2, 7}, {2, 3}, {5, 100}, {5, 99}}
	for _, a := range keys {
		for _, b := range keys {
			if a.Priority != b.Priority {
				if less(a, b, false) != less(a, b, true) {
					t.Fatalf("keys with different priorities compared differently under lifo: %v vs %v", a, b)
				}
				continue
			}
			if a.Seq == b.Seq {
				continue
			}
			if less(a, b, true) != !less(a, b, false) {
				t.Fatalf("lifo did not invert fifo for %v vs %v", a, b)
			}
		}
	}
}

func TestReadyHeapPopsInComparatorOrder(t *testing.T) {
	for _, lifo := range []bool{false, true} {
		h := &readyHeap{lifo: lifo}
		for _, m := range []*Message{
			{ID: "a", Priority: 0, Seq: 3},
			{ID: "b", Priority: 2, Seq: 5},
			{ID: "c", Priority: 0, Seq: 1},
			{ID: "d", Priority: 2, Seq: 2},
			{ID: "e", Priority: 0, Seq: 2},
		} {
			h.push(m)
		}
		var got []string
		for h.Len() > 0 {
			got = append(got, h.pop().ID)
		}
		want := []string{"d", "b", "c", "e", "a"}
		if lifo {
			want = []string{"b", "d", "a", "e", "c"}
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("lifo=%v pop order %v, want %v", lifo, got, want)
			}
		}
	}
}

func TestDelayedHeapPopsByVisibleAt(t *testing.T) {
	h := &delayedHeap{}
	for _, m := range []*Message{
		{ID: "late", VisibleAt: 300, Seq: 1},
		{ID: "soon", VisibleAt: 100, Seq: 2},
		{ID: "tie-b", VisibleAt: 200, Seq: 9},
		{ID: "tie-a", VisibleAt: 200, Seq: 4},
	} {
		h.push(m)
	}
	var got []string
	for h.Len() > 0 {
		got = append(got, h.pop().ID)
	}
	want := []string{"soon", "tie-a", "tie-b", "late"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delayed pop order %v, want %v", got, want)
		}
	}
}

func TestLeaseHeapPopsByExpiry(t *testing.T) {
	h := &leaseHeap{}
	for _, e := range []leaseEntry{
		{id: "c", expiresAt: 300},
		{id: "a", expiresAt: 100},
		{id: "b", expiresAt: 200},
	} {
		h.push(e)
	}
	if h.peek().id != "a" {
		t.Fatalf("peek returned %q, want the earliest expiry", h.peek().id)
	}
	var got []string
	for h.Len() > 0 {
		got = append(got, h.pop().id)
	}
	if got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("lease pop order %v", got)
	}
}
