package kafkawire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// xorshift is a tiny deterministic generator for tests.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := uint64(*x)
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = xorshift(v)
	return v
}
func (x *xorshift) intn(n int) int { return int(x.next() % uint64(n)) }
func (x *xorshift) bytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(x.next())
	}
	return b
}

func frameOf(p []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(p))), p...)
}

func TestFramerSplits(t *testing.T) {
	rng := xorshift(0x9E3779B97F4A7C15)
	for iter := range 200 {
		var payloads [][]byte
		var stream []byte
		for i, n := 0, 1+rng.intn(6); i < n; i++ {
			p := rng.bytes(1 + rng.intn(40))
			payloads = append(payloads, p)
			stream = append(stream, frameOf(p)...)
		}
		f := NewFramer(1 << 10)
		var got [][]byte
		for rest := stream; len(rest) > 0; {
			n := 1 + rng.intn(9)
			if iter%4 == 0 {
				n = 1
			}
			n = min(n, len(rest))
			fr, err := f.Push(rest[:n])
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fr...)
			rest = rest[n:]
		}
		if len(got) != len(payloads) || f.Buffered() != 0 {
			t.Fatalf("iter %d: %d frames, buffered %d", iter, len(got), f.Buffered())
		}
		for i := range got {
			if !bytes.Equal(got[i], payloads[i]) {
				t.Fatalf("iter %d frame %d differs", iter, i)
			}
		}
	}
}

func TestFramerErrors(t *testing.T) {
	for name, hdr := range map[string][]byte{
		"zero":     {0, 0, 0, 0},
		"negative": {0xff, 0xff, 0xff, 0xfe},
		"oversize": {0, 0, 0, 11},
	} {
		f := NewFramer(10)
		if _, err := f.Push(hdr); !errors.Is(err, ErrBadFrame) {
			t.Errorf("%s: %v", name, err)
		}
		if _, err := f.Push([]byte{1}); !errors.Is(err, ErrBadFrame) {
			t.Errorf("%s: error not sticky: %v", name, err)
		}
	}
	f := NewFramer(10)
	fr, err := f.Push(append(frameOf([]byte("ok")), 0, 0, 0, 0))
	if err == nil || len(fr) != 1 {
		t.Fatalf("frames before error must be returned: %v %v", fr, err)
	}
}

func TestFramerNoAliasing(t *testing.T) {
	f := NewFramer(100)
	in := append(frameOf([]byte("aaaa")), frameOf([]byte("bbbb"))...)
	fr, _ := f.Push(in)
	if len(fr) != 2 {
		t.Fatal(len(fr))
	}
	for i := range in {
		in[i] = 'Z'
	}
	fr[0][0] = 'Q'
	if string(fr[1]) != "bbbb" || string(fr[0]) != "Qaaa" {
		t.Fatalf("aliasing: %q %q", fr[0], fr[1])
	}
	more, _ := f.Push(frameOf([]byte("cccc")))
	if string(fr[1]) != "bbbb" || string(more[0]) != "cccc" {
		t.Fatal("later push mutated earlier frames")
	}
}

func FuzzFramer(f *testing.F) {
	f.Add([]byte{0, 0, 0, 2, 1, 2, 0, 0}, uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, step uint8) {
		fr := NewFramer(64)
		s := max(int(step), 1)
		for len(data) > 0 {
			n := min(s, len(data))
			frames, err := fr.Push(data[:n])
			for _, x := range frames {
				if len(x) == 0 || len(x) > 64 {
					t.Fatal("bad frame size")
				}
			}
			if err != nil {
				return
			}
			data = data[n:]
		}
	})
}
