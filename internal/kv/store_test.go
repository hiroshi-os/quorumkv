package kv

import "testing"

func TestSetGet(t *testing.T) {
	s := New()
	s.ApplyBytes(EncodeSet("a", "1"))
	s.ApplyBytes([]byte(`{"op":"NOOP"}`))
	v, ok := s.Get("a")
	if !ok || v != "1" {
		t.Fatalf("got %q %v", v, ok)
	}
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss")
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	s.Apply(Command{Op: "SET", Key: "k", Value: "v"})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := s.Get("k"); !ok {
			b.Fatal()
		}
	}
}
