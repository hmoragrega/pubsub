package pubsub

import "testing"

func BenchmarkNewID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		NewID()
	}
}
