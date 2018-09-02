package batchwriter_test

import (
	"github.com/fatlotus/batchwriter"
	"io"
	"io/ioutil"
	"net"
	"testing"
)

func RunBenchmark(b *testing.B, size int64) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	b.SetBytes(size)

	if err != nil {
		b.Fatalf("listen: %s", err)
	}
	go func() {
		writer, err := net.Dial("tcp",
			listener.(*net.TCPListener).Addr().String())
		defer writer.Close()
		if err != nil {
			b.Fatalf("dial: %s", err)
		}
		batch := batchwriter.NewSize(writer, 1000)
		defer batch.Close()
		buf := make([]byte, size)
		for i := 0; i < b.N; i++ {
			_, err := batch.Write(buf)
			if err != nil {
				b.Fatalf("write: %s", err)
			}
		}
	}()
	reader, err := listener.Accept()
	if err != nil {
		b.Fatalf("accept: %s", err)
	}
	defer reader.Close()

	if _, err := io.Copy(ioutil.Discard, reader); err != nil {
		b.Fatalf("readall: %s", err)
	}
}

func BenchmarkSingleByte(b *testing.B) {
	RunBenchmark(b, 1)
}

func Benchmark512Bytes(b *testing.B) {
	RunBenchmark(b, 512)
}

func Benchmark4KBytes(b *testing.B) {
	RunBenchmark(b, 4096)
}

func Benchmark1MBytes(b *testing.B) {
	RunBenchmark(b, 1024*1024)
}
