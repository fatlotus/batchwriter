package batchwriter_test

import (
	"github.com/fatlotus/batchwriter"
	"testing"
	"time"
)

type FastWriter [][]byte

func (t *FastWriter) Write(b []byte) (int, error) {
	*t = append(*t, b)
	return len(b), nil
}

func TestBatchingFastClient(t *testing.T) {
	sink := &FastWriter{}
	b := batchwriter.NewSize(sink, 10)
	for i := 0; i < 9; i++ {
		buf := []byte{byte(i)}
		if !b.WriteAsync(buf) {
			t.Fatalf("buffer was full after %d writes\n", i)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatalf("error: %s\n", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("error: %s\n", err)
	}
	// Make sure we don't spin on bufio.Writer#Flush()
	if b.NumFlushes > 1 {
		t.Fatalf("too many flushes: %d\n", b.NumFlushes)
	}
	if len(*sink) != 1 {
		t.Fatalf("sent too many messages: %d (should only send one)\n",
			len(*sink))
	}
	if string((*sink)[0]) != "\x00\x01\x02\x03\x04\x05\x06\x07\x08" {
		t.Fatalf("sent invalid message: %#v\n", (*sink)[0])
	}
}

type SlowWriter int

func (s SlowWriter) Write(b []byte) (int, error) {
	time.Sleep(100 * time.Millisecond)
	return len(b), nil
}

func TestBatchingSlowClient(t *testing.T) {
	b := batchwriter.NewSize(SlowWriter(0), 10)
	for i := 0; i < 10; i++ {
		if !b.WriteAsync([]byte{byte(i)}) {
			t.Fatalf("buffer was full after %d writes\n", i)
		}
	}
	if b.WriteAsync([]byte{0}) {
		t.Fatalf("expecting backpressure\n")
	}
	for i := 0; i < 10; i++ {
		if _, err := b.Write([]byte{byte(i)}); err != nil {
			t.Fatalf("failed to write: %v\n", err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatalf("error: %s\n", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("error: %s\n", err)
	}
	if b.NumFlushes != 2 {
		t.Fatalf("too many flushes: %d\n", b.NumFlushes)
	}
}
