package batchwriter

import (
	"bufio"
	"io"
)

type Writer struct {
	Err        error
	NumFlushes int

	messages chan []byte
	sub      io.Writer
	writer   *bufio.Writer
	done     chan bool
}

func New(w io.Writer) *Writer {
	return NewSize(w, 10)
}

func NewSize(sub io.Writer, size int) *Writer {
	w := &Writer{
		messages: make(chan []byte, size),
		sub:      sub,
		writer:   bufio.NewWriter(sub),
		done:     make(chan bool),
	}
	go w.Flusher()
	return w
}

func (w *Writer) Flusher() {
	defer close(w.done)
	dirty := false
	for w.Err == nil {
		if dirty {
			select {
			case msg := <-w.messages:
				if msg == nil {
					w.NumFlushes += 1
					w.Err = w.writer.Flush()
					return
				}
				_, w.Err = w.writer.Write(msg)
			default:
				w.NumFlushes += 1
				w.Err = w.writer.Flush()
				dirty = false
			}
		} else {
			msg := <-w.messages
			if msg == nil {
				w.NumFlushes += 1
				w.Err = w.writer.Flush()
				return
			}
			_, w.Err = w.writer.Write(msg)
			dirty = true
		}
	}
}

// Blocking interface for io.Writer compatibility.
func (w *Writer) Write(buf []byte) (int, error) {
	w.messages <- buf
	return len(buf), w.Err
}

// Asynchronous interface.
//
// If this function returns false, it indicates that the send buffer was full
// and we were about to block.
func (w *Writer) WriteAsync(buf []byte) bool {
	select {
	case w.messages <- buf:
		return true
	default:
		return false
	}
}

// Shuts down the batched writer.
func (w *Writer) Close() error {
	select {
	case w.messages <- []byte(nil):
		<-w.done
	case <-w.done:
	}
	return w.Err
}
