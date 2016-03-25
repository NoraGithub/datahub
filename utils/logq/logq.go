package logq

import (
	"bytes"
	"container/list"
	"io"
	"log"
	"sync"
)

var (
	logList = list.New()
	logbuf  bytes.Buffer
	clogger = new(&logbuf)
	mu      sync.Mutex
)

type Clogger struct {
	l *log.Logger
}

// New creates a new Clogger.   The out variable sets the
// destination to which log data will be written.
// The prefix appears at the beginning of each generated log line.
// The flag argument defines the logging properties.
func new(out io.Writer) *Clogger {
	return &Clogger{l: log.New(out, "", log.LstdFlags)}
}

/*
func (c *Clogger) SetOutput(w io.Writer) {
	c.l.SetOutput(w)
}
*/

func (c *Clogger) Log(v ...interface{}) {
	c.l.Print(v)
}

func LogPutqueue(l string) {
	mu.Lock()
	defer mu.Unlock()

	clogger.Log(l)
	logList.PushBack(logbuf.String())
	logbuf.Reset()
}

func LogGetqueue() (s []string) {
	mu.Lock()
	defer mu.Unlock()

	var next *list.Element
	for e := logList.Front(); e != nil; e = next {
		v := e.Value.(string)
		next = e.Next()
		logList.Remove(e)
		mu.Unlock()
		s = append(s, v)
		mu.Lock()
	}

	return s
}
