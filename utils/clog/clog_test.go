package clog

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func expect(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Errorf("Expected %v (type %v) - Got %v (type %v)", b, reflect.TypeOf(b), a, reflect.TypeOf(a))
	} else {
		t.Log("Test ok!")
	}
}

func TestLog(t *testing.T) {
	SetLogLevel(LOG_LEVEL_DEBUG)
	logstr := "hello world!!"
	logstr2 := "goodbye world."
	Infof("%s, %s", logstr, logstr2)
	Debugf("%s, %s", logstr, logstr2)
	Errorf("%s, %s", logstr, logstr2)
	Warnf("%s, %s", logstr, logstr2)
	Tracef("%s, %s", logstr, logstr2)
	Printf("%s, %s", logstr, logstr2)
	Info(logstr, logstr2)
	Debug(logstr, logstr2)
	Error(logstr, logstr2)
	Warn(logstr, logstr2)
	Trace(logstr, logstr2)
	Println(logstr, logstr2)
	t.Log("TEST OK")

}

func TestLogLevel(t *testing.T) {
	SetLogLevel(LOG_LEVEL_TRACE)
	lvl := GetLogLevel()
	expect(t, lvl, LOG_LEVEL_TRACE)
}

func TestLogFile(t *testing.T) {
	SetLogFile("/tmp/asdsaaf")
	CloseLogFile()
	var null *os.File = nil
	expect(t, logfileFd, null)
}

func TestLogger(t *testing.T) {

	var buf bytes.Buffer

	s := "hello world!"
	SetOutput(&buf)
	Info(s)
	t.Log("buffer:", &buf)
	buf.Reset()
	Warn(s)
	t.Log("buffer:", &buf)
}

func TestLogLevelEnv(t *testing.T) {
	os.Setenv("DATAHUB_LOGLEVEL", "fatal")
	checkLogEnv()
	lvl := GetLogLevel()
	expect(t, lvl, LOG_LEVEL_FATAL)
}
