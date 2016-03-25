package logq

import (
	"strings"
	"testing"
)

var s = []string{
	"1111111111",
	"2222222222",
	"3333333333",
	"4444444444",
	"5555555555",
	"6666666666",
	"7777777777",
}

func TestLogq(t *testing.T) {

	LogPutqueue(s[0])
	LogPutqueue(s[1])
	LogPutqueue(s[2])

	a := LogGetqueue()
	if len(a) != 3 {
		t.Fatal("len(a) is not equal", len(a), "and we expect 3 returned.")
	} else {
		t.Log("sizeof log buf:", len(a))
	}

	for i := 0; i < len(a); i++ {
		if ok := strings.Contains(a[i], s[i]); !ok {
			t.Fatal(a[i], "doesn't contain", s[i])
		} else {
			t.Log(a[i], "contains", s[i])
		}
	}

	LogPutqueue(s[3])
	LogPutqueue(s[4])
	LogPutqueue(s[5])
	LogPutqueue(s[6])

	a = LogGetqueue()
	if len(a) != 4 {
		t.Fatal("len(a) is not equal", len(a), "and we expect 4 returned.")
	} else {
		t.Log("sizeof log buf:", len(a))
	}

	for i := 0; i < len(a); i++ {
		if ok := strings.Contains(a[i], s[i+3]); !ok {
			t.Fatal(a[i], "doesn't contain", s[i+3])
		} else {
			t.Log(a[i], "contains", s[i+3])
		}
	}

	t.Log("test....ok")

}
