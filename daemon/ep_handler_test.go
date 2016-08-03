package daemon

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func Test_epGetHandler(t *testing.T) {
	req, _ := http.NewRequest("GET", "/ep", strings.NewReader(""))
	rw := httptest.NewRecorder()

	epGetHandler(rw, req, nil)
	if !Expect(t, rw, "you don't have any entrypoint.") {
		t.Logf("1.Get ep -------- fail")
	} else {
		t.Logf("1.Get ep -------- success")
	}

	EntryPoint = "http://127.0.0.1:8888"
	EntryPointStatus = "available"
	epGetHandler(rw, req, nil)
	if !Expect(t, rw, "http://127.0.0.1:8888 available") {
		t.Logf("2.Get ep -------- fail")
	} else {
		t.Logf("2.Get ep -------- success")
	}
}

func Test_epPostHandler(t *testing.T) {
	epcon := []Context{
		Context{
			desc:        "1.Post ep",
			requestBody: `{"entrypoint":"http://localhost:8888"}`,
			rw:          httptest.NewRecorder(),
			out:         "OK. your entrypoint is: http://localhost:8888\nThe entrypoint is setted successfully.  You can check whether it is available in one minute.",
		},
		Context{
			desc:        "2.Post ep with err json",
			requestBody: `{"wrong format"}`,
			rw:          httptest.NewRecorder(),
			out:         "",
		},
	}
	for _, v := range epcon {
		req, _ := http.NewRequest("GET", "/ep", strings.NewReader(v.requestBody))
		epPostHandler(v.rw, req, v.ps)
		if !Expect(t, v.rw, v.out) {
			t.Logf("%s fail!", v.desc)
		} else {
			t.Logf("%s success.", v.desc)
		}
	}

}

func Test_epDeleteHandler(t *testing.T) {
	req, _ := http.NewRequest("DELETE", "/ep", strings.NewReader(""))
	rw := httptest.NewRecorder()

	epDeleteHandler(rw, req, nil)
	if !Expect(t, rw, "OK. your entrypoint has been removed") {
		t.Logf("1.Delete ep -------- fail")
	} else {
		t.Logf("1.Delete ep -------- success")
	}
}
