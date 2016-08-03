package daemon

import (
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func Test_getAccessToken(t *testing.T) {
	tmp := DefaultServerAPI

	w := httptest.NewRecorder()
	server := mockServerFor_getAccessToken(1, "http://localhost:60000")
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)
	DefaultServerAPI = server.URL
	loginAuthStr = "Token 3281f6af065790adc9e79eec4588d905="
	token, entrypoint, err := getAccessToken("/transaction/rtest/itest/ttest", w)
	if err != nil {
		t.Error("1.getAccessToken fail-------", token, entrypoint, err)
	} else {
		t.Log("1.getAccessToken success-------")
	}

	w2 := httptest.NewRecorder()
	server2 := mockServerFor_getAccessToken(2, "")
	defer server2.Close()
	t.Logf("Started httptest.Server on %v", server2.URL)
	DefaultServerAPI = server2.URL
	token, entrypoint, err = getAccessToken("/transaction/rtest/itest/ttest", w2)
	if token == "" && entrypoint == "" && err != nil {
		t.Log("2.getAccessToken success-------")
	} else {
		t.Error("2.getAccessToken fail-------", token, entrypoint, err)
	}

	DefaultServerAPI = tmp
}

// *********************** Mock transcation Server ********************* //
func mockServerFor_getAccessToken(rcase int, ep string) *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			log.Fatalf("Expecting Request.Method POST, but got %v", req.Method)
		}
		switch rcase {
		case 1:
			fmt.Fprintf(rsp, `{ "code":1,
							"msg":"OK",
							"data":{"accesstoken":"a1a2a3a4a5a6a7a8",
							 		"remainingtime":"72h3m0.5s",
									"entrypoint":"%s"}
						 }`, ep)
		case 2:
			fmt.Fprintf(rsp, `{ "code":1,
							"msg":"OK",
							"data":{" ",
							 		"remainingtime":"72h3m0.5s",
									"entrypoint":"http://www.exaple.com:5678"}
						 }`)
		default:
		}

	}

	return httptest.NewServer(http.HandlerFunc(handler))
}

func Test_putToJobQueue(t *testing.T) {

	jobid := putToJobQueue("tagTest", "/var/lib/datahub/datahub-Unit-Test/testfile.txt", "downloaded", 100)
	t.Log("putToJobQueue job id:", jobid)
}

func Test_download_dl(t *testing.T) {
	w := httptest.NewRecorder()
	server := mockServerFor_download()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)

	pull := "/pull/rtest/itest/tagtest?token=a1a2a3a4a5a6a7a8&username=12345@qq.com"
	url := server.URL + pull
	var c = make(chan int)
	p := ds.DsPull{Datapool: "datapool12345"}

	go func() { <-c }()
	size, err := download(url, p, w, c)
	if err == nil {
		t.Error("1.download fail-------- size:", size)
	} else {
		t.Log("1.download success--------")
	}

	sqldpinsert := fmt.Sprintf(`insert into DH_DP (DPID, DPNAME, DPTYPE, DPCONN, STATUS)
					values (null, '%s', '%s', '%s', 'A')`, Name, Type, Conn)
	if _, err := g_ds.Insert(sqldpinsert); err != nil {
		os.Remove("/var/lib/datahub/datahubUnitTest")
		t.Error(err)
	}

	go func() { <-c }()
	size, err = download(url, testP, w, c)
	if size == 0 && err == nil {
		t.Log("2.download success--------")
	} else {
		t.Error("2.download fail--------- size:", size)
	}

	go func() { <-c }()
	dl(pull, server.URL, testP, w, c)

}

// *********************** Mock download Server ********************* //
func mockServerFor_download() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			log.Fatalf("Expecting Request.Method GET, but got %v", req.Method)
		}
		rsp.WriteHeader(http.StatusBadGateway)
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}

func Test_pullHandler(t *testing.T) {
	tmp := DefaultServer

	serverDown := mockServerFor_download()
	defer serverDown.Close()

	text := `{
			"tag":"tagTest",
    		"destname":"tagTestD.txt",
    		"itemdesc":"dirrepoitemtest",
    		"datapool":"DatahubUnitTest"
    		}`
	req, _ := http.NewRequest("POST", "/subscriptions/repotest/itemtest/pull", strings.NewReader(text))

	w := httptest.NewRecorder()
	server := mockServerFor_getAccessToken(1, serverDown.URL)
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)
	DefaultServer = server.URL
	loginAuthStr = "Token 3281f6af065790adc9e79eec4588d905="

	pullHandler(w, req, httprouter.Params{{"repo", "repotest"}, {"item", "itemtest"}})
	if w.Code == http.StatusBadGateway {
		t.Log("pullHandler success--------")
	}
	DeleteAllHard(Name, testP.Repository, testP.Dataitem, testP.Tag)
	DefaultServer = tmp
}
