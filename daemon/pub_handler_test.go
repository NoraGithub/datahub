package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var (
	PubDpName = "DatahubUnitTest"
	PubDpType = "file"
	PubDpConn = "/var/lib/datahub/datahubUnitTest"
	PubRepo   = "testpubRepo"
	PubItem   = "testpubItem"
	PubTag    = "testpubTag"
)
var PubData = ds.PubPara{
	Datapool:   PubDpName,
	Detail:     "testpubtagfile.txt",
	Accesstype: "public",
	Comment:    "use for test",
	ItemDesc:   "topub",
}
var (
	PubDir = PubDpConn + "/" + PubData.ItemDesc
	Fname  = PubDir + "/" + PubData.Detail
	FText  = `GOPATH = () # we disallow local import for non-local packages, if $GOROOT happens
            # to be under $GOPATH, then some tests below will fail`
	MetaFile = PubDir + "/Meta.md"
	Mtext    = `###Golang environment variables\n\n-GOPATH\n\n`
)

func Test_pubItemHandler(t *testing.T) {
	//create tag file
	os.MkdirAll(PubDir, 0777)
	f, err := os.OpenFile(Fname, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	if n, e := f.Write([]byte(FText)); e != nil {
		t.Errorf("write file error, %v, size:%v\n", e, n)
	}
	defer f.Close()

	//create meta.md
	fm, err := os.OpenFile(MetaFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	if n, e := fm.Write([]byte(Mtext)); e != nil {
		t.Errorf("write meta file error, %v, size:\n", e, n)
	}
	defer fm.Close()

	server := mockServerFor_Pub()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)
	tmp := DefaultServer
	DefaultServer = server.URL
	defer func() { DefaultServer = tmp }()

	jsondata, _ := json.Marshal(PubData)
	req, _ := http.NewRequest("POST", "/repositories/testpubRepo/testpubItem", bytes.NewBuffer(jsondata))
	w := httptest.NewRecorder()
	pubItemHandler(w, req, httprouter.Params{{PubRepo, PubItem}})
	if w.Code != http.StatusBadRequest {
		t.Errorf("1.pubItemHandler fail-------- %v %v", w.Code, w.Body.String())
	} else {
		t.Log("1.pubItemHandler success--------")
	}

	sqldpinsert := fmt.Sprintf(`insert into DH_DP (DPID, DPNAME, DPTYPE, DPCONN, STATUS)
					values (null, '%s', '%s', '%s', 'A')`, PubDpName, PubDpType, PubDpConn)
	if _, err := g_ds.Insert(sqldpinsert); err != nil {
		os.Remove("/var/lib/datahub/datahubUnitTest")
		t.Error(err)
	}
	fmt.Println(sqldpinsert)

	w2 := httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/repositories/testpubRepo/testpubItem", bytes.NewBuffer(jsondata))
	pubItemHandler(w2, req, httprouter.Params{{"repo", PubRepo}, {"item", PubItem}})
	if w2.Code != http.StatusOK {
		t.Errorf("2.pubItemHandler fail-------- %v %v", w.Code, w2.Body.String())
	} else {
		t.Log("2.pubItemHandler success--------")
	}

}

func Test_pubTagHandler(t *testing.T) {
	server := mockServerFor_Pub()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)
	tmp := DefaultServer
	DefaultServer = server.URL
	defer func() { DefaultServer = tmp }()

	jsondata, _ := json.Marshal(PubData)
	req, _ := http.NewRequest("POST", "/repositories/testpubRepo/testpubItem/testpubTag", bytes.NewBuffer(jsondata))
	w := httptest.NewRecorder()
	pubTagHandler(w, req, httprouter.Params{{"repo", PubRepo}, {"item", PubItem}, {"tag", PubTag}})
	if w.Code != http.StatusOK {
		t.Errorf("1.pubTagHandler fail-------- %v %v", w.Code, w.Body.String())
	} else {
		t.Log("1.pubTagHandler success--------")
	}
}

func Test_recoverdata(t *testing.T) {
	DeleteAllHard(PubDpName, PubRepo, PubItem, PubTag)
	t.Log("recover db")
}

// *********************** Mock Pub Server ********************* //
func mockServerFor_Pub() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" && req.Method != "GET" {
			log.Fatalf("Expecting Request.Method POST, but got %v", req.Method)
		}

		fmt.Fprintln(rsp, `{ 	"code":0,
								"msg":"OK",
								"data":""
							}`)
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}
