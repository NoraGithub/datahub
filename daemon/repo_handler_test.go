package daemon

import (
	"net/http/httptest"
	"net/http"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"fmt"
	"testing"
	"bytes"
	"encoding/json"
	"github.com/julienschmidt/httprouter"
)

func Test_repoDelOneItemHandler(t *testing.T) {
	server := mockServerFor_DelItem()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)
	tmp := DefaultServer
	DefaultServer = server.URL
	defer func() { DefaultServer = tmp }()

	jsondata, _ := json.Marshal(PubData)
	req, _ := http.NewRequest("GET", "/subscriptions/push/testpubRepo/testpubItem?phase=1", bytes.NewBuffer(jsondata))
	w := httptest.NewRecorder()
	repoDelOneItemHandler(w, req, httprouter.Params{{"repo", PubRepo}, {"item", PubItem}})
	if w.Code != http.StatusOK {
		t.Errorf("1.pubTagHandler fail-------- %v %v", w.Code, w.Body.String())
	} else {
		t.Log("1.pubTagHandler success--------")
	}
}

func mockServerFor_DelItem() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			log.Fatalf("Expecting Request.Method GET, but got %v", req.Method)
		}

		fmt.Fprintln(rsp, `{ 	"code":0,
					"msg":"OK",
					"data":""
				   }`)
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}