package daemon

import (
	"fmt"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func init() {
	DefaultServer = "http://localhost:35888"
	//go testserver()
}

func testserver() {
	var tsl = new(StoppabletcpListener)

	Listener, err := net.Listen("tcp", ":35888")
	if err != nil {
		log.Fatal(err)
	}

	tsl, err = tcpNew(Listener)
	if err != nil {
		log.Fatal(err)
	}

	tRouter := httprouter.New()
	tRouter.GET("/", sayhello)

	http.Handle("/", tRouter)

	server := http.Server{Handler: tRouter}

	log.Info("p2p server start")
	server.Serve(tsl)
	log.Info("p2p server stop")
}

func Test_commToServer(t *testing.T) {
	w := httptest.NewRecorder()
	server := mockServerFor_commToServer()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)
	//url, _ := url.Parse(server.URL)
	tmp := DefaultServer
	DefaultServer = server.URL
	defer func() { DefaultServer = tmp }()
	_, err := commToServer("get", "/", nil, w)
	if err != nil {
		t.Errorf("1.commToServer fail-------", err)
	}

	DefaultServer = "111111"
	_, err = commToServer("get", "/", nil, w)
	fmt.Println("err:", err)
	if err == nil {
		t.Error("2.commToServer with err server fail-------")
	}
}

// *********************** Mock commToServer ********************* //
func mockServerFor_commToServer() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			log.Fatalf("Expecting Request.Method GET, but got %v", req.Method)
		}

		fmt.Fprintf(rsp, `Test_commToServer response test`)
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}

func Test_loginHandler(t *testing.T) {
	server := mockServerFor_loginNgix()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)
	//url, _ := url.Parse(server.URL)
	tmp := DefaultServer
	DefaultServer = server.URL
	defer func() { DefaultServer = tmp }()

	req, _ := http.NewRequest("GET", "/", strings.NewReader(`{"username":"yuanwm@asiainfo.com"}`))
	req.Header.Set("Authorization", "Basic eXVhbndtQGFzaWFpbmZvLmNvbToxMTQ0NmZjM2ZjMTBhMjdjMTJiZjM1NjI3MmQ4OTg0OAo=")
	w := httptest.NewRecorder()
	loginHandler(w, req)
	fmt.Println("gstrUsername", gstrUsername)
	if !loginLogged {
		t.Error("Login error.")
	}
}

// *********************** Mock ngix login ********************* //
func mockServerFor_loginNgix() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			log.Fatalf("Expecting Request.Method GET, but got %v", req.Method)
		}

		fmt.Fprintf(rsp, `{"token":"3281f6af065790adc9e79eec4588d905="}`)
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}
