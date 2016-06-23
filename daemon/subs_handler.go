package daemon

import (
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
)

func subsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.RequestURI(), "(subs)")
	reqBody, _ := ioutil.ReadAll(r.Body)

	commToServer("get", r.URL.RequestURI(), reqBody, w)

	return
}

func userStatusHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(heartbeat/status)")

	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", r.URL.Path, reqBody, w)

	return
}

func tagStatusHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if len(loginAuthStr) == 0 {
		HttpNoData(w, http.StatusUnauthorized, cmd.ErrorServiceUnavailable, " ")
		return
	}
	repository := ps.ByName("repo")
	dataitem := ps.ByName("item")
	tag := ps.ByName("tag")
	uri := fmt.Sprintf("/api/daemon/tags/status?repname=%s&itemname=%s&tagname=%s", repository, dataitem, tag)
	log.Println(uri)
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", uri, reqBody, w)
	return
}

func tagOfItemStatusHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if len(loginAuthStr) == 0 {
		HttpNoData(w, http.StatusUnauthorized, cmd.ErrorServiceUnavailable, " ")
		return
	}
	repository := ps.ByName("repo")
	dataitem := ps.ByName("item")
	uri := fmt.Sprintf("/api/daemon/tags/status?repname=%s&itemname=%s", repository, dataitem)
	log.Println(uri)
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", uri, reqBody, w)
	return
}
