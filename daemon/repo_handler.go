package daemon

import (
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
)

func repoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(repo)")
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", r.URL.Path+"?size=100", reqBody, w)

	return

}

func repoRepoNameHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(repodetail)")
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", r.URL.Path+"?items=1&size=-1", reqBody, w)

	return
}

func repoItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(repo/item)")
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", r.URL.Path+"?size=100", reqBody, w)

	return
}

func repoTagHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(repo/item/tag)")
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", r.URL.Path, reqBody, w)
	return
}
