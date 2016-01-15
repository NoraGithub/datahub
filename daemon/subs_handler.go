package daemon

import (
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
