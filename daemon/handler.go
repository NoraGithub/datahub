package daemon

import (
	"bytes"
	"encoding/json"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"io/ioutil"
	"net/http"
	"strings"
)

var (
	loginLogged       = false
	loginAuthStr      string
	loginBasicAuthStr string
	gstrUsername      string
	DefaultServer     = "https://hub.dataos.io/api"
)

type UserForJson struct {
	Username string `json:"username", omitempty`
}

type tk struct {
	Token string `json:"token"`
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	url := DefaultServer + "/" //r.URL.Path
	//r.ParseForm()

	if _, ok := r.Header["Authorization"]; !ok {

		if !loginLogged {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	userjsonbody, _ := ioutil.ReadAll(r.Body)
	userforjson := UserForJson{}
	if err := json.Unmarshal(userjsonbody, &userforjson); err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	gstrUsername = userforjson.Username
	log.Println("login to", url, "Authorization:", r.Header.Get("Authorization"), gstrUsername)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", r.Header.Get("Authorization"))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	result := &ds.Result{}
	log.Println("login return", resp.StatusCode)
	if resp.StatusCode == http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println(string(body))

		result.Data = &tk{}
		if err = json.Unmarshal(body, result); err != nil {
			log.Error(err)
			w.WriteHeader(http.StatusServiceUnavailable)

			l := log.Println(resp.StatusCode, string(body))
			logq.LogPutqueue(l)
			return
		} else {

			loginAuthStr = "Token " + result.Data.(*tk).Token //must be pointer
			loginLogged = true
			log.Println(loginAuthStr)
			loginBasicAuthStr = r.Header.Get("Authorization")
		}
	} else if resp.StatusCode == http.StatusForbidden {
		body, _ := ioutil.ReadAll(resp.Body)
		l := log.Println(string(body))
		logq.LogPutqueue(l)
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
	} else {
		w.WriteHeader(resp.StatusCode)
	}

}

func commToServer(method, path string, buffer []byte, w http.ResponseWriter) (resp *http.Response, err error) {
	//Trace()
	s := log.Info("daemon: connecting to", DefaultServer+path)
	logq.LogPutqueue(s)
	req, err := http.NewRequest(strings.ToUpper(method), DefaultServer+path, bytes.NewBuffer(buffer))
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	//req.Header.Set("User", "admin")

	if resp, err = http.DefaultClient.Do(req); err != nil {
		log.Error(err)
		d := ds.Result{Code: cmd.ErrorServiceUnavailable, Msg: err.Error()}
		body, e := json.Marshal(d)
		if e != nil {
			log.Error(e)
			return resp, e
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write(body)
		return resp, err
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	w.Write(body)
	log.Info(resp.StatusCode, string(body))
	return
}
