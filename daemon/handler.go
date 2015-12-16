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
	loginLogged   = false
	loginAuthStr  string
	gstrUsername  string
	DefaultServer = "http://hub.dataos.io/api"
)

type UserForJson struct {
	Username string `json:"username", omitempty`
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	url := DefaultServer + "/" //r.URL.Path
	r.ParseForm()

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
	log.Println("login return", resp.StatusCode)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	} else if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		//fmt.Println("test body", string(body))
		log.Println(string(body))
		type tk struct {
			Token string `json:"token"`
		}
		token := &tk{}
		if err = json.Unmarshal(body, token); err != nil {
			log.Error(err)
			//w.WriteHeader(resp.StatusCode)
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write(body)
			log.Println(resp.StatusCode, string(body))
			return
		} else {
			loginAuthStr = "Token " + token.Token
			loginLogged = true
			log.Println(loginAuthStr)
		}
	}
	w.WriteHeader(resp.StatusCode)
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
