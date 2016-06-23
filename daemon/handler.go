package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"strings"
)

var (
	loginLogged       = false
	loginAuthStr      string
	loginBasicAuthStr string
	gstrUsername      string
	DefaultServer     = "https://hub.dataos.io"
	DefaultServerAPI  = DefaultServer + "/api"
)

type UserForJson struct {
	Username string `json:"username", omitempty`
}

type tk struct {
	Token string `json:"token"`
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	url := DefaultServerAPI + "/" //r.URL.Path
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

func logoutHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("logout.")
	if loginAuthStr == "" {
		w.WriteHeader(http.StatusUnauthorized)
	} else {
		loginAuthStr = ""
		w.WriteHeader(http.StatusOK)
	}
}

func commToServer(method, path string, buffer []byte, w http.ResponseWriter) (body []byte, err error) {
	//Trace()
	s := log.Info("daemon: connecting to", DefaultServer+path)
	logq.LogPutqueue(s)
	req, err := http.NewRequest(strings.ToUpper(method), DefaultServer+path, bytes.NewBuffer(buffer))
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	//req.Header.Set("User", "admin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error(err)
		d := ds.Result{Code: cmd.ErrorServiceUnavailable, Msg: err.Error()}
		body, e := json.Marshal(d)
		if e != nil {
			log.Error(e)
			return body, e
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write(body)
		return body, err
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	body, err = ioutil.ReadAll(resp.Body)
	w.Write(body)
	log.Info(resp.StatusCode, string(body))
	return
}

func commToServerGetRsp(method, path string, buffer []byte) (resp *http.Response, err error) {

	s := log.Info("daemon: connecting to", DefaultServer+path)
	logq.LogPutqueue(s)
	req, err := http.NewRequest(strings.ToUpper(method), DefaultServer+path, bytes.NewBuffer(buffer))
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Error(err)
		return resp, err
	}

	return resp, nil
}

func whoamiHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	code := 0
	msg := "OK"
	httpcode := http.StatusOK
	userstru := &ds.User{}
	if len(loginAuthStr) > 0 {
		userstru.Username = gstrUsername
	} else {
		userstru.Username = ""
		code = cmd.ErrorUnAuthorization
		msg = "Not login."
		httpcode = http.StatusUnauthorized
	}

	b, _ := buildResp(code, msg, userstru)
	w.WriteHeader(httpcode)
	w.Write(b)
}

func itemPulledHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "item pulled or not")
	repo := ps.ByName("repo")
	item := ps.ByName("item")

	itemInfo := ItemInDatapool{}
	itemInfo.Dpname, itemInfo.Dpconn, itemInfo.Dptype, itemInfo.ItemLocation = GetDpnameDpconnItemdesc(repo, item)

	if len(itemInfo.ItemLocation) == 0 {
		JsonResult(w, http.StatusOK, cmd.ErrorItemNotExist, "The DataItem hasn't been pulled.", nil)
	} else {
		JsonResult(w, http.StatusOK, cmd.ResultOK, "The DataItem has been pulled.", &itemInfo)
	}
}

func publishedItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "item published of a datapool")
	datapool := ps.ByName("dpname")
	status := "published"

	repoInfos := make([]ds.RepoInfo, 0)
	repoInfos, err := GetRepoInfo(datapool, status)

	log.Debug(repoInfos)

	if err != nil {
		log.Error(err)
		return
	}

	if len(repoInfos) == 0 {
		msg := fmt.Sprintf("Published DataItem of %s is empty.", datapool)
		JsonResult(w, http.StatusOK, cmd.ErrorPublishedItemEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("All DataItem has been published of %s.", datapool)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, &repoInfos)
	}
}

func pulledItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "item published of a datapool")
	datapool := ps.ByName("dpname")
	status := "pulled"

	repoInfos := make([]ds.RepoInfo, 0)
	repoInfos, err := GetRepoInfo(datapool, status)

	if err != nil {
		log.Error(err)
		return
	}

	if len(repoInfos) == 0 {
		msg := fmt.Sprintf("Pulled DataItem of %s is empty.", datapool)
		JsonResult(w, http.StatusOK, cmd.ErrorPublishedItemEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("All DataItem has been pulled of %s.", datapool)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, &repoInfos)
	}
}

func JsonResult(w http.ResponseWriter, statusCode int, code int, msg string, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	result := ds.Result{Code: code, Msg: msg, Data: data}
	jsondata, err := json.Marshal(&result)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(getJsonBuildingErrorJson()))
	} else {
		w.WriteHeader(statusCode)
		w.Write(jsondata)
	}
}

func getJsonBuildingErrorJson() []byte {

	return []byte(log.Infof(`{"code": %d, "msg": %s}`, cmd.ErrorMarshal, "Json building error"))

}
