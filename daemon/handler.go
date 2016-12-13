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
	"strconv"
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
	ServerUrl string `json:"serverurl", omitempty`
}

type tk struct {
	Token string `json:"token"`
}

func authDaemon(w http.ResponseWriter, r *http.Request) bool {
	log.Println(r.URL, "|", r.RequestURI, "|", r.RemoteAddr, "|", r.URL.RequestURI(), "|", r.Host)
	if r.Host == "127.0.0.1:35600" {
		return true
	}
	auth, ok := r.Header["X-Daemon-Auth"]
	log.Debug("DaemonAuthrization:", DaemonAuthrization)
	if !ok || auth[0] != DaemonAuthrization {
		JsonResult(w, http.StatusUnauthorized, cmd.ErrorUnAuthorization, "", nil)
		log.Error("connect daemon refused!", auth, ok, r.Header)
		return false
	}

	return true
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
	if len(userforjson.ServerUrl) != 0 {
		url = userforjson.ServerUrl + "/api"
	}
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

func publishedOfDatapoolHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "published of a datapool", r)
	r.ParseForm()
	datapool := ps.ByName("dpname")
	status := "published"

	count := getRepoCountByDp(datapool, status)
	offset, limit := optionalOffsetAndSize(r, 10, 1, 100)
	log.Debug("offset, limit", offset, limit)
	validateOffsetAndLimit(count, &offset, &limit)

	repoInfos, err := GetRepoInfo(datapool, status, offset, limit)

	log.Debug(repoInfos, offset, limit)

	if err != nil {
		log.Error(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}

	if len(repoInfos) == 0 {
		msg := fmt.Sprintf("No published dataitem in %s.", datapool)
		JsonResult(w, http.StatusOK, cmd.ErrorPublishedItemEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("Dataitems have been published into %s.", datapool)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, newQueryListResult(count, &repoInfos))
	}
}

func pulledOfDatapoolHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "pulled of a datapool")
	r.ParseForm()
	dpName := ps.ByName("dpname")
	status := "pulled"

	count := getRepoCountByDp(dpName, status)
	offset, limit := optionalOffsetAndSize(r, 10, 1, 100)
	validateOffsetAndLimit(count, &offset, &limit)

	repoInfos, err := GetRepoInfo(dpName, status, offset, limit)

	if err != nil {
		log.Error(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}

	if len(repoInfos) == 0 {
		msg := fmt.Sprintf("No pulled dataitem in %s.", dpName)
		JsonResult(w, http.StatusOK, cmd.ErrorPublishedItemEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("Dataitems have been pulled into %s.", dpName)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, newQueryListResult(count, &repoInfos))
	}
}

func publishedOfRepoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "item published of a repository")
	r.ParseForm()
	dpName := ps.ByName("dpname")
	repoName := ps.ByName("repo")

	isPublished := "Y"
	count := getItemCountByDpRepo(dpName, repoName, isPublished)
	offset, limit := optionalOffsetAndSize(r, 10, 1, 100)
	validateOffsetAndLimit(count, &offset, &limit)

	publishedRepoItems, err := GetPublishedRepoInfo(dpName, repoName, offset, limit)
	if err != nil {
		log.Debug(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}

	if len(publishedRepoItems) == 0 {
		msg := fmt.Sprintf("Pushlied DataItem of %s is empty.", repoName)
		JsonResult(w, http.StatusOK, cmd.ErrorPublishedItemEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("All DataItems have been published of %s.", repoName)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, newQueryListResult(count, publishedRepoItems))
	}
}

func pulledOfRepoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "item pulled of a repository")
	r.ParseForm()
	dpName := ps.ByName("dpname")
	repoName := ps.ByName("repo")

	isPublished := "N"
	count := getItemCountByDpRepo(dpName, repoName, isPublished)
	offset, limit := optionalOffsetAndSize(r, 10, 1, 100)
	validateOffsetAndLimit(count, &offset, &limit)

	pulledRepoItems, err := GetPulledRepoInfo(dpName, repoName, offset, limit)
	if err != nil {
		log.Debug(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}

	if len(pulledRepoItems) == 0 {
		msg := fmt.Sprintf("Pulled DataItem of %s is empty.", repoName)
		JsonResult(w, http.StatusOK, cmd.ErrorPublishedItemEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("All DataItems have been pulled of %s.", repoName)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, newQueryListResult(count, pulledRepoItems))
	}
}

func pulledTagOfItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "tags pulled of dataitem")
	r.ParseForm()

	dpname := ps.ByName("dpname")
	repo := ps.ByName("repo")
	item := ps.ByName("item")

	count, err := getPulledTagCount(dpname, repo, item)
	if err != nil {
		log.Debug(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}
	offset, limit := optionalOffsetAndSize(r, 10, 1, 100)
	log.Debug("offset, limit", offset, limit)
	validateOffsetAndLimit(count, &offset, &limit)

	pulledTagsOfItem, err := GetPulledTagsOfItemInfo(dpname, repo, item, offset, limit)
	if err != nil {
		log.Debug(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}

	if len(pulledTagsOfItem) == 0 {
		msg := fmt.Sprintf("Pulled tags of %s/%s is empty.", repo, item)
		JsonResult(w, http.StatusOK, cmd.ErrorPulledTagEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("All tags have been pulled of %s/%s", repo, item)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, newQueryListResult(count, &pulledTagsOfItem))
	}
}

func publishedTagOfItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Debug(r.URL.Path, "tags published of dataitem")
	r.ParseForm()

	dpname := ps.ByName("dpname")
	repo := ps.ByName("repo")
	item := ps.ByName("item")

	count, err := getPublishedTagCount(dpname, repo, item)
	if err != nil {
		log.Debug(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}
	offset, limit := optionalOffsetAndSize(r, 10, 1, 100)
	log.Debug("offset, limit", offset, limit)
	validateOffsetAndLimit(count, &offset, &limit)

	publishedTagsOfItem, err := GetPublishedTagsOfItemInfo(dpname, repo, item, offset, limit)
	if err != nil {
		log.Debug(err)
		JsonResult(w, http.StatusInternalServerError, cmd.InternalError, err.Error(), nil)
		return
	}

	if len(publishedTagsOfItem) == 0 {
		msg := fmt.Sprintf("Published tags of %s/%s is empty.", repo, item)
		JsonResult(w, http.StatusOK, cmd.ErrorPulledTagEmpty, msg, nil)
	} else {
		msg := fmt.Sprintf("All tags have been published of %s/%s", repo, item)
		JsonResult(w, http.StatusOK, cmd.ResultOK, msg, newQueryListResult(count, &publishedTagsOfItem))
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

type QueryListResult struct {
	Total   int64       `json:"total"`
	Results interface{} `json:"results"`
}

func newQueryListResult(count int64, results interface{}) *QueryListResult {
	return &QueryListResult{Total: count, Results: results}
}

func validateOffsetAndLimit(count int64, offset *int64, limit *int) {
	if *limit < 1 {
		*limit = 1
	}
	if *offset >= count {
		*offset = count - int64(*limit)
	}
	if *offset < 0 {
		*offset = 0
	}
	if *offset+int64(*limit) > count {
		*limit = int(count - *offset)
	}
}

func optionalOffsetAndSize(r *http.Request, defaultSize int64, minSize int64, maxSize int64) (int64, int) {
	size := optionalIntParamInQuery(r, "size", defaultSize)
	if size == -1 {
		return 0, -1
	}
	page := optionalIntParamInQuery(r, "page", 0)
	if page < 1 {
		page = 1
	}
	page -= 1

	if minSize < 1 {
		minSize = 1
	}
	if maxSize < 1 {
		maxSize = 1
	}
	if minSize > maxSize {
		minSize, maxSize = maxSize, minSize
	}

	if size < minSize {
		size = minSize
	} else if size > maxSize {
		size = maxSize
	}

	return page * size, int(size)
}

func optionalIntParamInQuery(r *http.Request, paramName string, defaultInt int64) int64 {
	if r.Form.Get(paramName) == "" {
		log.Debug("paramName nil", paramName, r.Form)
		return defaultInt
	}

	i, err := strconv.ParseInt(r.Form.Get(paramName), 10, 64)
	if err != nil {
		log.Debug("ParseInt", err)
		return defaultInt
	} else {
		return i
	}
}
