package daemon

import (
	"encoding/json"
	"errors"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type ItemInDatapool struct {
	Dpname       string `json:"dpname", omitempty`
	Dptype       string `json:"dptype", omitempty`
	Dpconn       string `json:"dpconn", omitempty`
	ItemLocation string `json:"itemlocation", omitempty`
}

func repoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(repo)")
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", r.URL.Path+"?size=-1", reqBody, w)

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
	commToServer("get", r.URL.Path+"?size=-1", reqBody, w)

	return
}

func repoTagHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(repo/item/tag)")
	reqBody, _ := ioutil.ReadAll(r.Body)
	commToServer("get", r.URL.Path, reqBody, w)
	return
}

func repoDelOneItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	if len(loginAuthStr) == 0 {
		HttpNoData(w, http.StatusUnauthorized, cmd.ErrorServiceUnavailable, " ")
		return
	}
	repository := ps.ByName("repo")
	dataitem := ps.ByName("item")

	r.ParseForm()
	ensure, _ := strconv.Atoi(r.Form.Get("ensure"))

	reqBody, _ := ioutil.ReadAll(r.Body)

	exist, msg, err := judgeRepoOrItemExist(repository, dataitem)
	if err != nil {
		log.Error(err)
		HttpNoData(w, http.StatusInternalServerError, cmd.ErrorServiceUnavailable, err.Error())
		return
	}
	if exist == false {
		HttpNoData(w, http.StatusOK, cmd.RepoOrItemNotExist, msg)
		return
	}

	if ensure == 0 {
		path := "/api/subscriptions/push/" + repository + "/" + dataitem + "?phase=1"

		retResp := ds.Response{}
		Pages := ds.ResultPages{}
		retResp.Data = &Pages

		resp, err := commToServerGetRsp("get", path, reqBody)
		defer resp.Body.Close()
		if err != nil {
			log.Error(err)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorServiceUnavailable, err.Error())
			return
		}
		respbody, _ := ioutil.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusOK {
			unmarshalerr := json.Unmarshal(respbody, &retResp)
			if unmarshalerr != nil {
				log.Error(unmarshalerr)
				HttpNoData(w, http.StatusInternalServerError, cmd.ErrorUnmarshal, "error while unmarshal respBody")
				return
			}
			log.Info(string(respbody))
			if Pages.Total > 0 {

				HttpNoData(w, http.StatusOK, cmd.ExitsConsumingPlan, "Exist consuming subscription plan")
				return
			} else {

				HttpNoData(w, http.StatusOK, cmd.NoConsumingPlan, "No consuming subscription plan")
				return
			}
		} else {
			HttpNoData(w, resp.StatusCode, cmd.ErrorUnknowError, "")
			log.Error(string(respbody))
			return
		}

	} else if ensure == 1 {
		err := delItem(repository, dataitem)
		if err != nil {
			log.Error(err)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorSqlExec, "error while delete item")
			return
		}
		err = delTagsForDelItem(repository, dataitem)
		if err != nil {
			log.Error(err)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorSqlExec, "error while delete tags")
			return
		}
		resp, err := commToServerGetRsp("delete", r.URL.Path, reqBody)
		if err != nil {
			log.Error(err)
			HttpNoData(w, resp.StatusCode, cmd.ErrorServiceUnavailable, "commToServer err")
			return
		}
		defer resp.Body.Close()

		respbody, _ := ioutil.ReadAll(resp.Body)
		retResp := ds.Response{}
		unmarshalerr := json.Unmarshal(respbody, &retResp)
		if unmarshalerr != nil {
			log.Error(unmarshalerr)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorUnmarshal, "error while unmarshal respBody")
			return
		}
		if resp.StatusCode == http.StatusOK {
			HttpNoData(w, http.StatusOK, cmd.ResultOK, retResp.Msg)
			log.Info("Msg :", retResp.Msg, "HttpCode :", resp.StatusCode)
		} else {
			HttpNoData(w, resp.StatusCode, retResp.Code, retResp.Msg)
			log.Error("Error :", retResp.Msg, "HttpCode :", resp.StatusCode)
			rollbackDelItem(repository, dataitem)
			rollbackDelTags(repository, dataitem)
		}
	}

}

func repoDelTagHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	if len(loginAuthStr) == 0 {
		HttpNoData(w, http.StatusUnauthorized, cmd.ErrorServiceUnavailable, " ")
		return
	}

	repository := ps.ByName("repo")
	dataitem := ps.ByName("item")
	tag := ps.ByName("tag")

	reqBody, _ := ioutil.ReadAll(r.Body)

	if strings.Contains(tag, "*") {
		tagsname, err := getBatchDelTagsName(repository, dataitem, tag)
		if err != nil {
			log.Error(err)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorSqlExec, "error while delete tag.")
			return
		}
		if len(tagsname) == 0 {
			log.Println("没有匹配的tag")
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorSqlExec, "No match tag.")
			return
		}
		successflag := true
		for _, tagname := range tagsname {
			if successflag {
				_, err := delTag(repository, dataitem, tagname)
				if err != nil {
					log.Error(err)
					HttpNoData(w, http.StatusInternalServerError, cmd.ErrorSqlExec, "error while delete tag")
					return
				}

				path := "/api/repositories/" + repository + "/" + dataitem + "/" + tagname
				resp, err := commToServerGetRsp("delete", path, reqBody)
				if err != nil {
					log.Error(err)
					HttpNoData(w, resp.StatusCode, cmd.ErrorServiceUnavailable, "commToServer error")
					return
				}
				defer resp.Body.Close()

				result := ds.Response{}

				respbody, err := ioutil.ReadAll(resp.Body)

				unmarshalerr := json.Unmarshal(respbody, &result)
				if unmarshalerr != nil {
					log.Error(unmarshalerr)
					HttpNoData(w, http.StatusInternalServerError, cmd.ErrorUnmarshal, "error while unmarshal respBody")
					return
				}
				if resp.StatusCode == http.StatusOK && result.Code == 0 {
					continue
				} else if resp.StatusCode == http.StatusOK && result.Code != 0 {
					HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
					log.Info("Error :", result.Msg, "ResultCode:", result.Code, "HttpCode :", resp.StatusCode)
					successflag = false
					break
				} else {
					HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
					log.Info("Error :", result.Msg, "ResultCode:", result.Code, "HttpCode :", resp.StatusCode)
					successflag = false
					break
				}
			}

		}
		if successflag {
			log.Info("批量删除tag成功")
			HttpNoData(w, http.StatusOK, cmd.ResultOK, "ok.")
		}
	} else {
		tagid, err := delTag(repository, dataitem, tag)
		if err != nil {
			log.Error(err)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorSqlExec, "error while delete tag")
			return
		}

		reqBody, _ := ioutil.ReadAll(r.Body)
		path := "/api/repositories/" + repository + "/" + dataitem + "/" + tag
		resp, err := commToServerGetRsp("delete", path, reqBody)
		if err != nil {
			log.Error(err)
			HttpNoData(w, resp.StatusCode, cmd.ErrorServiceUnavailable, "commToServer error")
			return
		}
		defer resp.Body.Close()

		result := ds.Response{}

		respbody, err := ioutil.ReadAll(resp.Body)

		unmarshalerr := json.Unmarshal(respbody, &result)
		if unmarshalerr != nil {
			log.Error(unmarshalerr)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorUnmarshal, "error while unmarshal respBody")
			return
		}
		if resp.StatusCode == http.StatusOK && result.Code == 0 {
			HttpNoData(w, http.StatusOK, cmd.ResultOK, result.Msg)
			log.Info("Msg :", result.Msg, "ResultCode:", result.Code, "HttpCode :", resp.StatusCode)
		} else if resp.StatusCode == http.StatusOK && result.Code != 0 {
			HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
			log.Info("Error :", result.Msg, "ResultCode:", result.Code, "HttpCode :", resp.StatusCode)
			rollbackDelTag(tagid)
		} else {
			HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
			log.Info("Error :", result.Msg, "ResultCode:", result.Code, "HttpCode :", resp.StatusCode)
		}
	}
}

func judgeTagExistHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	if len(loginAuthStr) == 0 {
		HttpNoData(w, http.StatusUnauthorized, cmd.ErrorServiceUnavailable, " ")
		return
	}

	repository := ps.ByName("repo")
	dataitem := ps.ByName("item")
	tag := ps.ByName("tag")

	if strings.ContainsAny(tag, "*") {
		exist, msg, err := judgeRepoOrItemExist(repository, dataitem)
		if err != nil {
			log.Error(err)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorServiceUnavailable, err.Error())
			return
		}
		if exist == false {
			HttpNoData(w, http.StatusBadRequest, cmd.RepoOrItemNotExist, msg)
			return
		} else {
			HttpNoData(w, http.StatusOK, cmd.TagExist, msg)
			return
		}
	} else {
		exist, msg, err := judgeRepoOrItemExist(repository, dataitem)
		if err != nil {
			log.Error(err)
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorServiceUnavailable, err.Error())
			return
		}
		if exist == false {
			HttpNoData(w, http.StatusBadRequest, cmd.RepoOrItemNotExist, msg)
			return
		} else {
			exist, msg, err = judgeTagExist(repository, dataitem, tag)
			if err != nil {
				log.Error(err)
				HttpNoData(w, http.StatusInternalServerError, cmd.ErrorServiceUnavailable, err.Error())
				return
			}
			if exist == false {
				HttpNoData(w, http.StatusBadRequest, cmd.TagNotExist, msg)
				return
			} else {
				HttpNoData(w, http.StatusOK, cmd.TagExist, msg)
				return
			}
		}
	}

	return
}

func judgeRepoOrItemExist(repository, dataitem string) (exist bool, msg string, err error) {

	path := "/api/repositories/" + repository + "/" + dataitem

	exist = false

	resp, err := commToServerGetRsp("get", path, nil)
	if err != nil {
		log.Error(err)
		//HttpNoData(w, http.StatusInternalServerError, cmd.ErrorServiceUnavailable, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		err = errors.New("unkown error")
		return
	}
	result := ds.Response{}
	respbody, _ := ioutil.ReadAll(resp.Body)
	unmarshalerr := json.Unmarshal(respbody, &result)
	if unmarshalerr != nil {
		log.Error(unmarshalerr)
		//HttpNoData(w, http.StatusInternalServerError, cmd.ErrorUnmarshal, "error while unmarshal respBody")
		return
	}
	log.Info(string(respbody))

	if resp.StatusCode == http.StatusBadRequest && result.Code == cmd.ServerErrResultCode1009 {
		//HttpNoData(w, http.StatusBadRequest, cmd.RepoOrItemNotExist, result.Msg)
		msg = result.Msg
		return
	} else if resp.StatusCode == http.StatusOK && result.Code == cmd.ServerErrResultCodeOk {
		exist = true
		msg = result.Msg
		return
	}

	return
}

func judgeTagExist(repository, dataitem, tag string) (exist bool, msg string, err error) {

	path := "/api/repositories/" + repository + "/" + dataitem + "/" + tag

	exist = false

	resp, err := commToServerGetRsp("get", path, nil)
	defer resp.Body.Close()
	if err != nil {
		log.Error(err)
		return
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		err = errors.New("unkown error")
		return
	}

	result := ds.Response{}
	respbody, _ := ioutil.ReadAll(resp.Body)
	unmarshalerr := json.Unmarshal(respbody, &result)
	if unmarshalerr != nil {
		log.Error(unmarshalerr)
		return
	}
	log.Info(string(respbody))

	if resp.StatusCode == http.StatusBadRequest && result.Code == cmd.ServerErrResultCode1009 {
		msg = result.Msg
		return
	} else if resp.StatusCode == http.StatusOK && result.Code == cmd.ServerErrResultCodeOk {
		exist = true
		msg = result.Msg
		return
	}

	return
}
