package daemon

import (
	"encoding/json"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"strconv"
)

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

	if ensure == 0 {
		path := "/subscriptions/push/" + repository + "/" + dataitem + "?phase=1"

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

func repoDelOneTagHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	if len(loginAuthStr) == 0 {
		HttpNoData(w, http.StatusUnauthorized, cmd.ErrorServiceUnavailable, " ")
		return
	}

	repository := ps.ByName("repo")
	dataitem := ps.ByName("item")
	tag := ps.ByName("tag")

	err := delTag(repository, dataitem, tag)
	if err != nil {
		log.Error(err)
		HttpNoData(w, http.StatusInternalServerError, cmd.ErrorSqlExec, "error while delete tag")
		return
	}

	reqBody, _ := ioutil.ReadAll(r.Body)
	path := "repositories/" + repository + "/" + dataitem + "/" + tag
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
		rollbackDelTag(repository, dataitem, tag)
	} else {
		HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
		log.Info("Error :", result.Msg, "ResultCode:", result.Code, "HttpCode :", resp.StatusCode)
	}

}
