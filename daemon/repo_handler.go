package daemon

import (
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"fmt"
	"strings"
	"encoding/json"
	"github.com/asiainfoLDP/datahub/ds"
	"bufio"
	"os"
	"github.com/asiainfoLDP/datahub/cmd"
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

func repoDelOneItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	repository := ps.ByName("repo")
	dataitem := ps.ByName("item")
	path := "/subscriptions/push/"+repository+"/"+dataitem+"?phase=1"

	reqBody, _ := ioutil.ReadAll(r.Body)

	msg := ds.Response{}
	Pages := ds.ResultPages{}
	msg.Data = &Pages

	respbody, err := commToServer("get", path, reqBody, w)
	if err != nil {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorServiceUnavailable, "commToServer err")
		return
	}
	HttpNoData(w, http.StatusOK, cmd.ResultOK, "OK")

	unmarshalerr := json.Unmarshal(respbody, &msg)
	if unmarshalerr != nil {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "error while unmarshal respBody")
		return
	}

	if msg.Code == 0 && Pages.Total != 0 {
		fmt.Print("DataHub : Order not completed, if deleted,the deposit will return to the subscribers. "+
		"DataItem deleted, and you could not be recovery, and all tags would be deleted either."+
		"Are you sure to delete the current DataItem?[Y or N]:")
		if GetEnsure() == true {
			//fmt.Println("item delete...")
			err := delItem(repository, dataitem)
			if err != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorSqlExec, "error while delete item")
				return
			}
			err = delTagsForDelItem(repository, dataitem)
			if err != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorSqlExec, "error while delete tags")
				return
			}
			respbody, err = commToServer("delete", r.URL.Path, reqBody, w)
			if err != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorServiceUnavailable, "commToServer err")
				return
			}
			HttpNoData(w, http.StatusOK, cmd.ResultOK, "OK")

			unmarshalerr = json.Unmarshal(respbody, &msg)
			if unmarshalerr != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "error while unmarshal respBody")
				return
			}

			if msg.Code != 0 {
				fmt.Println("Error :",msg.Msg)
				rollbackDelItem(repository, dataitem)
				rollbackDelTags(repository, dataitem)
				return
			}
		} else {
			return
		}
	} else if msg.Code == 0 && Pages.Total == 0 {
		fmt.Print("Datahub : After you delete the DataItem, data could not be recovery, and all tags would be deleted either."+
		"Are you sure to delete the current DataItem?[Y or N]:")
		if GetEnsure() == true {
			//fmt.Println("item delete...")
			err := delItem(repository, dataitem)
			if err != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorSqlExec, "error while delete item")
				return
			}
			err = delTagsForDelItem(repository, dataitem)
			if err != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorSqlExec, "error while delete tags")
				return
			}
			respbody, err = commToServer("delete", r.URL.Path, reqBody, w)
			if err != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorServiceUnavailable, "commToServer err")
				return
			}
			HttpNoData(w, http.StatusOK, cmd.ResultOK, "OK")

			unmarshalerr = json.Unmarshal(respbody, &msg)
			if unmarshalerr != nil {
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "error while unmarshal respBody")
				return
			}

			if msg.Code != 0 {
				fmt.Println("Error :",msg.Msg)
				rollbackDelItem(repository, dataitem)
				rollbackDelTags(repository, dataitem)
				return
			}
		} else {
			return
		}
	} else {
		fmt.Println("Error :", msg.Msg)
		return
	}
}

func GetEnsure() bool {
	reader := bufio.NewReader(os.Stdin)
	en, _ := reader.ReadBytes('\n')
	ens := strings.Trim(string(en), "\n")
	ens = strings.ToLower(ens)
	Yes := []string{"y", "yes"}
	for _, y := range Yes {
		if ens == y {
			return true
		}
	}
	return false
}