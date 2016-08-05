package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

func Subs(login bool, args []string) (err error) {
	f := mflag.NewFlagSet("subs", mflag.ContinueOnError)
	f.Usage = subsUsage
	if err = f.Parse(args); err != nil {
		return err
	}
	itemDetail := false

	var uri string

	if len(args) == 0 {
		uri = "/subscriptions/dataitems?phase=1"
		err = cmdSubsRepo(itemDetail, uri, args)
	} else if len(args) == 1 {
		if false == strings.Contains(args[0], "/") {
			uri = "/subscriptions/dataitems?phase=1&repname=" + args[0]
			err = cmdSubsRepo(itemDetail, uri, args)
		} else {
			repo, item, err := GetRepoItem(args[0])
			if err != nil {
				fmt.Println(ErrMsgArgument)
				subsUsage()
				return err
			}
			//fmt.Println(repo, item)

			sub, err := itemSubsOrNot(repo, item)
			if sub == true && err == nil {

				uri = "/repositories/" + args[0]
				itemDetail = true
				return Repo(login, args) //deal  repo/item:tag by repo cmd

			}
		}
	} else {
		fmt.Println(ErrMsgArgument)
		subsUsage()
		return
	}

	return err
}

func cmdSubsRepo(detail bool, uri string, args []string) error {
	resp, err := commToDaemon("GET", uri, nil)
	if err != nil {
		fmt.Println("DataHub", err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)

		if detail {
			subsResp(detail, body, args[0])
		} else {
			subsResp(detail, body, "")
		}

	} else if resp.StatusCode == 401 {

		if err := Login(false, nil); err == nil {
			Subs(true, args)
		} else {
			//fmt.Println(err)
		}
	} else {
		showError(resp)
	}
	return err
}

func GetRepoItem(arg string) (repo, item string, err error) {
	s := strings.Trim(arg, "/")

	if split := strings.Split(s, "/"); len(split) != 2 {
		err = errors.New("dataitem not found")
		return
	} else {
		repo = split[0]
		split2 := strings.Split(split[1], ":")
		item = split2[0]

		if len(repo) == 0 {
			err = errors.New("Lenth of repository is 0")
			return
		} else if len(item) == 0 {
			err = errors.New("Lenth of dataitem is 0")
			return
		}
	}
	return repo, item, nil
}

func itemSubsOrNot(repo, item string) (sub bool, err error) {
	uri := fmt.Sprintf("/subscriptions/pull/%s/%s?phase=1&size=1", repo, item)
	resp, err := commToDaemon("GET", uri, nil)
	if err != nil {
		fmt.Println(err)
		return false, err
	}

	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		pages := &ds.ResultPages{}
		result := &ds.Result{Data: pages}
		err := json.Unmarshal(body, &result)
		if err != nil {
			panic(err)
		}
		//fmt.Println("DataHub : Total price plans:", pages.Total)
		if pages.Total > 0 {
			return true, nil
		} else {
			fmt.Println("DataHub : You have not subscribed current dataitem yet.")
			return false, nil
		}
	} else if resp.StatusCode == 401 {
		if err = Login(false, nil); err == nil {
			return itemSubsOrNot(repo, item)
		} else {
			//fmt.Println(err)
		}
	} else {
		showError(resp)
	}
	return false, err
}

func subsResp(detail bool, respbody []byte, repoitem string) {

	if detail {
		subs := ds.Data{}
		result := &ds.Result{Data: &subs}
		err := json.Unmarshal(respbody, &result)
		if err != nil {
			panic(err)
		}
		n, _ := fmt.Printf("%s\t%s\t%s\n", "REPOSITORY/ITEM[:TAG]", "UPDATETIME", "COMMENT")

		printDash(n + 20)
		for _, tag := range subs.Taglist {
			fmt.Printf("%s:%-8s\t%s\t%s\n", repoitem, tag.Tag, tag.Optime, tag.Comment)
		}
	} else {
		var itemStatus string
		subs := []ds.Data{}
		pages := ds.ResultPages{Results: &subs}
		result := &ds.Result{Data: &pages}
		err := json.Unmarshal(respbody, &result)
		if err != nil {
			panic(err)
		}

		citem := []string{"REPOSITORY/ITEM"}
		ctype := []string{"TYPE"}
		cstatus := []string{"STATUS"}
		for _, item := range subs {
			//crepo = append(crepo, item.Repository_name)
			citem = append(citem, item.Repository_name+"/"+item.Dataitem_name)
			ctype = append(ctype, "file")
			itemStatus, err = getItemStatus(item.Repository_name, item.Dataitem_name)
			if err != nil {
				//fmt.Println("Error :", err)
				cstatus = append(cstatus, "")
			}
			cstatus = append(cstatus, itemStatus)
			//fmt.Printf("%s/%-8s\t%s\t%s\n", item.Repository_name, item.Dataitem_name, "file", itemStatus)
		}
		utils.PrintFmt(citem, ctype, cstatus)
	}

}

func getItemStatus(reponame, itemname string) (string, error) {
	uri := "/repositories/" + reponame + "/" + itemname
	resp, err := commToDaemon("get", uri, nil)
	if err != nil {
		return "", err
	}
	result := ds.Result{}
	itemInfo := ds.ItemInfo{}
	result.Data = &itemInfo
	respbody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode == http.StatusOK {
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			return "", err
		}
	} else {
		return "", errors.New(string(respbody))
		//showResponse(resp)
	}

	uri = "/heartbeat/status/" + itemInfo.Create_user
	resp, err = commToDaemon("get", uri, nil)
	if err != nil {
		return "", err
	}
	itemStatus := ds.ItemStatus{}
	result.Data = &itemStatus
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode == http.StatusOK {
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			return "", err
		}
	} else {
		showResponse(resp)
	}

	return itemStatus.Status, nil
}

func subsUsage() {
	fmt.Printf("Usage:\n%s subs\n", os.Args[0])
	fmt.Println("\nList the repositories and dataitems which have been subscribed.")
	fmt.Printf("\n%s subs [REPO]\n", os.Args[0])
	fmt.Println("\nList the dataitems which have been subscribed of the repository.")
	fmt.Printf("\n%s subs [REPO]/[ITEM]\n", os.Args[0])
	fmt.Println("\nList the detail of the dataitem which have been subscribed.")
}
