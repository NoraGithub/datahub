package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
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
	if len(args) > 1 {
		fmt.Println("invalid argument..")
		subsUsage()
		return
	}

	uri := "/subscriptions/dataitems?phase=1"

	if len(args) == 1 {
		if false == strings.Contains(args[0], "/") {
			uri = uri + "&repname=" + args[0]
		} else {
			uri = "/repositories"
			uri = uri + "/" + args[0]
			itemDetail = true
			return Repo(login, args) //deal  repo/item:tag by repo cmd
		}
	}

	resp, err := commToDaemon("GET", uri, nil)
	if err != nil {
		fmt.Println(err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)

		if itemDetail {
			subsResp(itemDetail, body, args[0])
		} else {
			subsResp(itemDetail, body, "")
		}

	} else if resp.StatusCode == 401 {

		if err := Login(false, nil); err == nil {
			Subs(login, args)
		} else {
			//fmt.Println(string(body))
			//fmt.Println(resp.StatusCode ,err)
			fmt.Println(err)
		}
	} else {

		showError(resp)
	}

	return err
}

func subsUsage() {
	fmt.Printf("Usage: %s subs [URL]/[REPO]/[ITEM]\n", os.Args[0])
	fmt.Println("\nList the repositories and dataitems which have been subscribed")
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
		printDash(n + 12)
		for _, tag := range subs.Taglist {
			fmt.Printf("%s:%-8s\t%s\t%s\n", repoitem, tag.Tag, tag.Optime, tag.Comment)
		}
	} else {
		subs := []ds.Data{}
		pages := ds.ResultPages{Results: &subs}
		result := &ds.Result{Data: &pages}
		err := json.Unmarshal(respbody, &result)
		if err != nil {
			panic(err)
		}
		n, _ := fmt.Printf("%s/%-8s\t%s\n", "REPOSITORY", "ITEM", "TYPE")
		printDash(n + 5)
		for _, item := range subs {
			fmt.Printf("%s/%-8s\t%s\n", item.Repository_name, item.Dataitem_name, "file")
		}

	}

}
