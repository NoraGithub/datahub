package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"net/url"
	"os"
	"strings"
)

func Pull(login bool, args []string) (err error) {
	var repo, item string
	dstruc := ds.DsPull{}
	f := mflag.NewFlagSet("pull", mflag.ContinueOnError)
	f.StringVar(&dstruc.DestName, []string{"-destname", "d"}, "", "indicates the name that tag will be stored as ")
	pbAutomatic := f.Bool([]string{"-automatic", "a"}, false, "pull a new tag of a dataitem automatically")

	if len(args) < 2 || (len(args) >= 2 && (args[0][0] == '-' || args[1][0] == '-')) {
		pullUsage()
		return
	}
	f.Usage = pullUsage
	if err = f.Parse(args[2:]); err != nil {
		return err
	}
	u, err := url.Parse(args[0])
	if err != nil {
		fmt.Println(err)
		return
	}
	dstruc.Automatic = *pbAutomatic
	source := strings.Trim(u.Path, "/")

	if url := strings.Split(source, "/"); len(url) != 2 {
		fmt.Println("invalid argument..")
		pullUsage()
		return
	} else {
		target := strings.Split(url[1], ":")
		if len(target) == 1 {
			target = append(target, "latest")
		} else if len(target[1]) == 0 {
			target[1] = "latest"
		}
		//uri = fmt.Sprintf("%s/%s:%s", url[0], target[0], target[1])
		repo = url[0]
		item = target[0]
		dstruc.Tag = target[1]
		if len(dstruc.DestName) == 0 {
			dstruc.DestName = dstruc.Tag
		}
	}

	//get datapool and itemdesc
	if store := strings.Split(strings.Trim(args[1], "/"), "://"); len(store) == 1 {
		dstruc.Datapool = store[0]
		dstruc.ItemDesc = repo + "_" + item
	} else if len(store) == 2 {
		dstruc.Datapool = store[0]
		dstruc.ItemDesc = strings.Trim(store[1], "/")
		if len(dstruc.Datapool) == 0 {
			fmt.Println("DATAPOOL://LOCATION are required!")
			pullUsage()
			return
		}
		if len(dstruc.ItemDesc) == 0 {
			dstruc.ItemDesc = repo + "_" + item
		}
	} else {
		fmt.Println("DATAPOOL://LOCATION format error!")
		pullUsage()
		return
	}

	jsonData, err := json.Marshal(dstruc)
	if err != nil {
		return
	}

	resp, err := commToDaemon("post", "/subscriptions/"+repo+"/"+item+"/pull", jsonData)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		//body, _ := ioutil.ReadAll(resp.Body)
		//ShowMsgResp(body, true)
		showResponse(resp)
		//fmt.Printf("%s/%s:%s will be download to %s\n.", repo, item, ds.Tag, ds.Datapool)

	} else if resp.StatusCode == 401 {
		if err := Login(false, nil); err == nil {
			Pull(login, args)
		} else {
			fmt.Println(err)
			return err
		}
	} else {
		showError(resp)

		return nil
	}
	//body, _ := ioutil.ReadAll(resp.Body)
	//fmt.Println(body)

	return nil // dl(uri)
	//return nil
}

func pullUsage() {
	fmt.Printf("Usage: %s pull [REPO]/[ITEM][:TAG]  DATAPOOL[://LOCATION]  [--destname]\n", os.Args[0])
	fmt.Println("\nPull a tag from the provider\n")
	fmt.Println("  --destname, -d = name  indicates the name that tag will be stored as")
}
