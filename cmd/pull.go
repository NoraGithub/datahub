package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func Pull(login bool, args []string) (err error) {
	var repo, item string
	dstruc := ds.DsPull{}
	f := mflag.NewFlagSet("pull", mflag.ContinueOnError)
	f.StringVar(&dstruc.DestName, []string{"-destname", "d"}, "", "Indicates the name that tag will be stored as ")
	pbAutomatic := f.Bool([]string{"-automatic", "a"}, false, "Pull the new tags of a dataitem automatically")
	pbCancelAutomatic := f.Bool([]string{"-cancel", "c"}, false, "Cancel the automatical pulling of a dataitem")

	if len(args) < 2 || (len(args) >= 2 && (args[0][0] == '-' || args[1][0] == '-')) {
		//fmt.Println(ErrMsgArgument)
		pullUsage()
		return
	}
	f.Usage = pullUsage
	if err = f.Parse(args[2:]); err != nil {
		fmt.Println(err)
		return err
	}
	u, err := url.Parse(args[0])
	if err != nil {
		fmt.Println(err)
		return
	}
	dstruc.Automatic = *pbAutomatic
	dstruc.CancelAutomatic = *pbCancelAutomatic

	source := strings.Trim(u.Path, "/")

	if url := strings.Split(source, "/"); len(url) != 2 {
		fmt.Println(ErrMsgArgument)
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
			fmt.Println("Datahub : DATAPOOL://LOCATION are required!")
			pullUsage()
			return
		}
		if len(dstruc.ItemDesc) == 0 {
			dstruc.ItemDesc = repo + "_" + item
		}
	} else {
		fmt.Println("Error : DATAPOOL://LOCATION format error!")
		pullUsage()
		return
	}

	jsonData, err := json.Marshal(dstruc)
	if err != nil {
		fmt.Println("Error")
		return
	}

	resp, err := commToDaemon("post", "/subscriptions/"+repo+"/"+item+"/pull", jsonData)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		//body, _ := ioutil.ReadAll(resp.Body)
		//ShowMsgResp(body, true)
		showResponse(resp)

	} else if resp.StatusCode == http.StatusUnauthorized {
		if err := Login(false, nil); err == nil {
			return Pull(login, args)
		} else {
			fmt.Println(err)
			return err
		}
	} else {
		result := ds.Response{}
		respbody, _ := ioutil.ReadAll(resp.Body)
		//fmt.Println(string(respbody))
		unmarshalerr := json.Unmarshal(respbody, &result)
		if unmarshalerr != nil {
			fmt.Println("Error : Pull error.", unmarshalerr)
			return unmarshalerr
		}
		if result.Code == ServerErrResultCode5009 {
			fmt.Println("DataHub : Failed to get subscription")
		} else if result.Code == ServerErrResultCode5012 {
			fmt.Println("DataHub : Permission denied,you have not subscribed current repo yet.")
		} else if result.Code == ServerErrResultCode5023 {
			fmt.Println("DataHub : Currently the data is unavaliable.")
		} else {
			//showError(resp)
			fmt.Println("Error : ", result.Msg)
		}
	}
	//showError(resp)

	return nil
}

//body, _ := ioutil.ReadAll(resp.Body)
//fmt.Println(body)

//return nil // dl(uri)
//return nil
//}

func pullUsage() {
	fmt.Printf("Usage:\n%s pull [REPO]/[ITEM][:TAG]  DATAPOOL[://LOCATION]  [OPTION]\n", os.Args[0])
	fmt.Println("\nPull a tag from the provider.")
	fmt.Println("\nOption:\n")
	fmt.Println("--destname, -d        Indicate the name that tag will be stored as\n")
	fmt.Printf("%s pull [REPO]/[ITEM] DATAPOOL [OPTION]\n", os.Args[0])
	fmt.Println("\nset or cancel the automatical pulling of tags.")
	fmt.Println("\nOptions:\n")
	fmt.Println("--automatic, -a        Pull the new tags of a dataitem automatically")
	fmt.Println("--cancel, -c           Cancel the automatical pulling of a dataitem")
}
