package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"io/ioutil"
	"net/http"
	"strings"
)

func ItemOrTagRm(needLogin bool, args []string) error {

	if len(args) > 1 {
		fmt.Println("DataHub : Invalid argument.")
		itemrmUsage()
		return nil
	}

	arg := args[0]
	var repository string
	var dataitem string
	//var tag string

	splitStr := strings.Split(arg, "/")
	if len(splitStr) == 2 {
		repository = splitStr[0]
		dataitem = splitStr[1]
		uri := "/repositories/" + repository + "/" + dataitem
		resp, err := commToDaemon("DELETE", uri, nil)
		if err != nil {
			fmt.Println(err)
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			result := ds.Result{}
			if err := json.Unmarshal(body, &result); err != nil {
				fmt.Println(err)
				return err
			}

			ensureRm(result.Code, uri)

		} else if resp.StatusCode == http.StatusUnauthorized {
			if err = Login(false, nil); err == nil {
				err = ItemOrTagRm(needLogin, args)
			} else {
				fmt.Println(err)
			}
		} else {
			showError(resp)
		}

		return err
	} else if len(splitStr) == 3 {
		return nil

	} else {
		fmt.Println("invalid argument..")
		tagrmUsage()
		return nil
	}
	return nil
}

func ensureRm(code int, uri string) {
	uri += "?ensure=1"
	if code == ExitsConsumingPlan {
		fmt.Print("DataHub : Order not completed, if deleted,the deposit will return to the subscribers. " +
			"DataItem deleted, and you could not be recovery, and all tags would be deleted either." +
			"Are you sure to delete the current DataItem?[Y or N]:")
	} else if code == NoConsumingPlan {
		fmt.Print("Datahub : After you delete the DataItem, data could not be recovery, and all tags would be deleted either." +
			"Are you sure to delete the current DataItem?[Y or N]:")
	}
	if GetEnsure() == true {
		resp, err := commToDaemon("DELETE", uri, nil)
		if err != nil {
			fmt.Println(err)

		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {

			showResponse(resp)

		} else {
			showError(resp)

		}
	} else {

	}

}

func tagrmUsage() {
	fmt.Printf("Usage: datahub repo rm [REPO]/[ITEM]:[tag]\n")
	fmt.Println("Remove a tag")
}

func itemrmUsage() {
	fmt.Printf("Usage: datahub repo rm [REPO]/[ITEM]\n")
	fmt.Println("Remove a item")
}
