package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
	"strings"
)

func ItemOrTagRm(needLogin bool, args []string) error {
	f := mflag.NewFlagSet("repo rm", mflag.ContinueOnError)
	f.Usage = itemortagrmUsage
	if err := f.Parse(args); err != nil {
		return err
	}
	if len(args) == 0 {
		itemortagrmUsage()
		return nil
	} else if len(args) > 1 {
		fmt.Println(ErrMsgArgument)
		itemortagrmUsage()
		return errors.New(ErrMsgArgument)
	}

	arg := args[0]
	var repository string
	var dataitem string
	var tag string

	splitStr := strings.Split(arg, ":")
	if len(splitStr) == 1 {
		splitStr2 := strings.Split(splitStr[0], "/")
		if len(splitStr2) != 2 {
			fmt.Println(ErrMsgArgument)
			return errors.New(ErrMsgArgument)
		}
		repository = splitStr2[0]
		dataitem = splitStr2[1]
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
	} else if len(splitStr) == 2 {
		fmt.Print("DataHub : After you delete the Tag, data could not be recovery.\n" +
			"Are you sure to delete the current Tag?[Y or N]:")
		if GetEnsure() == true {
			splitStr2 := strings.Split(splitStr[0], "/")
			repository = splitStr2[0]
			dataitem = splitStr2[1]
			tag = splitStr[1]
			uri := "/repositories/" + repository + "/" + dataitem + "/" + tag
			resp, err := commToDaemon("DELETE", uri, nil)
			if err != nil {
				fmt.Println(err)
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				showResponse(resp)

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
		}
	} else {
		fmt.Println(ErrMsgArgument)
		itemortagrmUsage()
		return errors.New(ErrMsgArgument)
	}
	return nil
}

func ensureRm(code int, uri string) {
	uri += "?ensure=1"
	if code == ExitsConsumingPlan {
		fmt.Print("DataHub : Order not completed, if deleted, the deposit will return to the subscribers.\n" +
			"DataItem deleted, and you could not be recovery, and all tags would be deleted either.\n" +
			"Are you sure to delete the current DataItem?[Y or N]:")
	} else if code == NoConsumingPlan {
		fmt.Print("Datahub : After you delete the DataItem, data could not be recovery, and all tags would be deleted either.\n" +
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
		return
	}

}

func itemortagrmUsage() {
	fmt.Println("Usage: datahub repo rm [REPO]/[ITEM]")
	fmt.Println("Remove a item.\n")

	fmt.Println("Usage: datahub repo rm [REPO]/[ITEM]:[tag]")
	fmt.Println("Remove a tag.")
}
