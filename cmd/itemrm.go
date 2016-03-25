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
		fmt.Println(ValidateErrMsgArgument)
		itemortagrmUsage()
		return errors.New(ValidateErrMsgArgument)
	}

	arg := args[0]

	if validateArgs(arg) == false {
		fmt.Println(ValidateErrMsgArgument)
		itemortagrmUsage()
		return errors.New(ValidateErrMsgArgument)
	}

	var repository string
	var dataitem string
	var tag string

	splitStr := strings.Split(arg, ":")
	if len(splitStr) == 1 {
		splitStr2 := strings.Split(splitStr[0], "/")
		if len(splitStr2) != 2 {
			fmt.Println(ValidateErrMsgArgument)
			itemortagrmUsage()
			return errors.New(ValidateErrMsgArgument)
		}
		repository = splitStr2[0]
		dataitem = splitStr2[1]
		uri := "/repositories/" + repository + "/" + dataitem
		resp, err := commToDaemon("DELETE", uri, nil)
		if err != nil {
			fmt.Println("Error :",err)
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

			ensureRmItem(result.Code, uri, result.Msg)

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
		if splitStr[1] == "" {
			fmt.Println(ValidateErrMsgArgument)
			itemortagrmUsage()
			return errors.New(ValidateErrMsgArgument)
		}

		splitStr2 := strings.Split(splitStr[0], "/")
		repository = splitStr2[0]
		dataitem = splitStr2[1]
		tag = splitStr[1]

		uri := "/repositories/" + repository + "/" + dataitem + "/" + tag + "/judge"

		resp, err := commToDaemon("GET", uri, nil)
		if err != nil {
			fmt.Println("Error :", err)
			return err
		}
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		result := ds.Result{}

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusBadRequest {
			if err := json.Unmarshal(body, &result); err != nil {
				fmt.Println("Error :", err)
				return err
			}
			ensureRmTag(resp.StatusCode, result.Code, result.Msg, repository, dataitem, tag)

		} else if resp.StatusCode == http.StatusUnauthorized {
			if err = Login(false, nil); err == nil {
				err = ItemOrTagRm(needLogin, args)
			} else {
				fmt.Println("Error :",err)
			}
		}
	} else {
		fmt.Println(ValidateErrMsgArgument)
		itemortagrmUsage()
		return errors.New(ValidateErrMsgArgument)
	}
	return nil
}

func ensureRmItem(code int, uri, msg string) {
	uri += "?ensure=1"

	if code == ExitsConsumingPlan {
		fmt.Print("DataHub : Order not completed, if deleted, the deposit will return to the subscribers.\n" +
			"DataItem deleted, and you could not be recovery, and all tags would be deleted either.\n" +
			"Are you sure to delete the current DataItem?[Y or N]:")
	} else if code == NoConsumingPlan {
		fmt.Print("Datahub : After you delete the DataItem, data could not be recovery, and all tags would be deleted either.\n" +
			"Are you sure to delete the current DataItem?[Y or N]:")
	} else if code == RepoOrItemNotExist {
		fmt.Println("Error :", msg)
		return
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

func ensureRmTag(statusCode, code int, msg, repository, dataitem, tag string)  {

	if statusCode == http.StatusBadRequest && code == RepoOrItemNotExist {
		fmt.Println("Error :", msg)
		return
	} else if statusCode == http.StatusBadRequest && code == TagNotExist {
		fmt.Println("Error :", msg)
		return
	} else if statusCode == http.StatusOK && code == TagExist {
		fmt.Print("DataHub : After you delete the Tag, data could not be recovery.\n" +
		"Are you sure to delete the current Tag?[Y or N]:")

		if GetEnsure() == true {
			uri := "/repositories/" + repository + "/" + dataitem + "/" + tag
			resp, err := commToDaemon("DELETE", uri, nil)
			if err != nil {
				fmt.Println(err)
				return
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
	} else {
		fmt.Println("Error :", msg)
	}

	return
}

func validateArgs(arg string) bool {
	if strings.ContainsAny(arg, "/") == false || strings.Count(arg, "/") < 1 || strings.IndexAny(arg, "/") == 0 || strings.IndexAny(arg, "/") == len(arg)-1 {
		return false
	}

	return true
}

func itemortagrmUsage() {
	fmt.Println("Usage: datahub repo rm [REPO]/[ITEM]")
	fmt.Println("Remove a item.\n")

	fmt.Println("Usage: datahub repo rm [REPO]/[ITEM]:[TAG]")
	fmt.Println("Remove a tag.")
}