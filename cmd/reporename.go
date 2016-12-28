package cmd

import (
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"net/http"
	"strings"
)

func RepoAndItemRename(login bool, args []string) error {
	f := mflag.NewFlagSet("repo rename", mflag.ContinueOnError)
	f.Usage = repooritemrenameUsage
	if err := f.Parse(args); err != nil {
		return err
	}

	if len(args) == 0 || len(args) == 1 || len(args) > 2 {
		fmt.Println(ErrMsgArgument)
		repooritemrenameUsage()
		return errors.New(ErrMsgArgument)
	}
	arg := args[0]
	var uri string
	var repository string
	var dataitem string
	newname := args[1]

	if strings.ContainsAny(arg, "/") {
		if strings.Count(arg, "/") > 1 || strings.IndexAny(arg, "/") == 0 || strings.IndexAny(arg, "/") == len(arg)-1 {
			fmt.Println(ValidateErrMsgArgument_rename)
			repooritemrenameUsage()
			return errors.New(ValidateErrMsgArgument_rename)
		}
		splitStr := strings.Split(arg, "/")

		repository = splitStr[0]
		dataitem = splitStr[1]
		uri = "/repositories/" + repository + "/" + newname + "/" + dataitem
	} else {
		repository = args[0]
		uri = "/repositories/" + repository + "/" + newname
	}
	resp, err := commToDaemon("PUT", uri, nil)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		showResponse(resp)
	} else if resp.StatusCode == http.StatusUnauthorized {
		fmt.Println("Error : Not login.")
		return err
	} else {
		showError(resp)
	}
	return nil
}

func repooritemrenameUsage() {
	fmt.Println("Usage: datahub repo rename [REPO] [REPONEWNAME]")
	fmt.Println("Rename a repo.\n")

	fmt.Println("Usage: datahub repo rename [REPO]/[ITEM] [ITEMNEWNAME]")
	fmt.Println("Rename a item.")
}
