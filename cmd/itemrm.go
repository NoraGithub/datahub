package cmd

import (
	"fmt"
	"strings"
)

func ItemOrTagRm(needLogin bool, args []string) error {

	if len(args) > 1 {
		fmt.Println("invalid argument..")
		itemrmUsage()
		return nil
	}
	arg := args[0]
	var repository string
	var dataitem string
	//var tag string

	splitStr := strings.Split(arg, "/");
	if len(splitStr) == 2 {
		repository = splitStr[0]
		dataitem = splitStr[1]

		resp, err := commToDaemon("DELETE", "/repositories/"+repository+"/"+dataitem, nil)
		if err != nil {
			fmt.Println(err)
			return err
		}
		defer resp.Body.Close()

		//showResponse(resp)

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

func tagrmUsage() {
	fmt.Printf("Usage: datahub repo rm [REPO]/[ITEM]:[tag]\n")
	fmt.Println("Remove a tag")
}

func itemrmUsage() {
	fmt.Printf("Usage: datahub repo rm [REPO]/[ITEM]\n")
	fmt.Println("Remove a item")
}
