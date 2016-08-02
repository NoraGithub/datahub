package cmd

import (
	"fmt"
	"os"
)

func EpRm(needLogin bool, args []string) (err error) {

	if len(args) > 0 {
		eprmUsage()
		return
	}

	resp, err := commToDaemon("delete", "/ep", nil)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	showResponse(resp)

	return err
}

func eprmUsage() {
	fmt.Printf("Usage: %s ep rm\n", os.Args[0])
	fmt.Println("\nRemove the entrypoint.")
}
