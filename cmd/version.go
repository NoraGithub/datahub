package cmd

import (
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"os"
)

func Version(needLogin bool, args []string) (err error) {
	if len(args) == 0 {
		fmt.Println("datahub", ds.DATAHUB_VERSION)
		return
	}

	if len(args) > 0 {
		verUsage()
		return
	}
	return nil
}

func verUsage() {
	fmt.Printf("Usage:\n%s version\n", os.Args[0])
	fmt.Printf("\nshow datahub version.\n")

}
