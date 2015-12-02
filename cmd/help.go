package cmd

import (
	"fmt"
)

type CmdHelp struct {
	Name    string
	Handler func()
}

var cmdhelps = []CmdHelp{
	{"dp", dpUsage},
	{"ep", epUsage},
	{"login", loginUsage},
	{"pub", pubUsage},
	{"pull", pullUsage},
	{"repo", repoUsage},
	{"subs", subsUsage},
}

func Help(login bool, args []string) (err error) {

	if len(args) > 0 {
		for _, v := range cmdhelps {
			if args[0] == v.Name {
				v.Handler()
				return
			}
		}
		fmt.Printf("datahub: '%s' not found, see 'datahub --help'.\n", args[0])
	} else {
		ShowUsage()
	}
	return
}
