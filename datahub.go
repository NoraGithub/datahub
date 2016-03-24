package main

import (
	"fmt"
	"github.com/asiainfoLDP/datahub/client"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/daemon"
	//"github.com/asiainfoLDP/datahub/daemon/daemonigo"
	"github.com/asiainfoLDP/datahub/ds"
	flag "github.com/asiainfoLDP/datahub/utils/mflag"
	"os"
)

var (
	runDaemon bool
)

func init() {
	flagParse()
}

func flagParse() {
	flDaemon := flag.Bool([]string{"D", "-daemon"}, false, "Enable daemon mode")
	flVersion := flag.Bool([]string{"V", "-version"}, false, "Show version")
	flToken := flag.String([]string{"-token"}, "", "user token")

	flag.Usage = cmd.ShowUsage
	//flag.PrintDefaults()
	flag.Parse()
	//fmt.Printf("run daemon: %v, version: %v\n", *flDaemon, *flVersion)

	if *flVersion {
		fmt.Println("datahub", ds.DATAHUB_VERSION)
		os.Exit(0)
	}

	if len(*flToken) == 40 {
		//daemonigo.Token = *flToken
		daemon.DaemonID = *flToken
	}

	if *flDaemon {
		runDaemon = true

	}
}

func main() {

	if runDaemon {
		daemon.RunDaemon()
	} else {
		client.RunClient()
	}
}
