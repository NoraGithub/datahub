package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"os"
	"strings"
)

type FormatDpCreate struct {
	Name string `json:"dpname, omitempty"`
	Type string `json:"dptype, omitempty"`
	Conn string `json:"dpconn, omitempty"`
}

var DataPoolTypes = []string{"file", "db", "hdfs", "jdbc", "s3", "api", "storm"}

func DpCreate(needLogin bool, args []string) (err error) {
	f := mflag.NewFlagSet("dp create", mflag.ContinueOnError)
	d := FormatDpCreate{}
	//f.StringVar(&d.Type, []string{"-type", "t"}, "file", "datapool type")
	//f.StringVar(&d.Conn, []string{"-conn", "c"}, "", "datapool connection info")
	f.Usage = dpcUseage
	if err = f.Parse(args); err != nil {
		return err
	}
	if len(args) == 1 {
		fmt.Printf("Are you really going to create a datapool:%s in default type 'file'?\nY or N:", args[0])
		if GetEnsure() == true {
			d.Name = args[0]
			d.Conn = GstrDpPath
			d.Type = "file"
		} else {
			return
		}
	} else {
		if len(args) != 2 || len(args[0]) == 0 {
			fmt.Printf("invalid argument.\nSee '%s --help'.\n", f.Name())
			return
		}
		d.Name = args[0]
		sp := strings.Split(args[1], "://")
		//fmt.Println("sp len:", len(sp), sp)
		if len(sp) > 1 && len(sp[1]) > 0 {
			d.Type = strings.ToLower(sp[0])
			if sp[1][0] != '/' && d.Type == "file" {
				fmt.Println("please input absolute path after 'file://', e.g. file:///home/user/mydp")
				return
			}
			d.Conn = "/" + strings.Trim(sp[1], "/")
		} else if len(sp) == 1 && len(sp[0]) != 0 {
			d.Type = "file"
			if sp[0][0] != '/' {
				fmt.Println("please input absolute path , e.g. /home/user/mydp")
				return
			}
			d.Conn = "/" + strings.Trim(sp[0], "/")
		} else {
			fmt.Printf("Invalid argument.\nSee '%s --help'.\n", f.Name())
			return
		}
	}

	var allowtype bool = false
	for _, v := range DataPoolTypes {
		if d.Type == v {
			allowtype = true
		}
	}
	if !allowtype {
		fmt.Println("Datapool type need to be:", DataPoolTypes)
		return
	}

	if needLogin && !Logged {
		login(false)
	}
	jsonData, err := json.Marshal(d)
	if err != nil {
		return err
	}
	resp, err := commToDaemon("POST", "/datapools", jsonData)
	defer resp.Body.Close()
	showResponse(resp)

	return err
}

func GetEnsure() bool {
	reader := bufio.NewReader(os.Stdin)
	en, _ := reader.ReadBytes('\n')
	ens := strings.Trim(string(en), "\n")
	Yes := []string{"y", "yes", "Yes", "Y", "YES"}
	for _, y := range Yes {
		if ens == y {
			return true
		}
	}
	return false
}

func dpcUseage() {
	fmt.Println("Usage of datahub dp create:")
	fmt.Println("  datahub dp create DATAPOOL [file://][ABSOLUTE PATH]")
	fmt.Println("  e.g. datahub dp create dptest file:///home/user/test")
	fmt.Println("       datahub dp create dptest /home/user/test")
	fmt.Println("Create a datapool\n")

}
