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
	f := mflag.NewFlagSet("datahub dp create", mflag.ContinueOnError)
	d := FormatDpCreate{}
	//f.StringVar(&d.Type, []string{"-type", "t"}, "file", "datapool type")
	//f.StringVar(&d.Conn, []string{"-conn", "c"}, "", "datapool connection info")
	f.Usage = dpcUseage //--help
	if err = f.Parse(args); err != nil {
		return err
	}
	if len(args) == 1 {
		d.Conn = GstrDpPath + "\\Datapool"
		fmt.Printf("DataHub : Are you sure to create a datapool "+
			"with default type 'file' and path \"%v\" ?\n[Y or N]:", d.Conn)
		if GetEnsure() == true {
			d.Name = args[0]
			d.Type = "file"
			//fmt.Println("input yes")
		} else {
			//fmt.Println("input no")
			return
		}
	} else {
		if len(args) != 2 || len(args[0]) == 0 {
			fmt.Printf("DataHub : Invalid argument.\nSee '%s --help'.\n", f.Name())
			return
		}
		d.Name = args[0]
		sp := strings.Split(args[1], "://")

		if len(sp) > 1 && len(sp[1]) > 0 {
			d.Type = strings.ToLower(sp[0])
			if strings.Contains(sp[1], ":") == false && d.Type == "file" {
				fmt.Println("DataHub : Please input absolute path after 'file://', e.g. file://D:\\data\\mydatapool")
				return
			}
			/*if d.Type == "file" {
				d.Conn = "/" + strings.Trim(sp[1], "/")
			} else {
				d.Conn = strings.Trim(sp[1], "/")
			}*/
			d.Conn = sp[1]

			//} else if len(sp) == 1 && len(sp[0]) != 0 {
			//d.Type = "file"
			/*if sp[0][0] != '/' {
				fmt.Printf("DataHub : Please input path for '%s'.\n", args[0])
				return
			}
			d.Conn = "/" + strings.Trim(sp[0], "/")*/
		} else {
			fmt.Printf("Error : Invalid argument.\nSee '%s --help'.\n", f.Name())
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
		fmt.Println("DataHub : Datapool type need to be:", DataPoolTypes)
		return
	}
	//fmt.Println(d)
	jsonData, err := json.Marshal(d)
	if err != nil {
		fmt.Println(err)
		return err
	}
	//fmt.Println("req to daemon")
	resp, err := commToDaemon("POST", "/datapools", jsonData)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()
	showResponse(resp)

	return err
}

func GetEnsure() bool {
	reader := bufio.NewReader(os.Stdin)
	en, _ := reader.ReadBytes('\n')
	//fmt.Printf("%X\n", en)
	ens := strings.Trim(string(en), "\r\n") //on windows \r\n
	ens = strings.ToLower(ens)
	Yes := []string{"y", "yes"}
	for _, y := range Yes {
		if ens == y {
			return true
		}
	}
	return false
}

func dpcUseage() {
	fmt.Println("Usage of datahub dp create:")
	fmt.Println("  datahub dp create DATAPOOL [[file://][ABSOLUTE_PATH]] [[s3://][BUCKET]]")
	fmt.Println("  e.g. datahub dp create dptest file://C:\\data")
	fmt.Println("       datahub dp create s3dp s3://mybucket")
	fmt.Println("Create a datapool\n")

}
