package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"net/http"
	"os"
	"strings"
)

type FormatDpCreate struct {
	Name string `json:"dpname, omitempty"`
	Type string `json:"dptype, omitempty"`
	Conn string `json:"dpconn, omitempty"`
	Host string `json:"host, omitempty"`
	Port string `json:"port, omitempty"`
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
		if strings.Contains(args[0], "/") == true {
			fmt.Println("DataHub : The name of datapool can't contain '/'.")
			return
		}
		fmt.Print("DataHub : Are you sure to create a datapool ",
			" with default type 'file' and path \"/var/lib/datahub\" ?\n[Y or N]:")
		if GetEnsure() == true {
			d.Name = args[0]
			d.Conn = GstrDpPath
			d.Type = "file"
		} else {
			return
		}
	} else {
		if len(args) != 2 || len(args[0]) == 0 {
			fmt.Printf("DataHub : Invalid argument.\nSee '%s --help'.\n", f.Name())
			return
		}
		if strings.Contains(args[0], "/") == true {
			fmt.Println("DataHub : The name of datapool can't contain '/'.")
			return
		}
		d.Name = args[0]
		sp := strings.Split(args[1], "://")

		if len(sp) > 1 && len(sp[1]) > 0 {
			d.Type = strings.ToLower(sp[0])
			if sp[1][0] != '/' && d.Type == "file" {
				fmt.Println("DataHub : Please input absolute path after 'file://', e.g. file:///home/user/mydp")
				return
			}
			if d.Type == "file" {
				d.Conn = "/" + strings.Trim(sp[1], "/")
			} else if d.Type == "s3" {
				//d.Conn = strings.Trim(sp[1], "/")
				d.Conn = sp[1]
			} else if d.Type == "hdfs" {
				d.Conn = sp[1]
				if validateDpconn(d.Conn, d.Type) == false {
					return
				}
			}

		} else if len(sp) == 1 && len(sp[0]) != 0 {
			d.Type = "file"
			if sp[0][0] != '/' {
				fmt.Printf("DataHub : Please input path for '%s'.\n", args[0])
				return
			}
			d.Conn = "/" + strings.Trim(sp[0], "/")
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

	jsonData, err := json.Marshal(d)
	if err != nil {
		return err
	}
	resp, err := commToDaemon("POST", "/datapools", jsonData)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		showResponse(resp)
	} else {
		showError(resp)
	}

	return err
}

func GetEnsure() bool {
	reader := bufio.NewReader(os.Stdin)
	en, _ := reader.ReadBytes('\n')
	ens := strings.Trim(string(en), "\n")
	ens = strings.ToLower(ens)
	Yes := []string{"y", "yes"}
	for _, y := range Yes {
		if ens == y {
			return true
		}
	}
	return false
}

func validateDpconn(dpconn, dptype string) bool {
	if dptype == "hdfs" {
		args := strings.Split(dpconn, "@")
		usernamepassword := args[0]
		entrypoint := args[1]
		if strings.Contains(usernamepassword, ":") == false {
			fmt.Println("DataHub : Invalid username or password.\nSee 'datahub dp create --help'.")
			return false
		} else {
			args = strings.SplitN(usernamepassword, ":", 2)
			username := args[0]
			//password := args[1]
			if username == "" {
				fmt.Println("DataHub : Username can not be empty.\nSee 'datahub dp create --help'.")
				return false
			}
		}

		if strings.Contains(entrypoint, ":") == false || strings.Count(entrypoint, ":") != 1 {
			fmt.Println("DataHub : Invalid PORT.\nSee 'datahub dp create --help'.")
			return false
		}
		if strings.Count(entrypoint, ".") != 3 {
			fmt.Println("Datahub : Invalid IP.\nSee 'datahub dp create --help'.")
			return false
		} else {
			args := strings.Split(entrypoint, ".")
			for _, arg := range args {
				if arg == "" {
					fmt.Println("Datahub : Invalid IP.\nSee 'datahub dp create --help'.")
					return false
				}
			}
		}

	}

	return true
}

func dpcUseage() {
	fmt.Println("Usage of datahub dp create:")
	fmt.Println("datahub dp create DATAPOOL [[file://][ABSOLUTE_PATH]] [[s3://][BUCKET##ID##KEY##REGION]] [[hdfs://][USERNAME:PASSWORD@HOST:PORT]]")
	fmt.Println("e.g. datahub dp create dptest file:///home/user/test")
	fmt.Println("     datahub dp create s3dp s3://mybucket##ABCDEFGHIGKLMNSSSLSS##lC4SBSSBx5HC/bSfniihhlnH3qpCJjgkLKDDSWAf##cn-north-1")
	fmt.Println("     datahub dp create hdfsdp hdfs://root:123@127.0.0.1:9000")
	fmt.Println("Create a datapool.\n")

}
