package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
	"os"
)

type FormatDp struct {
	Name string `json:"dpname"`
	Type string `json:"dptype"`
	//Conn string `json:"dpconn"`
}

type Item struct {
	Repository string `json:"repository"`
	DataItem   string `json:"dataitem"`
	Tag        string `json:"tag"`
	Time       string `json:"time"`
	Publish    string `json:"publish"`
	TagDetail  string `json:"detail, omitempty"`
	ItemDesc   string `json:"itemdesc, omitempty"`
}
type FormatDpDetail struct {
	Name  string `json:"dpname"`
	Type  string `json:"dptype"`
	Conn  string `json:"dpconn"`
	Items []Item `json:"items"`
}

func Dp(needLogin bool, args []string) (err error) {
	if needLogin && !Logged {
		login(false)
	}
	f := mflag.NewFlagSet("dp", mflag.ContinueOnError)
	f.Usage = dpUsage
	if err = f.Parse(args); err != nil {
		return err
	}

	if len(args) == 0 {
		resp, err := commToDaemon("GET", "/datapools", nil)
		if err != nil {
			fmt.Println(err)
			return err
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusOK {
			dpResp(false, body)
		} else {
			fmt.Println(string(body))
			fmt.Println(resp.StatusCode)
			err = errors.New(string(resp.StatusCode))
		}

	} else {
		//support: dp name1 name2 name3
		for _, v := range args {
			if len(v) > 0 && v[0] != '-' {
				strdp := fmt.Sprintf("/datapools/%s", v)
				resp, err := commToDaemon("GET", strdp, nil)
				if err != nil {
					fmt.Println(err)
					return err
				}
				defer resp.Body.Close()
				body, _ := ioutil.ReadAll(resp.Body)
				if resp.StatusCode == http.StatusOK {
					dpResp(true, body)
				} else {
					fmt.Println(resp.StatusCode)
					err = errors.New(string(resp.StatusCode))
				}
			}
		}
	}
	return err
}

func dpResp(bDetail bool, RespBody []byte) {
	if bDetail == false {
		strcDps := []FormatDp{}
		result := &ds.Result{Data: &strcDps}
		err := json.Unmarshal(RespBody, result)
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}

		if result.Code == ResultOK {
			n, _ := fmt.Printf("%-16s    %-8s\n", "DATAPOOL", "TYPE")
			printDash(n - 5)
			for _, dp := range strcDps {
				fmt.Printf("%-16s    %-8s\n", dp.Name, dp.Type)
			}
		} else {
			fmt.Println("ERROR:", result.Code, " Msg:", result.Msg)
		}
	} else {
		strcDp := FormatDpDetail{}
		result := ds.Result{Data: &strcDp}
		err := json.Unmarshal(RespBody, &result)
		if err != nil {
			fmt.Println("ERROR: dpname ", err)
			return
		}
		if result.Code == ResultOK {
			n, _ := fmt.Printf("%s%-16s\t%-16s\t%-16s\n", "DATAPOOL:", strcDp.Name, strcDp.Type, strcDp.Conn)
			for _, item := range strcDp.Items {
				RepoItemTag := item.Repository + "/" + item.DataItem + ":" + item.Tag
				if item.Publish == "Y" {
					fmt.Printf("%-32s\t%-20s\t%-5s\t%-32s\t%-64s\n", RepoItemTag, item.Time, "pub", item.ItemDesc, item.TagDetail)
				} else {
					fmt.Printf("%-32s\t%-20s\t%-5s\t%-32s\t%-64s\n", RepoItemTag, item.Time, "pull", item.ItemDesc, item.TagDetail)
				}
			}
			printDash(n)
		} else {
			fmt.Println("ERROR:", result.Code, " Msg:", result.Msg)
		}
	}
}

func GetResultMsg(RespBody []byte, bprint bool) (sMsgResp string) {
	result := &ds.Result{}
	err := json.Unmarshal(RespBody, result)
	if err != nil {
		sMsgResp = "Get /datapools  dpResp json.Unmarshal error!"
	} else {
		sMsgResp = "Result code:" + string(result.Code) + " Msg:" + string(result.Msg)
		if bprint == true {
			fmt.Println(sMsgResp)
		}
	}
	return sMsgResp
}

func dpUsage() {
	fmt.Printf("Usage: \n  %s dp [DATAPOOL]\n", os.Args[0])
	fmt.Println("List all the datapools or one datapool\n")
	dpcUseage()
	dprUseage()
	//fmt.Printf("\n", os.Args[0])
	//fmt.Println("  --type , -t, The type of datapool , file default")
	//fmt.Println("  --conn, -c, datapool connection info, for datapool with type of file, it's dir")
}
