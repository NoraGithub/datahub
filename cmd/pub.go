package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	//"net/url"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	PRIVATE = "private"
	PUBLIC  = "public"
)

func Pub(needlogin bool, args []string) (err error) {
	usage := "usage: datahub pub repository/dataitem DPNAME/ITEMDESC \n\t datahub pub repository/dataitem:tag TAGDETAIL"
	if len(args) < 2 {
		//fmt.Println(usage)
		pubUsage()
		return errors.New("args len error!")
	}
	pub := ds.PubPara{}
	//var largs []string = args
	var repo, item, tag, argfi, argse string
	f := mflag.NewFlagSet("pub", mflag.ContinueOnError)
	//f.StringVar(&pub.Datapool, []string{"-datapool", "p"}, "", "datapool name")
	f.StringVar(&pub.Accesstype, []string{"-accesstype", "t"}, "private", "dataitem accesstype, private or public")
	f.StringVar(&pub.Comment, []string{"-comment", "m"}, "", "comments")
	//f.StringVar(&pub.Detail, []string{"-detail", "d"}, "", "tag detail ,for example file name")
	f.Usage = pubUsage

	if err = f.Parse(args[2:]); err != nil {
		//fmt.Println("parse parameter error")
		return err
	}

	//fmt.Println(pub.Accesstype)
	//fmt.Println(pub.Comment)
	if len(args[0]) == 0 || len(args[1]) == 0 {
		fmt.Println(usage)
		return errors.New("need item or tag error!")
	}

	/*if len(f.Args()) > 0 {
		fmt.Printf("invalid argument.\nSee '%s --help'.\n", f.Name())
		return errors.New("invalid argument")
	}*/

	argfi = strings.Trim(args[0], "/")
	//deal arg[0]
	sp := strings.Split(argfi, "/")
	if len(sp) != 2 {
		//fmt.Println(usage)
		return errors.New("invalid repo/item")
	}
	repo = sp[0]
	sptag := strings.Split(sp[1], ":")
	l := len(sptag)
	if l == 1 {
		item = sptag[0]
		argse = strings.Trim(args[1], "/")
		se := strings.Split(argse, "/")
		if len(se) == 2 {
			pub.Datapool = se[0]
			pub.ItemDesc = se[1]
			err = PubItem(repo, item, pub, args)
		} else {
			fmt.Println("please input DPNAME/ITEMDESC when you publish dataitem.")
			err = errors.New("please input DPNAME/ITEMDESC when you publish dataitem.")
		}
	} else if l == 2 {
		item = sptag[0]
		tag = sptag[1]
		pub.Detail = args[1]
		err = PubTag(repo, item, tag, pub, args)
	} else {
		fmt.Printf("invalid argument.\nSee '%s --help'.\n", f.Name())
		return errors.New("invalid argument")
	}

	return err

}

func PubItem(repo, item string, p ds.PubPara, args []string) (err error) {
	url := repo + "/" + item
	if len(p.Accesstype) == 0 {
		p.Accesstype = PRIVATE
	}
	if len(p.Datapool) == 0 {
		fmt.Println("Publishing dataitem requires a parameter \"--datapool=???\" .")
		return
	}
	jsonData, err := json.Marshal(p)
	if err != nil {
		fmt.Println("Mrashal pubdata error while publishing dateitem.")
		return err
	}
	err = pubResp(url, jsonData, args)
	return err
}

func PubTag(repo, item, tag string, p ds.PubPara, args []string) (err error) {
	url := repo + "/" + item + "/" + tag
	if len(p.Detail) == 0 {
		fmt.Println("Publishing tag requires a parameter \"--detail=???\" to ")
		return
	}
	if p.Detail[0] != '/' && strings.Contains(p.Detail, "/") {
		p.Detail, err = filepath.Abs(p.Detail)
		if err != nil {
			log.Print(err.Error())
			return
		}
	}
	jsonData, err := json.Marshal(p)
	if err != nil {
		fmt.Println("Mrashal pubdata error while publishing tag.")
		return err
	}
	err = pubResp(url, jsonData, args)

	return err
}

func pubResp(url string, jsonData []byte, args []string) (err error) {
	resp, err := commToDaemon("POST", "/repositories/"+url, jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == 200 {
		result := ds.Result{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Pub error. ", err.Error())
			return err
		} else {
			if result.Code == 0 {
				fmt.Println("Pub success, ", result.Msg)
			} else {
				fmt.Println("Error code: ", result.Code, " Msg: ", result.Msg)
			}
		}
	} else if resp.StatusCode == 401 {
		if err := Login(false, nil); err == nil {
			Pub(true, args)
		} else {
			fmt.Println(err)
		}
	} else {
		result := ds.Result{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Pub error. ", err.Error())
			return err
		} else {
			fmt.Println("Http response code: ", resp.StatusCode, "  Error Code: ", result.Code, "  Msg: ", result.Msg)
		}
	}
	return err
}

func pubUsage() {
	fmt.Printf("usage: \n %s pub REPO/DATAITEM  DPNAME/ITEMDESC, --accesstype=?\n", os.Args[0])
	fmt.Println("  --accesstype,-t   Specify the access type of the dataitem:public or private, default private")
	fmt.Printf(" %s pub REPO/DATAITEM:Tag TAGDETAIL\n", os.Args[0])
	fmt.Println("  --comment,-m      Comments about the item or tag")
}
