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
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	PRIVATE = "private"
	PUBLIC  = "public"
)

func Pub(needlogin bool, args []string) (err error) {
	if len(args) < 2 {
		fmt.Println(ErrMsgArgument)
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

	if len(args) > 2 {
		if err = f.Parse(args[2:]); err != nil {
			fmt.Println("Error : parse parameter error.", err)
			return err
		}
	}

	if len(args[0]) == 0 || len(args[1]) == 0 {
		fmt.Println(ErrMsgArgument)
		pubUsage()
		return errors.New("need item or tag error!")
	}

	argfi = strings.Trim(args[0], "/")
	//deal arg[0]
	sp := strings.Split(argfi, "/")
	if len(sp) != 2 {
		fmt.Println(ErrMsgArgument)
		pubUsage()
		return errors.New("invalid repo/item")
	}
	repo = sp[0]
	sptag := strings.Split(sp[1], ":")
	l := len(sptag)
	if l == 1 {
		item = sptag[0]
		argse = strings.Trim(args[1], "/")
		se := strings.Split(argse, "://")
		if len(se) == 2 && len(se[1]) > 0 {
			pub.Datapool = se[0]
			pub.ItemDesc = strings.Trim(se[1], "/")
			err = PubItem(repo, item, pub, args)
		} else {
			fmt.Println("DataHub : Please input a valid datapool and path.")
			err = errors.New("Error : Please input a valid datapool and path.")
		}
	} else if l == 2 {
		item = sptag[0]
		tag = sptag[1]
		pub.Detail = args[1]
		err = PubTag(repo, item, tag, pub, args)
	} else {
		fmt.Printf("DataHub : Invalid argument.\nSee '%s --help'.\n", f.Name())
		return errors.New("Invalid argument.")
	}

	return err

}

func PubItem(repo, item string, p ds.PubPara, args []string) (err error) {
	url := repo + "/" + item
	if len(p.Accesstype) == 0 {
		p.Accesstype = PRIVATE
	}
	p.Accesstype = strings.ToLower(p.Accesstype)
	if p.Accesstype != PRIVATE && p.Accesstype != PUBLIC {
		fmt.Println("Error : Invalid accesstype, e.g accesstype=public, private")
		return
	}
	if len(p.Datapool) == 0 {
		fmt.Println("DataHub : Publishing dataitem requires a parameter \"--datapool=???\" .")
		return
	}
	jsonData, err := json.Marshal(p)
	if err != nil {
		fmt.Println("Error : Marshal pubdata error while publishing dateitem.")
		return err
	}

	resp, err := commToDaemon("POST", "/repositories/"+url, jsonData)
	if err != nil {
		fmt.Println("Error :", err)
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		result := ds.Result{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Error : Pub error.", err) //todo add http code
			return err
		} else {
			if result.Code == 0 {
				fmt.Println("DataHub : Successed in publishing.")
			} else {
				fmt.Printf("Error : %v\n", result.Msg)
			}
		}
	} else if resp.StatusCode == http.StatusUnauthorized {
		if err = Login(false, nil); err == nil {
			Pub(true, args)
		} else {
			fmt.Println(err)
		}
	} else {
		result := ds.Result{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Error : Pub error.", err)
			return err
		}
		if result.Code == 1008 {
			fmt.Printf("Error : Dataitem '%s' already exists.\n", item)
		} else if result.Code == 4010 {
			fmt.Printf("Error : Datapool '%s' not found.\n", p.Datapool)
		} else if result.Code == 1011 {
			fmt.Println("Error : Only 50 items should be included within each repository.")
		} else {
			fmt.Println("Error :", result.Msg)
		}
	}
	//err = pubResp(url, jsonData, args)
	return err
}

func PubTag(repo, item, tag string, p ds.PubPara, args []string) (err error) {
	url := repo + "/" + item + "/" + tag
	if len(p.Detail) == 0 {
		fmt.Println("DataHub : Publishing tag requires a parameter \"--detail=???\" ")
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
		fmt.Println("Error : Mrashal pubdata error while publishing tag.")
		return err
	}

	err = pubResp(url, jsonData, args)

	return err
}

func pubResp(url string, jsonData []byte, args []string) (err error) {
	resp, err := commToDaemon("POST", "/repositories/"+url, jsonData)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		result := ds.Result{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Error : Pub error.", err) //todo add http code
			return err
		} else {
			if result.Code == 0 {
				fmt.Println("DataHub : Successed in publishing.")
			} else {
				fmt.Printf("Error : %v\n", result.Msg)
			}
		}
	} else if resp.StatusCode == http.StatusUnauthorized {
		if err = Login(false, nil); err == nil {
			Pub(true, args)
		} else {
			fmt.Println(err)
		}
	} else {
		result := ds.Result{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Error : Pub error.", err)
			return err
		} else {
			fmt.Println("Error :", result.Msg)
		}

		/*else {
			fmt.Printf("Error : %v\n", result.Msg)
		}*/
	}
	return err
}

func pubUsage() {
	fmt.Printf("Usage: \n  %s pub REPO/DATAITEM  DPNAME://ITEMDESC\n", os.Args[0])
	fmt.Println("Publish a dataitem\n")
	fmt.Printf("  %s pub REPO/DATAITEM:TAG TAGDETAIL\n", os.Args[0])
	fmt.Println("Publish a tag\n")
	fmt.Println("Options:\n")
	fmt.Println("  --accesstype,-t   Specify the access type of the dataitem:public or private, default private")
	fmt.Println("  --comment,-m      Comments about the dataitem or tag")
}
