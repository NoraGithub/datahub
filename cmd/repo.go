package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"github.com/asiainfoLDP/datahub/utils"
)

const (
	Repos = iota
	ReposReponame
	ReposReponameDataItem
	ReposReponameDataItemTag
)

func Repo(login bool, args []string) (err error) {
	f := mflag.NewFlagSet("repo", mflag.ContinueOnError)
	f.Usage = repoUsage
	if err = f.Parse(args); err != nil {
		return err
	}
	var icmd int
	if len(args) > 1 {
		fmt.Println("DataHub : Invalid argument.")
		repoUsage()
		return
	}
	var repo, item, tag, uri string
	if len(args) == 0 {
		uri = "/repositories"
		icmd = Repos
	} else {
		u, err := url.Parse(args[0])
		if err != nil {
			fmt.Println("Error :", err)
			return err
		}
		source := u.Path
		if (len(u.Path) > 0) && (u.Path[0] == '/') {
			source = u.Path[1:]
		}

		urls := strings.Split(source, "/")
		lenth := len(urls)

		if lenth == 0 {
			uri = "/repositories"
			icmd = Repos
			//fmt.Println(uri, icmd)
		} else if lenth == 1 || (lenth == 2 && len(urls[1]) == 0) {
			uri = "/repositories/" + urls[0]
			icmd = ReposReponame
			repo = urls[0]
			//fmt.Println(uri, icmd)
		} else if lenth == 2 || (lenth == 3 && len(urls[2]) == 0) {
			target := strings.Split(urls[1], ":")
			tarlen := len(target)
			if tarlen == 1 || (tarlen == 2 && len(target[1]) == 0) {
				uri = "/repositories/" + urls[0] + "/" + target[0]
				icmd = ReposReponameDataItem
				repo = urls[0]
				item = target[0]
				//fmt.Println(uri, icmd)
			} else if tarlen == 2 {
				uri = "/repositories/" + urls[0] + "/" + target[0] + "/" + target[1]
				icmd = ReposReponameDataItemTag
				repo = urls[0]
				item = target[0]
				tag = target[1]
				//fmt.Println(uri, icmd)
			} else {
				fmt.Println("Error : The parameter after repo is in wrong format!")
				return errors.New("The parameter after repo is in wrong format!")
			}
		} else {
			fmt.Println("Error : The parameter after repo is in wrong format!")
			return errors.New("The parameter after repo is in wrong format!")
		}
	}

	resp, err := commToDaemon("get", uri, nil)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		repoResp(icmd, body, repo, item, tag)
	} else if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusBadRequest {
		body, _ := ioutil.ReadAll(resp.Body)
		result := ds.Result{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Error : http StatusCode:", resp.StatusCode, "Json format error!")
			return err
		}
		if result.Code == ServerErrResultCode1400 {
			if err = Login(false, nil); err == nil {
				err = Repo(login, args)
			} else {
				//fmt.Println(err)
			}
		} else {
			fmt.Printf("Error : %v\n", result.Msg)
			return nil
		}
		//fmt.Println(resp.StatusCode, "returned....")

	} else {
		showError(resp)
	}

	return err
}

func repoUsage() {
	fmt.Printf("Usage: %s repo [URL]/[REPO]/[ITEM]\n", os.Args[0])
	fmt.Println("\nShow the repository , dataitem and tag")
}

func repoResp(icmd int, respbody []byte, repo, item, tag string) {
	//fmt.Println(string(respbody))
	result := ds.Result{Code: ResultOK}
	if icmd == Repos {
		repos := []ds.Repositories{}
		result.Data = &repos
		err := json.Unmarshal(respbody, &result)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%-16s\n", "REPOSITORY") //, "UPDATETIME", "COMMENT")

		for _, v := range repos {
			fmt.Printf("%-16s\n", v.RepositoryName) //, v.Optime, v.Comment)
		}
	} else if icmd == ReposReponame {
		onerepo := ds.Repository{}
		result.Data = &onerepo
		err := json.Unmarshal(respbody, &result)
		if err != nil {
			panic(err)
		}
		fmt.Printf("REPOSITORY/DATAITEM\n")

		for _, v := range onerepo.DataItems {
			fmt.Printf("%s/%s\n", repo, v)
		}

	} else if icmd == ReposReponameDataItem {
		itemStatus, err := getItemStatus(repo, item)
		if err != nil {
			fmt.Println("Error :", err)
			return
		}
		var tagStatus string
		repoitemtags := ds.Data{}
		result.Data = &repoitemtags
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			fmt.Println("Error :", err)
			return
		}
		abnormalTags, err := getTagStatusOfItem(repo, item)
		if err != nil {
			fmt.Println("Error :", err)
			return
		}

		repoitemname := repo + "/" + item

		ctag := []string{"REPOSITORY/ITEM:TAG"}
		cupdatetime := []string{"UPDATETIME"}
		ccomment := []string{"COMMENT"}
		cstatus := []string{"STATUS"}

		if itemStatus == "offline" {
			for _, v := range repoitemtags.Taglist {
				//fmt.Printf("%s/%s:%s\t%s\t%s\t%s\n", repo, item, v.Tag, v.Optime, v.Comment, "ABNORMAL")
				cupdatetime = append(cupdatetime, v.Optime)
				ccomment = append(ccomment, v.Comment)
				cstatus = append(cstatus, "ABNORMAL")
			}
		} else {
			for _, v := range repoitemtags.Taglist {
				repoitemtag := repoitemname + ":" + v.Tag
				tagStatus = judgeTag(abnormalTags, repoitemtag)
				//fmt.Printf("%s/%s:%s\t%s\t%s\t%s\n", repo, item, v.Tag, v.Optime, v.Comment, tagStatus)
				ctag = append(ctag, repo+"/"+item+":"+v.Tag)
				cupdatetime = append(cupdatetime, v.Optime)
				ccomment = append(ccomment, v.Comment)
				cstatus = append(cstatus, tagStatus)
			}
		}
		utils.PrintFmt(ctag, cupdatetime, ccomment, cstatus)

	} else if icmd == ReposReponameDataItemTag {
		itemStatus, err := getItemStatus(repo, item)
		if err != nil {
			fmt.Println("Error :", err)
			return
		}
		tagStatus, err := getTagStatus(repo, item, tag)
		if err != nil {
			fmt.Println("Error :", err)
			return
		}
		onetag := ds.Tag{}
		result.Data = &onetag
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			fmt.Println("Error :", err)
			return
		}
		//n, _ := fmt.Printf("%s\t%s\t%s\t\t%s\n", "REPOSITORY/ITEM:TAG", "UPDATETIME", "COMMENT", "STATUS")
		ctag := []string{"REPOSITORY/ITEM:TAG"}
		cupdatetime := []string{"UPDATETIME"}
		ccomment := []string{"COMMENT"}
		cstatus := []string{"STATUS"}
		if itemStatus == "offline" {
			//fmt.Printf("%s/%s:%s\t%s\t%s\t%s\n", repo, item, tag, onetag.Optime, onetag.Comment, "ABNORMAL")
			ctag = append(ctag, repo+"/"+item+":"+tag)
			cupdatetime = append(cupdatetime, onetag.Optime)
			ccomment = append(ccomment, onetag.Comment)
			cstatus = append(cstatus, "ABNORMAL")
		} else {
			//fmt.Printf("%s/%s:%s\t%s\t%s\t%s\n", repo, item, tag, onetag.Optime, onetag.Comment, tegStatus)
			ctag = append(ctag, repo+"/"+item+":"+tag)
			cupdatetime = append(cupdatetime, onetag.Optime)
			ccomment = append(ccomment, onetag.Comment)
			cstatus = append(cstatus, tagStatus)
		}
		utils.PrintFmt(ctag, cupdatetime, ccomment, cstatus)
	}
}

func getTagStatus(reponame, itemname, tagname string) (string, error) {
	uri := "/daemon/" + reponame + "/" + itemname + "/" + tagname
	resp, err := commToDaemon("get", uri, nil)
	if err != nil {
		return "", err
	}
	result := ds.Result{}
	tagStatus := ds.TagStatus{}
	result.Data = &tagStatus
	respbody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode == http.StatusOK {
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			return "", err
		}
	} else if resp.StatusCode == http.StatusUnauthorized {
		return "", errors.New("Not login.")
	} else {
		return "", errors.New(string(respbody))
	}

	return tagStatus.Status, nil
}

func getTagStatusOfItem(reponame, itemname string) ([]string, error) {
	uri := "/daemon/" + reponame + "/" + itemname
	resp, err := commToDaemon("get", uri, nil)
	if err != nil {
		return nil, err
	}
	result := ds.Result{}
	tagStatus := ds.TagStatus{}
	result.Data = &tagStatus
	respbody, err := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK {
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			return nil, err
		}
	} else if resp.StatusCode == http.StatusUnauthorized {
		return nil, errors.New("Not login.")
	} else {
		return nil, errors.New(string(respbody))
	}
	return tagStatus.Results, nil
}

func judgeTag(abnormalTags []string, tag string) string {
	//fmt.Println(abnormalTags, tag)
	flag := true
	for _, abnormalTag := range abnormalTags {
		if abnormalTag == tag && flag {
			flag = false
			break
		}
	}
	if flag {
		return "NORMAL"
	}

	return "ABNORMAL"
}
