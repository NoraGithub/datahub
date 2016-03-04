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
		fmt.Println("invalid argument..")
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
			panic(err)
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
			}
		} else {
			fmt.Println("The parameter after repo is in wrong format!")
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
			fmt.Println("http StatusCode:", resp.StatusCode, "Json format error!")
			return err
		}
		if result.Code == 1400 {
			if err = Login(false, nil); err == nil {
				err = Repo(login, args)
			} else {
				fmt.Println(err)
			}
		} else {
			fmt.Printf("ERROR[%v] %v\n", result.Code, result.Msg)
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
		n, _ := fmt.Printf("%-16s\n", "REPOSITORY") //, "UPDATETIME", "COMMENT")
		printDash(n + 2)
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
		n, _ := fmt.Printf("REPOSITORY/DATAITEM\n")
		printDash(n + 12)
		for _, v := range onerepo.DataItems {
			fmt.Printf("%s/%s\n", repo, v)
		}

	} else if icmd == ReposReponameDataItem {
		repoitemtags := ds.Data{}
		result.Data = &repoitemtags
		err := json.Unmarshal(respbody, &result)
		if err != nil {
			panic(err)
		}
		n, _ := fmt.Printf("%s\t%s\t%s\n", "REPOSITORY/ITEM:TAG", "UPDATETIME", "COMMENT")
		printDash(n + 12)
		for _, v := range repoitemtags.Taglist {
			fmt.Printf("%s/%s:%s\t%s\t%s\n", repo, item, v.Tag, v.Optime, v.Comment)
		}
	} else if icmd == ReposReponameDataItemTag {
		onetag := ds.Tag{}
		result.Data = &onetag
		err := json.Unmarshal(respbody, &result)
		if err != nil {
			panic(err)
		}
		n, _ := fmt.Printf("%s\t%s\t%s\n", "REPOSITORY/ITEM:TAG", "UPDATETIME", "COMMENT")
		printDash(n + 12)
		fmt.Printf("%s/%s:%s\t%s\t%s\n", repo, item, tag, onetag.Optime, onetag.Comment)
	}
}

