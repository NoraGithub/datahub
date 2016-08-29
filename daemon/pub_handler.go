package daemon

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/daemon/dpdriver"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var SampleFiles = []string{"sample.md", "Sample.md", "SAMPLE.MD", "sample.MD", "SAMPLE.md"}
var MetaFiles = []string{"meta.md", "Meta.md", "META.MD", "meta.MD", "META.md"}
var PriceFile = "price.cfg"
var MaxRepoLength = 52
var MaxItemLength = 100
var MaxTagLength = 100
var MaxCommentLength = 600

type Sys struct {
	Supplystyle string `json:"supply_style"`
}
type Label struct {
	Ssys Sys `json:"sys"`
}
type ic struct {
	AccessType  string      `json:"itemaccesstype"`
	Comment     string      `json:"comment"`
	Meta        string      `json:"meta,omitempty"`
	Sample      string      `json:"sample,omitempty"`
	Slabel      Label       `json:"label"`
	PricePlans  []PricePlan `json:"price,omitempty"`
	Ch_itemname string      `json:"ch_itemname"`
}

type PricePlan struct {
	Time   int     `json:"time, omitempty"`
	Times  int     `json:"times, omitempty"`
	Unit   string  `json:"unit, omitempty"`
	Units  string  `json:"units, omitempty"`
	Money  float64 `json:"money, omitempty"`
	Expire int     `json:"expire, omitempty"`
}

func pubItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(pub dataitem)")
	repo := ps.ByName("repo")
	item := ps.ByName("item")

	if CheckLength(w, repo, MaxRepoLength) == false {
		return
	}

	if CheckLength(w, item, MaxItemLength) == false {
		return
	}

	if len(loginAuthStr) == 0 {
		HttpNoData(w, http.StatusUnauthorized, cmd.ErrorUnAuthorization, "Unlogin")
		return
	}

	pub := ds.PubPara{}
	pub.ItemDesc = strings.Trim(pub.ItemDesc, "/")
	if strings.Contains(pub.ItemDesc, "/") == true {
		log.Println("The path of item can't contain '/'.", pub.ItemDesc)
		w.Write([]byte(`{"msg":"The path of item can't contain '/'."}`))
		return
	}
	if CheckLength(w, pub.Comment, MaxCommentLength) == false {
		return
	}

	reqBody, _ := ioutil.ReadAll(r.Body)
	if err := json.Unmarshal(reqBody, &pub); err != nil {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "pub dataitem error while unmarshal reqBody")
		return
	}

	if CheckDataPoolExist(pub.Datapool) == false {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal,
			fmt.Sprintf("Datapool %s not found", pub.Datapool))
		return
	}

	//ToDO  check item dir exist

	priceplans := []PricePlan{}

	meta, sample, priceplans := GetMetaAndSampleAndPricePlan(pub.Datapool, pub.ItemDesc)
	icpub := ic{AccessType: pub.Accesstype,
		Comment:     pub.Comment,
		Meta:        meta,
		Sample:      sample,
		Ch_itemname: pub.Ch_itemname}

	//{"itemaccesstype":"private","comment":"","meta":"  ","label":{"sys":{"supply_style":"batch"}}}
	isys := Sys{Supplystyle: pub.SupplyStyle}
	icpub.Slabel = Label{Ssys: isys}
	if len(priceplans) > 0 {
		log.Info(priceplans)
		icpub.PricePlans = priceplans
	}

	body, err := json.Marshal(icpub)
	if err != nil {
		s := "pub dataitem error while marshal icpub struct"
		log.Println(s)
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorMarshal, s)
		return
	}
	log.Println(string(body))

	log.Println("daemon: connecting to", DefaultServer+r.URL.Path)
	req, err := http.NewRequest("POST", DefaultServer+r.URL.Path, bytes.NewBuffer(body))

	req.Header.Set("Authorization", loginAuthStr)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s := "Pub dataitem service unavailable"
		HttpNoData(w, http.StatusServiceUnavailable, cmd.ErrorServiceUnavailable, s)
		return
	}
	defer resp.Body.Close()
	//Get server result
	rbody, _ := ioutil.ReadAll(resp.Body)
	log.Println(resp.StatusCode, string(rbody))

	if resp.StatusCode == http.StatusOK {
		err := MkdirForDataItem(repo, item, pub.Datapool, pub.ItemDesc)
		if err != nil {
			RollBackItem(repo, item)
			HttpNoData(w, http.StatusBadRequest, cmd.ErrorInsertItem,
				fmt.Sprintf("Mkdir error! %s", err.Error()))
		} else {
			createTime := time.Now().String()
			err = InsertItemToDb(repo, item, pub.Datapool, pub.ItemDesc, createTime)
			if err != nil {
				RollBackItem(repo, item)
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorInsertItem,
					"Insert dataitem to datapool error, please check it immediately!")
			} else {
				HttpNoData(w, http.StatusOK, cmd.ResultOK, "OK")

				g_DaemonRole = PUBLISHER
			}
		}
	} else {

		result := ds.Result{}
		err = json.Unmarshal(rbody, &result)
		if err != nil {
			s := "Pub dataitem error while unmarshal server response"
			log.Println(s, err)
			HttpNoData(w, resp.StatusCode, cmd.ErrorUnmarshal, s)
			return
		}
		log.Println(resp.StatusCode, result.Msg)
		HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
	}

	return

}

func pubTagHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(pub tag)")

	pub := ds.PubPara{}
	if CheckLength(w, pub.Comment, MaxCommentLength) == false {
		return
	}

	reqBody, _ := ioutil.ReadAll(r.Body)
	if err := json.Unmarshal(reqBody, &pub); err != nil {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "pub tag error while unmarshal reqBody")
		return
	}
	if len(pub.Detail) == 0 {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "tag detail is not found")
		return
	}

	repo := ps.ByName("repo")
	item := ps.ByName("item")
	tag := ps.ByName("tag")
	log.Println("repo", repo, "item", item, "tag", tag)

	if !CheckLength(w, repo, MaxRepoLength) || !CheckLength(w, item, MaxItemLength) || !CheckLength(w, tag, MaxTagLength) {
		return
	}

	//get DpFullPath and check whether repo/dataitem has been published
	dpconn, dptype, itemDesc, err := CheckTagAndGetDpPath(repo, item, tag)
	log.Println("CheckTagAndGetDpPath ret:", dpconn, dptype, itemDesc)
	if err != nil {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorTagAlreadyExist, err.Error())
		return
	}
	splits := strings.Split(pub.Detail, "/")
	fileName := splits[len(splits)-1]

	//DpItemFullPath := dpconn + "/" + itemDesc
	//DestFullPathFileName := DpItemFullPath + "/" + fileName

	datapool, err := dpdriver.New(dptype)
	if err != nil {
		l := log.Error(err.Error())
		logq.LogPutqueue(l)
		HttpNoData(w, http.StatusInternalServerError, cmd.ErrorDatapoolNotExits, err.Error())
		return
	}

	exist, size, errc := datapool.CheckDataAndGetSize(dpconn, itemDesc, fileName)
	if errc != nil && exist == false {
		//errlog := fmt.Sprintf("File %v not found", DestFullPathFileName)
		l := log.Error(errc.Error())
		logq.LogPutqueue(l)
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorFileNotExist, errc.Error())
		return
	}
	/*if isFileExists(DestFullPathFileName) == false {
		errlog := fmt.Sprintf("File %v not found", DestFullPathFileName)
		l := log.Error(errlog)
		logq.LogPutqueue(l)
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorFileNotExist, errlog)
		return
	}

	if size, err := GetFileSize(DestFullPathFileName); err != nil {
		l := log.Errorf("Get %s size error, %v", DestFullPathFileName, err)
		logq.LogPutqueue(l)
	} else {
		pub.Comment += SizeToStr(size)
		//fmt.Sprintf(" Size:%v ", size)
	}*/

	if size > 0 {
		pub.Comment += SizeToStr(size)
	}

	body, e := json.Marshal(&struct {
		Comment string `json:"comment"`
	}{pub.Comment})

	if e != nil {
		s := "Pub tag error while marshal struct"
		log.Println(s)
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorMarshal, s)
		return
	}

	err = InsertPubTagToDb(repo, item, tag, fileName, pub.Comment)

	if err != nil {
		log.Error("Insert tag to db error.")
		return
	}
	log.Println("daemon: connecting to ", DefaultServer+r.URL.Path)
	req, err := http.NewRequest("POST", DefaultServer+r.URL.Path, bytes.NewBuffer(body))
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s := "Pub tag service unavailable"
		HttpNoData(w, http.StatusServiceUnavailable, cmd.ErrorServiceUnavailable, s)
		return
	}
	defer resp.Body.Close()

	//Get server result
	rbody, _ := ioutil.ReadAll(resp.Body)
	log.Println(resp.StatusCode, string(rbody))

	if resp.StatusCode == http.StatusOK {
		if dptype == "file" {
			//AddtoMonitor(DestFullPathFileName, repo+"/"+item+":"+tag) //do not monitor temporarily
		}
		HttpNoData(w, http.StatusOK, cmd.ResultOK, "OK")

		g_DaemonRole = PUBLISHER
	} else {

		err = rollbackInsertPubTagToDb(repo, item, tag)
		if err != nil {
			log.Error("rollbackInsertPubTagToDb error :", err)
			return
		}

		result := ds.Result{}
		err = json.Unmarshal(rbody, &result)
		if err != nil {
			s := "Pub dataitem error while unmarshal server response"
			log.Println(s)
			HttpNoData(w, resp.StatusCode, cmd.ErrorUnmarshal, s)
			return
		}
		log.Println(resp.StatusCode, result.Msg)
		HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
	}

	return

}

func newPubTagHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	log.Println(r.URL.Path, "(pub tag)")

	repo := ps.ByName("repo")
	item := ps.ByName("item")
	tag := ps.ByName("tag")
	log.Println("repo", repo, "item", item, "tag", tag) //log

	paras := ds.PubPara{}

	reqbody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("ioutil.ReadAll err:", err)
		JsonResult(w, http.StatusBadRequest, cmd.ErrorIOUtil, "Internal Error.", nil)
		return
	}
	err = json.Unmarshal(reqbody, &paras)
	if err != nil {
		log.Println("json.Unmarshal err:", err)
		JsonResult(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "Internal Error.", nil)
		return
	}

	if !CheckLength(w, repo, MaxRepoLength) || !CheckLength(w, item, MaxItemLength) || !CheckLength(w, tag, MaxTagLength) {
		JsonResult(w, http.StatusOK, cmd.ErrorOutMaxLength, "repo or item or tag length out of max length.", nil)
		return
	}

	isExist, err := CheckItemExist(repo, item)
	if isExist == false {
		if paras.Datapool != "" && paras.ItemDesc != "" {
			isExist := CheckDataPoolExist(paras.Datapool)
			if isExist == false {
				JsonResult(w, http.StatusOK, cmd.ErrorDatapoolNotExits, "datapool not exist.", nil)
				return
			}
			url := DefaultServer + "/api/repositories/" + repo + "/" + item
			log.Println("daemon: connecting to ", url) //log

			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Println("http.NewRequest err:", err)
				JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Println("http.DefaultClient.Do err:", err)
				JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
				return
			}
			defer resp.Body.Close()

			respbody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
				return
			}

			result := ds.Result{}
			itemInfo := ds.ItemInfo{}
			result.Data = &itemInfo
			if resp.StatusCode == http.StatusOK {

				err = json.Unmarshal(respbody, &result)
				if err != nil {
					log.Println("json.Unmarshal err:", err)
					JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
					return
				}
			} else {
				log.Println(string(respbody))
				err = json.Unmarshal(respbody, &result)
				if err != nil {
					log.Println("json.Unmarshal err:", err)
					JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
					return
				}
				JsonResult(w, resp.StatusCode, result.Code, result.Msg, nil)
				return
			}

			err = InsertItemToDb(repo, item, paras.Datapool, paras.ItemDesc, itemInfo.Optime)
			if err != nil {
				log.Println("InsertItemToDb err:", err)
				JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
				return
			}
		} else {
			JsonResult(w, http.StatusOK, cmd.ErrorItemNotExist, "item not been published.", nil)
			return
		}
	}

	//-------------------------------------------这是分割线-----------------------------------------------------

	dpconn, dptype, itemDesc, err := CheckTagAndGetDpPath(repo, item, tag)
	log.Println("CheckTagAndGetDpPath ret:", dpconn, dptype, itemDesc) //log
	if err != nil {
		log.Println("CheckTagAndGetDpPath err:", err)
		//rollbackInsertPubItem(w, repo, item)
		msg := fmt.Sprintf("Tag '%s' already exist.", tag)
		JsonResult(w, http.StatusOK, cmd.ErrorTagAlreadyExist, msg, nil)
		return
	}
	splits := strings.Split(paras.Detail, "/")
	fileName := splits[len(splits)-1]

	datapool, err := dpdriver.New(dptype)
	if err != nil {
		l := log.Error(err.Error())
		logq.LogPutqueue(l)
		//rollbackInsertPubItem(w, repo, item)
		JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
		return
	}

	exist, size, err := datapool.CheckDataAndGetSize(dpconn, itemDesc, fileName)
	if err != nil && exist == false {
		l := log.Error(err.Error())
		logq.LogPutqueue(l)
		//rollbackInsertPubItem(w, repo, item)
		JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
		return
	}

	if size > 0 {
		paras.Comment += SizeToStr(size)
	}

	body, e := json.Marshal(&struct {
		Comment string `json:"comment"`
	}{paras.Comment})
	if e != nil {
		s := "Pub tag error while marshal struct"
		log.Println(s)
		//rollbackInsertPubItem(w, repo, item)
		JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
		//fmt.Println("Marshal err:", err)
		return
	}

	url := DefaultServer + "/api/repositories/" + repo + "/" + item + "/" + tag
	log.Println("daemon: connecting to ", url) //log
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("DefaultClient.Do err:", err)
		//rollbackInsertPubItem(w, repo, item)
		JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
		return
	}
	defer resp.Body.Close()

	//Get server result
	result := ds.Result{}
	respbody, _ := ioutil.ReadAll(resp.Body)
	log.Println(resp.StatusCode, string(respbody)) //log

	if resp.StatusCode == http.StatusOK {
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			log.Println("Unmarshal err:", err)
			//rollbackInsertPubItem(w, repo, item)
			JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
			return
		}

		err = InsertPubTagToDb(repo, item, tag, fileName, paras.Comment)
		if err != nil {
			l := log.Error("Insert tag to db error.")
			logq.LogPutqueue(l)
			//rollbackInsertPubItem(w, repo, item)
			JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
			return
		}

		JsonResult(w, resp.StatusCode, result.Code, result.Msg, nil)

		g_DaemonRole = PUBLISHER
	} else if resp.StatusCode == http.StatusUnauthorized {

		JsonResult(w, http.StatusUnauthorized, cmd.ErrorUnAuthorization, "no login.", nil)

		//rollbackInsertPubItem(w, repo, item)
		return

	} else {
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			log.Println("Unmarshal err:", err)
			//rollbackInsertPubItem(w, repo, item)
			JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
			return
		}
		log.Println(string(respbody))
		//rollbackInsertPubItem(w, repo, item)
		JsonResult(w, resp.StatusCode, result.Code, result.Msg, nil)
	}

	return
}

func GetMetaAndSampleAndPricePlan(dpname, itemdesc string) (meta, sample string, plans []PricePlan) {
	dpconn := GetDataPoolDpconn(dpname)
	if len(dpconn) == 0 || len(itemdesc) == 0 {
		l := log.Errorf("dpconn:%s or itemdesc:%s is empty", dpconn, itemdesc)
		logq.LogPutqueue(l)
		return
	}

	path := dpconn + "/" + itemdesc

	meta = GetMetaData(path)
	sample = GetSampleData(path)
	plans = GetPricePlan(path)
	log.Debug(plans)

	return
}

func GetPricePlan(path string) (plans []PricePlan) {
	config := path + "/" + PriceFile
	if isFileExists(config) == true {
		bytes, err := ioutil.ReadFile(config)
		if err != nil {
			log.Error(err)
			return
		}
		log.Debug(string(bytes))
		type LPrices struct {
			PricePlans []PricePlan `json:"price,omitempty"`
		}
		struPrices := LPrices{}
		if err = json.Unmarshal(bytes, &struPrices); err != nil {
			log.Error(err)
			return
		}
		log.Debug(struPrices)
		plans = struPrices.PricePlans
	}
	return
}

func GetMetaData(itempath string) (meta string) {
	var filename string
	for _, v := range MetaFiles {
		filename = itempath + "/" + v
		if isFileExists(filename) == true {
			if bytes, err := ioutil.ReadFile(filename); err == nil {
				meta = string(bytes)
				return meta
			} else {
				l := log.Error(err)
				logq.LogPutqueue(l)
				return " "
			}
		}
	}
	return "  "
}

func GetSampleData(itempath string) (sample string) {
	var filename string
	for _, v := range SampleFiles {
		filename = itempath + "/" + v
		if isFileExists(filename) == true {
			if bytes, err := ioutil.ReadFile(filename); err == nil {
				sample = string(bytes)
				return sample
			} else {
				l := log.Error(err)
				logq.LogPutqueue(l)
			}
		}
	}
	d, err := os.Open(itempath) //ppen dir
	if err != nil {
		log.Println(err)
		return ""
	}
	defer d.Close()
	ff, _ := d.Readdir(10) //  return []fileinfo
	for i, fi := range ff {
		log.Printf("sample filename %d: %+v\n", i, fi.Name())
		filename = strings.ToLower(fi.Name())
		if filename != "sample.md" && filename != "meta.md" && filename != PriceFile {
			f, err := os.Open(itempath + "/" + fi.Name())
			log.Println("filename:", itempath+"/"+fi.Name())
			if err != nil {
				continue
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			scanner.Split(bufio.ScanLines)
			var i = 0
			for scanner.Scan() {
				if i > 9 {
					break
				}
				i++
				sample += scanner.Text() + "  \n" //md "  \n" is a new line
				//log.Println(scanner.Text())
			}
			break
		}
	}
	log.Println("sample data:", sample)
	//need lenth check
	return sample
}

func HttpNoData(w http.ResponseWriter, httpcode, errorcode int, msg string) {
	w.WriteHeader(httpcode)
	respbody, _ := json.Marshal(&struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}{
		errorcode,
		msg})
	w.Write(respbody)
}

func MkdirForDataItem(repo, item, dpname, itemdesc string) (err error) {
	dpconn, dptype := GetDataPoolDpconnAndDptype(dpname)
	if len(dpconn) != 0 {
		datapoolOpt, e := dpdriver.New(dptype)
		if e != nil {
			return e
		}
		err = datapoolOpt.CheckItemLocation(dpname, dpconn, itemdesc)

		return err
	} else {
		return errors.New(fmt.Sprintf("dpconn is not found for datapool %s", dpname))
	}
	return nil
}

func RollBackItem(repo, item string) {
	//Delete /repository/repo/item
	log.Println(repo, "/", item)
	err := DeleteItemOrTag(repo, item, "")
	if err != nil {
		log.Println("DeleteItem err ", err.Error())
	}
}

func CheckTagAndGetDpPath(repo, item, tag string) (dpconn, dptype, itemDesc string, err error) {
	exist, err := CheckTagExist(repo, item, tag)
	if err != nil {
		return "", "", "", err
	}
	if exist == true {
		return "", "", "", errors.New("Tag already exist.")
	}
	var dpname string
	dpname, dpconn, dptype, itemDesc = GetDpnameDpconnItemdesc(repo, item)
	if len(dpname) == 0 || len(dpconn) == 0 || len(dptype) == 0 {
		log.Println("dpname, dpconn,dptype itemDesc:", dpname, dpconn, dptype, itemDesc)
		return "", "", "", errors.New(fmt.Sprintf("Datapool %v not found.", dpname))
	} else if len(itemDesc) == 0 {
		log.Println("dpname, dpconn:", dpname, dpconn)
		return "", "", "", errors.New(fmt.Sprintf("Dataitem %v/%v not found.", repo, item))
	}
	//dppath = dpconn + "/" + itemDesc
	return
}

func RollBackTag(repo, item, tag string) {
	//Delete /repository/repo/item tag
	log.Println(repo, "/", item, ":", tag)
	err := DeleteItemOrTag(repo, item, tag)
	if err != nil {
		log.Println("DeleteTag err ", err.Error())
	}
}

/*func CopyFile(src, des string) (w int64, err error) {
	srcFile, err := os.Open(src)
	if err != nil {
		log.Println(err)
	}
	defer srcFile.Close()

	desFile, err := os.Create(des)
	if err != nil {
		log.Println(err)
	}
	defer desFile.Close()

	return io.Copy(desFile, srcFile)
}*/

func DeleteItemOrTag(repo, item, tag string) (err error) {
	uri := "/repositories/"
	if len(tag) == 0 {
		uri = uri + repo + "/" + item
	} else {
		uri = uri + repo + "/" + item + "/" + tag
	}
	log.Println(uri)
	req, err := http.NewRequest("DELETE", DefaultServerAPI+uri, nil)
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}
	if err != nil {
		return err
	}
	//req.Header.Set("User", "admin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println(resp.StatusCode, string(body))
		return errors.New(fmt.Sprintf("%d", resp.StatusCode))
	}
	return err
}

func GetFileSize(file string) (size int64, e error) {
	f, e := os.Stat(file)
	if e != nil {
		return 0, e
	}
	return f.Size(), nil
}

func SizeToStr(size int64) (s string) {
	if size < 0 {
		return ""
	}
	if size < 1024 {
		s = fmt.Sprintf(" Size:%v Bytes", size)
	} else if size >= 1024 && size < 1024*1024 {
		s = fmt.Sprintf(" Size:%.2f KB", float64(size)/1024)
	} else if size >= 1024*1024 && size < 1024*1024*1024 {
		s = fmt.Sprintf(" Size:%.2f MB", float64(size)/(1024*1024))
	} else if size >= 1024*1024*1024 && size < 1024*1024*1024*1024 {
		s = fmt.Sprintf(" Size:%.2f GB", float64(size)/(1024*1024*1024))
	} else if size >= 1024*1024*1024*1024 && size < 1024*1024*1024*1024*1024 {
		s = fmt.Sprintf(" Size:%.2f TB", float64(size)/(1024*1024*1024*1024))
	}
	return s
}

func ComputeMd5(filePath string) ([]byte, error) {
	var result []byte
	file, err := os.Open(filePath)
	if err != nil {
		return result, err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return result, err
	}

	return hash.Sum(result), nil
}

func CheckLength(w http.ResponseWriter, data string, length int) bool {
	if len(data) > length {
		m := fmt.Sprintf("length of %s can't over %d bytes", data, length)
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorOverLength, m)
		log.Warn(m)
		return false
	}

	return true
}

func rollbackInsertPubItem(w http.ResponseWriter, repo, item string) {
	err := rollbackInsertPubItemToDb(repo, item)
	if err != nil {
		l := log.Error("rollbackInsertPubTagToDb error :", err)
		logq.LogPutqueue(l)
		JsonResult(w, http.StatusBadRequest, cmd.InternalError, "Internal Error.", nil)
		return
	}
	return
}
