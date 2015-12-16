package daemon

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	//"io"
	"crypto/md5"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

var SampleFiles = []string{"sample.md", "Sample.md", "SAMPLE.MD", "sample.MD", "SAMPLE.md"}
var MetaFiles = []string{"meta.md", "Meta.md", "META.MD", "meta.MD", "META.md"}

type Sys struct {
	Supplystyle string `json:"supply_style"`
}
type Label struct {
	Ssys Sys `json:"sys"`
}
type ic struct {
	AccessType string `json:"itemaccesstype"`
	Comment    string `json:"comment"`
	Meta       string `json:"meta,omitempty"`
	Sample     string `json:"sample,omitempty"`
	Slabel     Label  `json:"label"`
}

func pubItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path, "(pub dataitem)")
	repo := ps.ByName("repo")
	item := ps.ByName("item")
	reqBody, _ := ioutil.ReadAll(r.Body)
	pub := ds.PubPara{}
	if err := json.Unmarshal(reqBody, &pub); err != nil {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, "pub dataitem error while unmarshal reqBody")
		return
	}
	if CheckDataPoolExist(pub.Datapool) == false {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal,
			fmt.Sprintf("datapool %s not exist, please check.", pub.Datapool))
		return
	}

	meta, sample := GetMetaAndSample(pub.Datapool, pub.ItemDesc)

	icpub := ic{AccessType: pub.Accesstype,
		Comment: pub.Comment,
		Meta:    meta,
		Sample:  sample}
	isys := Sys{Supplystyle: "batch"}
	icpub.Slabel = Label{Ssys: isys}

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
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s := "pub dataitem service unavailable"
		HttpNoData(w, http.StatusServiceUnavailable, cmd.ErrorServiceUnavailable, s)
		return
	}
	defer resp.Body.Close()

	//Get server result
	rbody, _ := ioutil.ReadAll(resp.Body)
	log.Println(resp.StatusCode, string(rbody))

	if resp.StatusCode == 200 {
		err := MkdirForDataItem(repo, item, pub.Datapool, pub.ItemDesc)
		if err != nil {
			RollBackItem(repo, item)
			HttpNoData(w, http.StatusBadRequest, cmd.ErrorInsertItem,
				fmt.Sprintf("Mkdir error! %s", err.Error()))
		} else {
			err = InsertItemToDb(repo, item, pub.Datapool, pub.ItemDesc)
			if err != nil {
				RollBackItem(repo, item)
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorInsertItem,
					"Insert dataitem to datapool error, please check it immediately!")
			} else {
				HttpNoData(w, http.StatusOK, cmd.ResultOK, "OK")
			}
		}
	} else {

		result := ds.Result{}
		err = json.Unmarshal(rbody, &result)
		if err != nil {
			s := "pub dataitem error while unmarshal server response"
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

	reqBody, _ := ioutil.ReadAll(r.Body)
	pub := ds.PubPara{}
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
	fmt.Println("repo", repo, "item", item, "tag", tag)

	//get DpFullPath and check whether repo/dataitem has been published
	DpItemFullPath, err := CheckTagAndGetDpPath(repo, item, tag)
	if err != nil || len(DpItemFullPath) == 0 {
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorUnmarshal, err.Error()+"  Datapool+Itemdesc Path: "+DpItemFullPath)
		return
	}
	splits := strings.Split(pub.Detail, "/")
	FileName := splits[len(splits)-1]
	DestFullPathFileName := DpItemFullPath + "/" + FileName

	if isFileExists(DestFullPathFileName) == false {
		errlog := fmt.Sprintf("%s is not found, please ensure %s is in dir:%s", DestFullPathFileName, FileName, DpItemFullPath)
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
	}

	body, err := json.Marshal(&struct {
		Commnet string `json:"comment"`
	}{
		pub.Comment})
	if err != nil {
		s := "pub tag error while marshal struct"
		log.Println(s)
		HttpNoData(w, http.StatusBadRequest, cmd.ErrorMarshal, s)
		return
	}

	log.Println("daemon: connecting to ", DefaultServer+r.URL.Path)
	req, err := http.NewRequest("POST", DefaultServer+r.URL.Path, bytes.NewBuffer(body))
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s := "pub tag service unavailable"
		HttpNoData(w, http.StatusServiceUnavailable, cmd.ErrorServiceUnavailable, s)
		return
	}
	defer resp.Body.Close()

	//Get server result
	rbody, _ := ioutil.ReadAll(resp.Body)
	log.Println(resp.StatusCode, string(rbody))

	if resp.StatusCode == 200 {
		/*if NeedCopy {
			if false == isDirExists(DestFullPath) {
				log.Println("mkdir ", DestFullPath)
				os.MkdirAll(DestFullPath, 0755)
			}
			count, err := CopyFile(pub.Detail, DestFullPathFileName)
			if err != nil {
				RollBackTag(repo, item, tag)
				HttpNoData(w, http.StatusBadRequest, cmd.ErrorInsertItem,
					fmt.Sprintf(" Copy file to datapool error, permission denied or path '%s' not exist! ", DestFullPath))
				return
			}
			log.Printf("Copy %d bytes from %s to %s", count, pub.Detail, DestFullPathFileName)
		}*/
		bmd5, err := ComputeMd5(DestFullPathFileName)
		if err != nil {
			log.Error(DestFullPathFileName, err, fmt.Sprintf("%x", bmd5))
		}
		err = InsertPubTagToDb(repo, item, tag, FileName)
		if err != nil {
			RollBackTag(repo, item, tag)
			HttpNoData(w, http.StatusBadRequest, cmd.ErrorInsertItem,
				"Insert dataitem to datapool error, please check it immediately!")
		} else {
			AddtoMonitor(DestFullPathFileName, repo+"/"+item+":"+tag)
			HttpNoData(w, http.StatusOK, cmd.ResultOK, "OK")
		}
	} else {

		result := ds.Result{}
		err = json.Unmarshal(rbody, &result)
		if err != nil {
			s := "pub dataitem error while unmarshal server response"
			log.Println(s)
			HttpNoData(w, resp.StatusCode, cmd.ErrorUnmarshal, s)
			return
		}
		log.Println(resp.StatusCode, result.Msg)
		HttpNoData(w, resp.StatusCode, result.Code, result.Msg)
	}

	return

}

func GetMetaAndSample(datapool, itemdesc string) (meta, sample string) {
	dpconn := GetDataPoolDpconn(datapool)
	if len(dpconn) == 0 || len(itemdesc) == 0 {
		l := log.Errorf("dpconn:%s or itemdesc:%s is empty", dpconn, itemdesc)
		logq.LogPutqueue(l)
		return
	}
	meta = GetMetaData(dpconn, itemdesc)
	sample = GetSampleData(dpconn, itemdesc)

	return
}

func GetMetaData(dpconn, itemdesc string) (meta string) {
	dirname := dpconn + "/" + itemdesc
	var filename string
	for _, v := range MetaFiles {
		filename = dirname + "/" + v
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

func GetSampleData(dpconn, itemdesc string) (sample string) {
	dirname := dpconn + "/" + itemdesc
	var filename string
	for _, v := range SampleFiles {
		filename = dirname + "/" + v
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
	d, err := os.Open(dirname) //ppen dir
	if err != nil {
		log.Println(err)
		return ""
	}
	defer d.Close()
	ff, _ := d.Readdir(10) //  return []fileinfo
	for i, fi := range ff {
		log.Printf("sample filename %d: %+v\n", i, fi.Name())
		filename = strings.ToLower(fi.Name())
		if filename != "sample.md" && filename != "meta.md" {
			f, err := os.Open(dirname + "/" + fi.Name())
			log.Println("filename:", dirname+"/"+fi.Name())
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

func MkdirForDataItem(repo, item, datapool, itemdesc string) (err error) {
	dpconn := GetDataPoolDpconn(datapool)
	if len(dpconn) != 0 {
		err = os.MkdirAll(dpconn+"/"+itemdesc, 0777)
		log.Println(dpconn + "/" + itemdesc)
		return err
	} else {
		return errors.New(fmt.Sprintf("dpconn is not found for datapool %s", datapool))
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

func CheckTagAndGetDpPath(repo, item, tag string) (dppath string, err error) {
	exist, err := CheckTagExist(repo, item, tag)
	if err != nil {
		return "", err
	}
	if exist == true {
		return "", errors.New("Tag already exist.")
	}
	dpname, dpconn, ItemDesc := GetDpnameDpconnItemdesc(repo, item)
	if len(dpname) == 0 || len(dpconn) == 0 || len(ItemDesc) == 0 {
		log.Println("dpname, dpconn, ItemDesc: ", dpname, dpconn, ItemDesc)
		return "", errors.New("dpname or dpconn not found.")
	}
	dppath = dpconn + "/" + ItemDesc
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
	req, err := http.NewRequest("DELETE", DefaultServer+uri, nil)
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
