package daemon

import (
	"container/list"
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
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DECIMAL_BASE = 10
const INT_SIZE_64 = 64

type AccessToken struct {
	Accesstoken   string `json:"accesstoken,omitempty"`
	Remainingtime string `json:"remainingtime,omitempty"`
	Entrypoint    string `json:"entrypoint,omitempty"`
}

var strret string

var (
	AutomaticPullList = list.New()
	pullmu            sync.Mutex
)

func pullHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path + "(pull)")
	result, _ := ioutil.ReadAll(r.Body)
	p := ds.DsPull{}
	p.ItemDesc = strings.Trim(p.ItemDesc, "/")
	if strings.Contains(p.ItemDesc, "/") == true {
		log.Println("The path of item can't contain '/'.", p.ItemDesc)
		w.Write([]byte(`{"msg":"The path of item can't contain '/'."}`))
		return
	}

	if err := json.Unmarshal(result, &p); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	p.Repository = ps.ByName("repo")
	p.Dataitem = ps.ByName("item")

	dpexist := CheckDataPoolExist(p.Datapool)
	if dpexist == false {
		e := fmt.Sprintf("Datapool '%s' does not exist.", p.Datapool)
		l := log.Error("Code:", cmd.ErrorDatapoolNotExits, e)
		logq.LogPutqueue(l)
		msgret := ds.Result{Code: cmd.ErrorDatapoolNotExits, Msg: e}
		resp, _ := json.Marshal(msgret)
		w.WriteHeader(http.StatusBadRequest)
		w.Write(resp)
		return
	}

	alItemdesc, err := GetItemDesc(p.Repository, p.Dataitem)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	if len(alItemdesc) != 0 && p.ItemDesc != alItemdesc {
		p.ItemDesc = alItemdesc
		//TODO add log tishi
	} else if len(p.ItemDesc) == 0 && len(alItemdesc) == 0 {
		p.ItemDesc = p.Repository + "_" + p.Dataitem
	}

	if dpconn := GetDataPoolDpconn(p.Datapool); len(dpconn) == 0 {
		strret = p.Datapool + " not found. " + p.Tag + " will be pulled into " + g_strDpPath + "/" + p.ItemDesc
	} else {
		strret = p.Repository + "/" + p.Dataitem + ":" + p.Tag + " will be pulled soon and can be found in " + dpconn + "/" + p.ItemDesc + "/" + p.Tag
	}

	//add to automatic pull list
	if p.Automatic == true {
		if true == CheckExistInQueue(p) {
			strret = p.Repository + "/" + p.Dataitem + " is being pulled automatically."
		} else {
			AutomaticPullPutqueue(p)
			strret = p.Repository + "/" + p.Dataitem + " will be pulled automatically."
		}

		msgret := ds.MsgResp{Msg: strret}
		resp, _ := json.Marshal(msgret)
		w.WriteHeader(http.StatusOK)
		w.Write(resp)
		return
	}
	if p.CancelAutomatic == true {
		AutomaticPullRmqueue(p)
		strret = "Cancel the automatical pulling of " + p.Repository + "/" + p.Dataitem + "successfully."
		msgret := ds.MsgResp{Msg: strret}
		resp, _ := json.Marshal(msgret)
		w.WriteHeader(http.StatusOK)
		w.Write(resp)
		return
	}

	url := "/transaction/" + ps.ByName("repo") + "/" + ps.ByName("item") + "/" + p.Tag

	token, entrypoint, err := getAccessToken(url, w)
	if err != nil {
		log.Println(err)
		strret = err.Error()
		return
	} else {
		url = "/pull/" + ps.ByName("repo") + "/" + ps.ByName("item") + "/" + p.Tag +
			"?token=" + token + "&username=" + gstrUsername

		chn := make(chan int)
		go dl(url, entrypoint, p, w, chn)
		<-chn
	}

	log.Println(strret)

	return
}

func dl(uri, ip string, p ds.DsPull, w http.ResponseWriter, c chan int) error {

	if len(ip) == 0 {
		ip = "http://localhost:65535"
		//TODO return
	}

	target := ip + uri
	log.Println(target)
	n, err := download(target, p, w, c)
	if err != nil {
		log.Printf("[%d bytes returned.]\n", n)
		log.Println(err)
	}
	return err
}

/*download routine, supports resuming broken downloads.*/
func download(url string, p ds.DsPull, w http.ResponseWriter, c chan int) (int64, error) {
	log.Printf("we are going to download %s, save to dp=%s,name=%s\n", url, p.Datapool, p.DestName)

	var out *os.File
	var err error
	var destfilename, tmpdestfilename, tmpdir, dpconn, dptype string

	dpconn, dptype = GetDataPoolDpconnAndDptype(p.Datapool)
	if len(dpconn) == 0 {
		err = fmt.Errorf("dpconn is null! datapool:%s ", p.Datapool)
		return ErrLogAndResp(c, w, http.StatusBadRequest, cmd.ErrorNoRecord, err)
	}

	//New a datapool object
	datapool, err := dpdriver.New(dptype)
	if err != nil {
		return ErrLogAndResp(c, w, http.StatusInternalServerError, cmd.ErrorNoDatapoolDriver, err)
	}
	destfilename, tmpdir, tmpdestfilename = datapool.GetDestFileName(dpconn, p.ItemDesc, p.DestName)

	os.MkdirAll(tmpdir, 0777)

	log.Info("open tmp destfile name:", tmpdestfilename)
	out, err = os.OpenFile(tmpdestfilename, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return ErrLogAndResp(c, w, http.StatusInternalServerError, cmd.ErrorOpenFile, err)
	}

	stat, err := out.Stat()
	if err != nil {
		return ErrLogAndResp(c, w, http.StatusInternalServerError, cmd.ErrorStatFile, err)
	}
	out.Seek(stat.Size(), 0)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "go-downloader")
	/* Set download starting position with 'Range' in HTTP header*/
	req.Header.Set("Range", "bytes="+strconv.FormatInt(stat.Size(), 10)+"-")
	log.Printf("%v bytes had already been downloaded.\n", stat.Size())

	log.Debug(EnvDebug("http_proxy", false))

	resp, err := http.DefaultClient.Do(req)

	/*Save response body to file only when HTTP 2xx received. TODO*/
	if err != nil || (resp != nil && resp.StatusCode/100 != 2) {
		log.Error("http error", err)
		if resp != nil {
			body, _ := ioutil.ReadAll(resp.Body)
			l := log.Error("http status code:", resp.StatusCode, "response Body:", string(body), err)
			logq.LogPutqueue(l)
			struMsg := &ds.MsgResp{}
			json.Unmarshal(body, struMsg)
			msg := struMsg.Msg
			if resp.StatusCode == 416 {
				msg = tmpdestfilename + " has already been downloaded."
			}
			r, _ := buildResp(resp.StatusCode, msg, nil)

			w.WriteHeader(resp.StatusCode)
			w.Write(r)
		} else {
			HttpNoData(w, http.StatusInternalServerError, cmd.ErrorOtherError, err.Error())
		}
		filesize := stat.Size()
		out.Close()
		if filesize == 0 {
			os.Remove(tmpdestfilename)
		}
		c <- -1
		return 0, err
	}
	defer resp.Body.Close()

	HttpNoData(w, http.StatusOK, cmd.ResultOK, strret)

	//write channel
	c <- 1

	jobtag := p.Repository + "/" + p.Dataitem + ":" + p.Tag

	srcsize, err := strconv.ParseInt(resp.Header.Get("X-Source-FileSize"), DECIMAL_BASE, INT_SIZE_64)
	md5str := resp.Header.Get("X-Source-MD5")
	status := "downloading"
	log.Info("pull tag:", jobtag, tmpdestfilename, status, srcsize)
	jobid := putToJobQueue(jobtag, tmpdestfilename, status, srcsize)

	n, err := io.Copy(out, resp.Body)
	if err != nil {
		out.Close()
		bl := log.Error(err)
		logq.LogPutqueue(bl)
		dlsize, e := GetFileSize(tmpdestfilename)
		if e != nil {
			l := log.Error(e)
			logq.LogPutqueue(l)
		}
		status = "failed"
		updateJobQueue(jobid, status, dlsize)
		return 0, err
	}
	out.Close()

	status = "downloaded"

	if len(md5str) > 0 {
		bmd5, err := ComputeMd5(tmpdestfilename)
		bmd5str := fmt.Sprintf("%x", bmd5)
		log.Debug("md5", md5str, tmpdestfilename, bmd5str)
		if err != nil {
			log.Error(tmpdestfilename, err, bmd5)
		} else if md5str != bmd5str {
			l := log.Errorf("check md5 code error! src md5:%v,  local md5:%v", md5str, bmd5str)
			logq.LogPutqueue(l)
			status = "md5 error"
			updateJobQueue(jobid, status, 0)
			return n, nil
		}
	}
	log.Printf("%d bytes downloaded.", n)

	if err := MoveFromTmp(tmpdestfilename, destfilename); err != nil {
		status = "MoveFromTmp error"
	}

	dlsize, e := GetFileSize(destfilename)
	if e != nil {
		l := log.Error(e)
		logq.LogPutqueue(l)
	}

	status = datapool.StoreFile(status, destfilename, dpconn, p.Datapool, p.ItemDesc, p.DestName)
	updateJobQueue(jobid, status, dlsize)

	tagComment := GetTagComment(p.Repository, p.Dataitem, p.Tag)

	InsertTagToDb(true, p, tagComment)
	return n, nil
}

func GetTagComment(repo, item, tag string) string {
	path := "/api/repositories/" + repo + "/" + item + "/" + tag

	resp, err := commToServerGetRsp("get", path, nil)
	if err != nil {
		log.Error(err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		err = errors.New("unkown error")
		log.Error("GET", path, resp.StatusCode)
		return ""
	}
	result := ds.Response{}
	struComment := &struct {
		Comment string `json:"comment"`
	}{}
	result.Data = struComment

	respbody, _ := ioutil.ReadAll(resp.Body)
	log.Println(string(respbody))
	unmarshalerr := json.Unmarshal(respbody, &result)
	if unmarshalerr != nil {
		log.Error(unmarshalerr)
		return ""
	}
	log.Println(result)

	return struComment.Comment
}

func ErrLogAndResp(c chan int, w http.ResponseWriter, httpcode, errorcode int, err error) (int64, error) {
	l := log.Error(err)
	logq.LogPutqueue(l)
	c <- -1
	HttpNoData(w, http.StatusBadRequest, cmd.ErrorNoRecord, err.Error())
	return 0, err
}

func MoveFromTmp(src, dest string) (err error) {
	err = os.Rename(src, dest)
	if err != nil {
		l := log.Errorf("Rename %v to %v error. %v", src, dest, err)
		logq.LogPutqueue(l)
	}
	return err
}

func getAccessToken(url string, w http.ResponseWriter) (token, entrypoint string, err error) {

	log.Println("daemon: connecting to", DefaultServerAPI+url, "to get accesstoken")
	req, err := http.NewRequest("POST", DefaultServerAPI+url, nil)
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		//w.WriteHeader(http.StatusServiceUnavailable)
		return "", "", err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	log.Println(resp.StatusCode, string(body))
	if resp.StatusCode != http.StatusOK {

		w.WriteHeader(resp.StatusCode)
		w.Write(body)

		return "", "", errors.New(string(body))
	} else {
		t := AccessToken{}
		result := &ds.Result{Data: &t}
		if err = json.Unmarshal(body, result); err != nil {
			return "", "", err
		} else {
			if len(t.Accesstoken) > 0 {
				//w.WriteHeader(http.StatusOK)
				return t.Accesstoken, t.Entrypoint, nil
			}
		}
	}
	return "", "", errors.New("get access token error.")
}

func AutomaticPullPutqueue(p ds.DsPull) {
	pullmu.Lock()
	defer pullmu.Unlock()

	AutomaticPullList.PushBack(p)
}

func AutomaticPullRmqueue(p ds.DsPull) {
	pullmu.Lock()
	defer pullmu.Unlock()

	var next *list.Element
	for e := AutomaticPullList.Front(); e != nil; e = next {
		v := e.Value.(ds.DsPull)
		if v.Repository == p.Repository && v.Dataitem == p.Dataitem {
			AutomaticPullList.Remove(e)
			log.Info(v, "removed from the queue.")
			break
		} else {
			next = e.Next()
		}
	}
}

func CheckExistInQueue(p ds.DsPull) (exist bool) {
	exist = false
	for e := AutomaticPullList.Front(); e != nil; e = e.Next() {
		v := e.Value.(ds.DsPull)
		if v.Repository == p.Repository && v.Dataitem == p.Dataitem {
			exist = true
			return
		}
	}
	return
}

func PullTagAutomatic() {
	for {
		time.Sleep(30 * time.Second)

		//log.Debug("AutomaticPullList.Len()", AutomaticPullList.Len())
		var Tags map[int]string
		for e := AutomaticPullList.Front(); e != nil; e = e.Next() {
			v := e.Value.(ds.DsPull)
			log.Info("PullTagAutomatic begin", v.Repository, v.Dataitem)
			Tags = GetTagFromMsgTagadded(v.Repository, v.Dataitem, NOTREAD)

			log.Println("Tags ", Tags)
			go PullItemAutomatic(Tags, v)

		}
	}
}

func PullItemAutomatic(Tags map[int]string, v ds.DsPull) {
	var d ds.DsPull = v
	for id, tag := range Tags {
		var chn = make(chan int)
		d.Tag = tag
		d.DestName = d.Tag

		go PullOneTagAutomatic(d, chn)
		<-chn

		UpdateStatMsgTagadded(id, ALREADYREAD)
	}
}

func PullOneTagAutomatic(p ds.DsPull, c chan int) {
	var ret string
	var w *httptest.ResponseRecorder = httptest.NewRecorder()
	url := "/transaction/" + p.Repository + "/" + p.Dataitem + "/" + p.Tag

	token, entrypoint, err := getAccessToken(url, w)
	if err != nil {
		log.Println(err)
		ret = err.Error()
		return
	} else {
		url = "/pull/" + p.Repository + "/" + p.Dataitem + "/" + p.Tag +
			"?token=" + token + "&username=" + gstrUsername

		go dl(url, entrypoint, p, w, c)
	}

	log.Println(ret)
}
