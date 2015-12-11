package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

const DECIMAL_BASE = 10
const INT_SIZE_64 = 64

type AccessToken struct {
	Accesstoken   string `json:"accesstoken,omitempty"`
	Remainingtime string `json:"remainingtime,omitempty"`
	Entrypoint    string `json:"entrypoint,omitempty"`
}

var strret string

func pullHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Println(r.URL.Path + "(pull)")
	result, _ := ioutil.ReadAll(r.Body)
	p := ds.DsPull{}

	if err := json.Unmarshal(result, &p); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	p.Repository = ps.ByName("repo")
	p.Dataitem = ps.ByName("item")

	dpexist := CheckDataPoolExist(p.Datapool)
	if dpexist == false {
		e := fmt.Sprintf("Datapool:%s not exist!", p.Datapool)
		l := log.Error(e)
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
	} else if len(p.ItemDesc) == 0 && len(alItemdesc) == 0 {
		p.ItemDesc = p.Repository + "_" + p.Dataitem
	}

	if dpconn := GetDataPoolDpconn(p.Datapool); len(dpconn) == 0 {
		strret = p.Datapool + " not found. " + p.Tag + " will be pull into " + g_strDpPath + "/" + p.ItemDesc
	} else {
		strret = p.Repository + "/" + p.Dataitem + ":" + p.Tag + " will be pull into " + dpconn + "/" + p.ItemDesc
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
	/*
		msgret := ds.MsgResp{Msg: strret}
		resp, _ := json.Marshal(msgret)
		w.WriteHeader(http.StatusOK)
		w.Write(resp)
	*/

	return

}

func dl(uri, ip string, p ds.DsPull, w http.ResponseWriter, c chan int) error {

	if len(ip) == 0 {
		ip = "http://localhost:65535"
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
	var destfilename string
	dpexist := CheckDataPoolExist(p.Datapool)
	if dpexist == false {
		//os.MkdirAll(g_strDpPath+"/"+p.ItemDesc, 0777)
		//destfilename = g_strDpPath + "/" + p.ItemDesc + "/" + p.DestName
		e := fmt.Sprintf("datapool:%s not exist!", p.Datapool)
		l := log.Error(e)
		logq.LogPutqueue(l)
		err = errors.New(e)
		return 0, err
	} else {
		dpconn := GetDataPoolDpconn(p.Datapool)
		if len(dpconn) == 0 {
			//os.MkdirAll(g_strDpPath+"/"+p.ItemDesc, 0777)
			//destfilename = g_strDpPath + "/" + p.ItemDesc + "/" + p.DestName
			e := fmt.Sprintf("dpconn is null! datapool:%s ", p.Datapool)
			l := log.Error(e)
			logq.LogPutqueue(l)
			err = errors.New(e)
			return 0, err
		} else {
			os.MkdirAll(dpconn+"/"+p.ItemDesc, 0777)
			destfilename = dpconn + "/" + p.ItemDesc + "/" + p.DestName
		}
	}
	log.Println("destfilename:", destfilename)
	out, err = os.OpenFile(destfilename, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return 0, err
	}

	stat, err := out.Stat()
	if err != nil {
		out.Close()
		return 0, err
	}
	out.Seek(stat.Size(), 0)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "go-downloader")
	/* Set download starting position with 'Range' in HTTP header*/
	req.Header.Set("Range", "bytes="+strconv.FormatInt(stat.Size(), 10)+"-")
	log.Printf("%v bytes had already been downloaded.\n", stat.Size())

	//job := DatahubJob[jobid]

	resp, err := http.DefaultClient.Do(req)

	/*Save response body to file only when HTTP 2xx received. TODO*/
	if err != nil || (resp != nil && resp.StatusCode/100 != 2) {
		if resp != nil {
			l := log.Error("http status code:", resp.StatusCode, err)
			logq.LogPutqueue(l)
			body, _ := ioutil.ReadAll(resp.Body)
			l = log.Error("response Body:", string(body))
			logq.LogPutqueue(l)
			msg := string(body)
			if resp.StatusCode == 416 {
				msg = destfilename + " has already been downloaded."
			}
			r, _ := buildResp(7000+resp.StatusCode, msg, nil)

			w.WriteHeader(resp.StatusCode)
			w.Write(r)
			c <- 1
		}
		filesize := stat.Size()
		out.Close()
		if filesize == 0 {
			os.Remove(destfilename)
		}
		return 0, err
	}
	defer resp.Body.Close()
	//fname := resp.Header.Get("Source-Filename")
	//if len(fname) > 0 {
	//	p.DestName = fname
	//}

	r, _ := buildResp(7000, strret, nil)
	w.WriteHeader(http.StatusOK)
	w.Write(r)
	c <- 1
	jobtag := p.Repository + "/" + p.Dataitem + ":" + p.Tag

	srcsize, err := strconv.ParseInt(resp.Header.Get("Source-FileSize"), DECIMAL_BASE, INT_SIZE_64)
	md5str := resp.Header.Get("Source-MD5")
	log.Info("pull tag:", jobtag, destfilename, "downloading", srcsize)
	jobid := putToJobQueue(jobtag, destfilename, "downloading", srcsize)

	n, err := io.Copy(out, resp.Body)
	if err != nil {
		out.Close()
		return 0, err
	}
	out.Close()

	status := "downloaded"

	if len(md5str) > 0 {
		bmd5, err := ComputeMd5(destfilename)
		log.Debug("md5", fmt.Sprintf("%x", bmd5), destfilename, md5str)
		if err != nil {
			log.Error(destfilename, err, bmd5)
		} else if md5str != fmt.Sprintf("%x", bmd5) {
			log.Errorf("check md5 code error! src md5:%v,  local md5:%v", md5str, bmd5)
			status = "md5 error"
		}
	}
	log.Printf("%d bytes downloaded.", n)
	//job.Dlsize = stat.Size()
	//job.Stat = "finished"
	//DatahubJob[jobid] = job
	updateJobQueue(jobid, status)

	InsertTagToDb(dpexist, p)
	return n, nil
}

func getAccessToken(url string, w http.ResponseWriter) (token, entrypoint string, err error) {

	log.Println("daemon: connecting to", DefaultServer+url, "to get accesstoken")
	req, err := http.NewRequest("POST", DefaultServer+url, nil)
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
	if resp.StatusCode != 200 {

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

func putToJobQueue(tag, destfilename, stat string, srcsize int64 /*, stat os.FileInfo*/) string {

	var jobid string
	var err error

	if jobid, err = genJobID(); err != nil {
		jobid = destfilename //ops...
	}

	job := ds.JobInfo{}
	job.ID = jobid
	job.Path = destfilename
	//job.Dlsize = stat.Size()
	job.Stat = stat
	job.Tag = tag
	job.Srcsize = srcsize
	//DatahubJob[jobid] = job
	DatahubJob = append(DatahubJob, job)

	saveJobDB()

	return jobid
}

func updateJobQueue(jobid, stat string) {
	for k, j := range DatahubJob {
		if j.ID == jobid {
			DatahubJob[k].Stat = stat
			DatahubJob[k].Dlsize = DatahubJob[k].Srcsize
			updateJobStatus()
		}
	}
}
