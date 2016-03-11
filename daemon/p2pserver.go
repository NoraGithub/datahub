package daemon

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/daemon/dpdriver"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
)

func startP2PServer() {
	p2pListener, err := net.Listen("tcp", ":35800")
	if err != nil {
		log.Fatal(err)
	}

	p2psl, err = tcpNew(p2pListener)
	if err != nil {
		log.Fatal(err)
	}

	P2pRouter := httprouter.New()
	P2pRouter.GET("/", sayhello)
	P2pRouter.GET("/pull/:repo/:dataitem/:tag", p2p_pull)
	P2pRouter.GET("/health", p2pHealthyCheckHandler)

	p2pserver := http.Server{Handler: P2pRouter}

	wg.Add(1)
	defer wg.Done()

	log.Info("p2p server start")
	p2pserver.Serve(p2psl)
	log.Info("p2p server stop")

}

func p2pHealthyCheckHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	healthbody := Beatbody{Daemonid: DaemonID}
	jsondata, err := json.Marshal(healthbody)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write(jsondata)
}

/*pull parses filename and target IP from HTTP GET method, and start downloading routine. */
func p2p_pull(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	l := log.Info("P2P PULL FROM", r.RemoteAddr, r.Method, r.URL.RequestURI(), r.Proto)
	logq.LogPutqueue(l)

	r.ParseForm()
	sRepoName := ps.ByName("repo")
	sDataItem := ps.ByName("dataitem")
	sTag := ps.ByName("tag")

	log.Info(sRepoName, sDataItem, sTag)
	jobtag := fmt.Sprintf("%s/%s:%s", sRepoName, sDataItem, sTag)
	var irpdmid, idpid int
	var stagdetail, itemdesc, dpconn, dpname, dptype string
	msg := &ds.MsgResp{}
	msg.Msg = "OK."

	irpdmid, idpid, itemdesc = GetRpdmidDpidItemdesc(sRepoName, sDataItem)
	if len(itemdesc) == 0 {
		itemdesc = sRepoName + "_" + sDataItem
	}
	log.Debug("dpid:", idpid, "rpdmid:", irpdmid, "itemdesc:", itemdesc)

	stagdetail = GetTagDetail(irpdmid, sTag)
	log.Debug("tagdetail", stagdetail)
	if len(stagdetail) == 0 {
		l := log.Warnf("%s(tag:%s) not found", stagdetail, sTag)
		logq.LogPutqueue(l)
		http.Error(rw, sTag+" not found", http.StatusNotFound)
		return
	}

	dpconn, dpname, dptype = GetDpconnDpnameDptypeByDpid(idpid)
	log.Debug("dpconn:", dpconn, "dpname:", dpname, "dptype:", dptype)

	datapool, err := dpdriver.New(dptype)
	if err != nil {
		WriteErrLogAndResp(rw, http.StatusInternalServerError, cmd.ErrorNoDatapoolDriver, err)
		return
	}

	filepathname := datapool.GetFileTobeSend(dpconn, dpname, itemdesc, stagdetail)
	//filepathname := dpconn + "/" + itemdesc + "/" + stagdetail
	log.Println("filename:", filepathname)
	if exists := isFileExists(filepathname); !exists {
		l := log.Error(filepathname, "not found")
		logq.LogPutqueue(l)
		putToJobQueue(jobtag, filepathname, "N/A", -1)
		msg.Msg = fmt.Sprintf("tag:%s not found", sTag)
		resp, _ := json.Marshal(msg)
		respStr := string(resp)
		rw.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(rw, respStr)
		return
	}

	tokenValid := false
	retmsg := ""
	token := r.Form.Get("token")
	username := r.Form.Get("username")
	if len(token) > 0 && len(username) > 0 {
		log.Println(r.URL.Path, "token:", token, "username:", username)
		url := "/transaction/" + sRepoName + "/" + sDataItem + "/" + sTag +
			"?cypt_accesstoken=" + token + "&username=" + username
		tokenValid, retmsg = checkAccessToken(url)
	}

	if !tokenValid {
		l := log.Warn(retmsg, token, username)
		logq.LogPutqueue(l)
		http.Error(rw, retmsg, http.StatusBadRequest)
		return
	}

	size, err := GetFileSize(filepathname)
	if err != nil {
		l := log.Errorf("Get %s size error, %v", filepathname, err)
		logq.LogPutqueue(l)
	}
	log.Printf("Tag file full path name :%v, size:%v", filepathname, size)
	//rw.Header().Set("Source-FileName", stagdetail)
	bmd5, err := ComputeMd5(filepathname)
	strmd5 := fmt.Sprintf("%x", bmd5)
	if err != nil {
		log.Error(filepathname, err, bmd5, strmd5)
	} else {
		rw.Header().Set("X-Source-MD5", strmd5)
	}
	rw.Header().Set("X-Source-FileSize", strconv.FormatInt(size, 10))

	l = log.Info("transfering", filepathname, bmd5, strmd5)
	logq.LogPutqueue(l)

	jobid := putToJobQueue(jobtag, filepathname, "transfering", size)
	http.ServeFile(rw, r, filepathname)
	updateJobQueue(jobid, "transfered", 0)

	return
}

func sayhello(rw http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	rw.WriteHeader(http.StatusOK)
	body, _ := ioutil.ReadAll(req.Body)
	fmt.Fprintf(rw, "%s Hello p2p HTTP !\n", req.URL.Path)
	fmt.Fprintf(rw, "%s \n", string(body))
}

func checkAccessToken(tokenUrl string) (bool, string) {
	log.Println("daemon: connecting to", DefaultServer+tokenUrl)
	req, err := http.NewRequest("GET", DefaultServer+tokenUrl, nil)
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, ""
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	type tokenDs struct {
		Valid bool `json:"valid"`
	}
	tkresp := tokenDs{}
	result := &ds.Result{Data: &tkresp}
	if err = json.Unmarshal(body, &result); err != nil {
		log.Println(err)
		return false, ""
	}
	log.Trace(string(body))

	return tkresp.Valid, result.Msg
}

func WriteErrLogAndResp(w http.ResponseWriter, httpcode, errorcode int, err error) error {
	l := log.Error(err)
	logq.LogPutqueue(l)
	HttpNoData(w, http.StatusBadRequest, cmd.ErrorNoRecord, err.Error())
	return err
}
