package daemon

import (
	"bytes"
	"encoding/json"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Beatbody struct {
	Daemonid   string   `json:"daemonid"`
	Entrypoint []string `json:"entrypoint"`
	Log        []string `json:"log,omitempty"`
	Role       int      `json:"role"` //0 puller, 1 publisher
	Errortag   []string `json:"abnormaltags,omitempty"`
}

type MessageData struct {
	Event     string    `json:"event,omitempty"`
	EventTime time.Time `json:"eventtime,omitempty"`
	Repname   string    `json:"repname,omitempty"`
	Itemname  string    `json:"itemname,omitempty"`
	Tag       string    `json:"tag,omitempty"`
}

type Messages struct {
	Messageid int         `json:"messageid,omitempty"`
	Type      string      `json:"type,omitempty"`
	Data      MessageData `json:"data,omitempty"`
}

var (
	EntryPoint       string
	EntryPointStatus = "not available"
	DaemonID         string
	heartbeatTimeout = 5 * time.Second
	Errortagsmap     = make(map[string]string)
)

var (
	AutoPull     bool = true
	g_DaemonRole int  = 0
)

const (
	TAGADDED    = "tag_added"
	NOTREAD     = 0
	ALREADYREAD = 1

	PUBLISHER = 1
	PULLER    = 0
)

func HeartBeat() {
	getEp := false

	g_DaemonRole = GetDaemonRoleByPubRecord()

	for {
		if len(DaemonID) == 0 {
			log.Warn("No daemonid. You'd better start datahub with the parameter \"--token\".")
			return
		}

		heartbeatbody := Beatbody{Daemonid: DaemonID, Role: g_DaemonRole}
		if getEp == false && len(EntryPoint) == 0 {
			EntryPoint = getEntryPoint()
			getEp = true
		}
		if len(EntryPoint) != 0 {
			heartbeatbody.Entrypoint = append(heartbeatbody.Entrypoint, EntryPoint)
		}

		logQueue := logq.LogGetqueue()
		if len(logQueue) > 0 {
			heartbeatbody.Log = logQueue
		}

		errortags := checkErrortagsmap(&Errortagsmap)
		if len(errortags) != 0 {
			heartbeatbody.Errortag = errortags
		}

		jsondata, err := json.Marshal(heartbeatbody)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
		}
		url := DefaultServerAPI + "/heartbeat"
		log.Trace("connecting to", url, string(jsondata))
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsondata))
		/*
			if len(loginAuthStr) > 0 {
				req.Header.Set("Authorization", loginAuthStr)
			}
		*/
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			time.Sleep(10 * time.Second)
			continue
		}

		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)
		log.Tracef("HeartBeat http statuscode:%v,  http body:%s", resp.StatusCode, body)

		result := ds.Result{}
		if err := json.Unmarshal(body, &result); err == nil {
			if result.Code == 0 {
				EntryPointStatus = "available"
			} else {
				EntryPointStatus = "not available"
			}
		}

		time.Sleep(heartbeatTimeout)
	}
}

func GetMessages() {
	log.Info("start GetMessages from messages server")
	var sleepInterval int
	var srtInterval string
	var e error
	url := DefaultServerAPI + "/notifications?forclient=1&type=item_event&status=0"
	for AutoPull == true {

		if srtInterval = os.Getenv("DATAHUB_MSG_INTERVAL"); len(srtInterval) > 0 {
			sleepInterval, e = strconv.Atoi(srtInterval)
			if e != nil {
				l := log.Error(e)
				logq.LogPutqueue(l)
			}
		} else {
			sleepInterval = 600
		}

		time.Sleep(time.Duration(sleepInterval) * time.Second)
		log.Debug("connecting to", url)
		req, err := http.NewRequest("GET", url, nil)

		if len(loginAuthStr) > 0 {
			req.Header.Set("Authorization", loginAuthStr)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)

			continue
		}
		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusOK {
			log.Debugf("HeartBeat http statuscode:%v,  http body:%s", resp.StatusCode, body)

			result := ds.Result{}
			Pages := ds.ResultPages{}
			MessagesSlice := []Messages{}
			Pages.Results = &MessagesSlice
			result.Data = &Pages

			if err := json.Unmarshal(body, &result); err == nil {
				if result.Code == 0 {
					log.Debug(result)
					for _, v := range MessagesSlice {
						if v.Type == "item_event" && v.Data.Event == TAGADDED {
							InsertToTagadded(v.Data.EventTime, v.Data.Repname, v.Data.Itemname, v.Data.Tag, NOTREAD)
						}
					}
				} else {
					l := log.Error("Get Messages errror:", result.Code)
					logq.LogPutqueue(l)
				}
			} else {
				log.Error(err)
			}

		} else if resp.StatusCode == http.StatusUnauthorized {
			log.Debug("not login", http.StatusUnauthorized)
			urllogin := DefaultServerAPI + "/"
			reql, err := http.NewRequest("GET", urllogin, nil)
			if len(loginBasicAuthStr) > 0 {
				reql.Header.Set("Authorization", loginBasicAuthStr)
				log.Info("user name:", gstrUsername)
			} else {
				log.Warn("not login")
				continue
			}

			respl, err := http.DefaultClient.Do(reql)
			if err != nil {
				log.Error(err)
				continue
			}
			defer respl.Body.Close()

			result := &ds.Result{}
			log.Println("login return", respl.StatusCode)
			if respl.StatusCode == 200 {
				body, _ := ioutil.ReadAll(respl.Body)
				log.Println(string(body))

				result.Data = &tk{}
				if err = json.Unmarshal(body, result); err != nil {
					log.Error(err)
					log.Println(respl.StatusCode, string(body))
					continue
				} else {
					loginAuthStr = "Token " + result.Data.(*tk).Token
					loginLogged = true
					log.Println(loginAuthStr)
				}
			}
		}
	}
}

func checkErrortagsmap(errortagsmap *map[string]string) (errortags []string) {

	errortags = make([]string, 0)
	for errortagfile, errortag := range *errortagsmap {
		f, err := os.Open(errortagfile)
		if err != nil && os.IsNotExist(err) {
			log.Error("------>file:", errortagfile, "不存在")
			errortags = append(errortags, errortag)
		} else {
			delete(*errortagsmap, errortagfile)
		}
		defer f.Close()
	}
	//log.Info("----------->errortags:",errortags)
	//for _, errortag := range errortags {
	//	log.Info("------------>errortag:", errortag)
	//}
	return
}

func CheckHealthClock() {
	log.Debug("--------->BEGIN")

	checkHealth(&Errortagsmap)

	timer := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-timer.C:
			now := time.Now()
			if now.Hour()%6 == 0 {
				log.Info("Time:", now)
				checkHealth(&Errortagsmap)
			}
		}
	}
	log.Debug("---------->END")
}

func checkHealth(errorTagsMap *map[string]string) {

	localfiles := make([]string, 0)
	alllocalfiles := make([]string, 0)
	localfilepath := GetLocalfilePath()

	for _, path := range localfilepath {
		localfiles = ScanLocalFile(path)
		for _, localfile := range localfiles {
			alllocalfiles = append(alllocalfiles, localfile)
		}
	}

	log.Info(alllocalfiles)
	var tagDetails map[string]string
	tagDetails = make(map[string]string)

	err := GetAllTagDetails(&tagDetails)
	if err != nil {
		log.Error(err)
	}

	var i int
	for file, tag := range tagDetails {
		for i = 0; i < len(alllocalfiles); i++ {
			//log.Info("--------->tag:", tag)
			//log.Info("--------->tagfile:",file)
			//log.Info("--------->localfile:",localfiles[i])
			if file == alllocalfiles[i] {
				break
			}
		}
		if i >= len(alllocalfiles) {
			(*errorTagsMap)[file] = tag
		}
	}

	//for errortagfile, errortag := range *errorTagsMap {
	//	log.Info("------->errortag:", errortag, "-------->", errortagfile)
	//}
}

func ScanLocalFile(path string) []string {

	localfiles := make([]string, 0)

	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		localfiles = append(localfiles, path)
		return nil
	})
	if err != nil {
		log.Error("filepath.Walk() returned %v\n", err)
	}

	//for _, localfile := range localfiles {
	//	//fmt.Println(localfile)
	//	log.Info("--------------------------------------------------------->localfile:", localfile)
	//}

	return localfiles
}

func RemoveDuplicatesAndEmpty(a []string) (ret []string) {
	a_len := len(a)
	for i := 0; i < a_len; i++ {
		if (i > 0 && a[i-1] == a[i]) || len(a[i]) == 0 {
			continue
		}
		ret = append(ret, a[i])
	}
	return
}
