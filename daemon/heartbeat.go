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
	"strconv"
	"time"
	"path/filepath"
)

type Beatbody struct {
	Daemonid   string   `json:"daemonid"`
	Entrypoint []string `json:"entrypoint"`
	Log        []string `json:"log,omitempty"`
	Role       int      `json:"role"` //0 puller, 1 publisher
	Errortag   []string `json:"error,omitempty"`
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

		ErrorTags := CheckHealth()
		heartbeatbody.Errortag = ErrorTags

		jsondata, err := json.Marshal(heartbeatbody)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
		}
		url := DefaultServer + "/heartbeat"
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
	url := DefaultServer + "/notifications?forclient=1&type=item_event&status=0"
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
			urllogin := DefaultServer + "/"
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

func ScanLocalFile(path string) ([]string) {
	log.Info("--------------------------------->Into ScanLocalFile().....................................")
	localfiles := make([]string, 0)

	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if ( f == nil ) {return err}
		if f.IsDir() {
			return nil
		}

		localfiles = append(localfiles, path)
		return nil
	})
	if err != nil {
		log.Error("filepath.Walk() returned %v\n", err)
	}

	for _, localfile := range localfiles {
		//fmt.Println(localfile)
		log.Info("--------------------------------------------------------->", localfile)
	}

	return localfiles
}

func CheckHealth() []string {
	log.Info("---------------------------------->Into CheckHealth()............................................")
	errortags := make([]string, 0)
	localfiles := make([]string, 0)
	dpnc := GetDatapoolNameAndConn();
	for _, dpconn := range dpnc {
		//dppath := GetDataPoolDpconn(dpname)
		localfiles = ScanLocalFile(dpconn)
	}
	var tagDetails map[string] string

	err := GetAllTagDetails(&tagDetails)
	if err != nil {
		log.Error(err)
	}

	var i int
	j := 0
	for file, tag := range tagDetails {
		for i = 0; i <len(localfiles); i++ {
			if file == localfiles[i] {
				continue
			}
		}
		if i >= len(localfiles) {
			errortags[j] = tag
		}
	}

	for _, errortag := range errortags {
		//fmt.Println(errortag)
		log.Info("-------------------------------------------------------->", errortag)
	}

	return errortags
}
