package daemon

import (
	"bytes"
	"encoding/json"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"io/ioutil"
	"net/http"
	"time"
)

type Beatbody struct {
	Daemonid   string   `json:"daemonid"`
	Entrypoint []string `json:"entrypoint"`
	Log        []string `json:"log,omitempty"`
}

var (
	EntryPoint       string
	EntryPointStatus = "not available"
	DaemonID         string
	heartbeatTimeout = 5 * time.Second
)

var (
	AutoPull bool = true
)

func HeartBeat() {
	getEp := false
	for {

		heartbeatbody := Beatbody{Daemonid: DaemonID}
		if getEp == false && len(EntryPoint) == 0 {
			EntryPoint = getEntryPoint()
			getEp = true
		}
		heartbeatbody.Entrypoint = append(heartbeatbody.Entrypoint, EntryPoint)

		logQueue := logq.LogGetqueue()
		if len(logQueue) > 0 {
			heartbeatbody.Log = logQueue
		}

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
	url := DefaultServer + "/notifications?forclient=1&type=item_event&status=0"
	for AutoPull == true {
		log.Trace("connecting to", url)
		req, err := http.NewRequest("GET", url, nil)

		if len(loginAuthStr) > 0 {
			req.Header.Set("Authorization", loginAuthStr)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			time.Sleep(30 * time.Second)
			continue
		}
		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusOK {
			log.Tracef("HeartBeat http statuscode:%v,  http body:%s", resp.StatusCode, body)

			result := ds.Result{}
			if err := json.Unmarshal(body, &result); err == nil {
				if result.Code == 0 {
					EntryPointStatus = "available"
				} else {
					EntryPointStatus = "not available"
				}
			}

			time.Sleep(900 * time.Second)
		} else if resp.StatusCode == http.StatusUnauthorized {
			reql, err := http.NewRequest("GET", url, nil)
			if len(loginBasicAuthStr) > 0 {
				reql.Header.Set("Authorization", loginBasicAuthStr)
				log.Info("user name:", gstrUsername)
			} else {
				log.Warn("not login")
				return
			}

			respl, err := http.DefaultClient.Do(reql)
			if err != nil {
				log.Error(err)
				return
			}
			defer respl.Body.Close()
			log.Println("login return", respl.StatusCode)
			if respl.StatusCode == 200 {
				body, _ := ioutil.ReadAll(respl.Body)
				log.Println(string(body))
				type tk struct {
					Token string `json:"token"`
				}
				token := &tk{}
				if err = json.Unmarshal(body, token); err != nil {
					log.Error(err)
					log.Println(respl.StatusCode, string(body))
					return
				} else {
					loginAuthStr = "Token " + token.Token
					loginLogged = true
					log.Println(loginAuthStr)
				}
			}
		}
	}
}
