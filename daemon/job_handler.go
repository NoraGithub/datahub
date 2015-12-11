package daemon

import (
	"crypto/rand"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

//var DatahubJob = make(map[string]ds.JobInfo) //job[id]=JobInfo
var DatahubJob = []ds.JobInfo{} //job[id]=JobInfo

func jobHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	log.Trace("from", req.RemoteAddr, req.Method, req.URL.RequestURI(), req.Proto)

	for i := len(DatahubJob) - 1; i >= 0; i-- {
		if DatahubJob[i].Stat == "downloading" {
			DatahubJob[i].Dlsize, _ = GetFileSize(DatahubJob[i].Path)
		}
	}

	//r, _ := buildResp(0, "ok", joblist)
	r, _ := buildResp(0, "ok", DatahubJob)
	w.WriteHeader(http.StatusOK)
	w.Write(r)

	//http.Error(w, log.Info(req.URL.RequestURI()), http.StatusNotImplemented)

	return

}

func jobDetailHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	log.Trace("from", req.RemoteAddr, req.Method, req.URL.RequestURI(), req.Proto)
	jobid := ps.ByName("id")

	var job []ds.JobInfo
	for _, v := range DatahubJob {
		if v.ID == jobid {
			if v.Stat == "downloading" {
				v.Dlsize, _ = GetFileSize(v.Path)
			}
			job = append(job, v)
		}
	}

	r, _ := buildResp(0, "ok", job)
	w.WriteHeader(http.StatusOK)
	w.Write(r)

	return

}

func jobRmHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	log.Trace("from", req.RemoteAddr, req.Method, req.URL.RequestURI(), req.Proto)

	jobid := ps.ByName("id")
	msg, code, httpcode := fmt.Sprintf("job %s not found.", jobid), 4404, http.StatusNotFound
	for idx, v := range DatahubJob {
		if v.ID == jobid {
			DatahubJob = append(DatahubJob[:idx], DatahubJob[idx+1:]...)
			removeJobDB()
			msg, code, httpcode = fmt.Sprintf("job %s deleted.", jobid), 0, http.StatusOK
		}
	}

	r, _ := buildResp(code, msg, nil)
	w.WriteHeader(httpcode)
	w.Write(r)

	return

}

func genJobID() (id string, err error) {
	c := 4
	b := make([]byte, c)
	_, err = rand.Read(b)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	return fmt.Sprintf("%x", b), nil
}

func saveJobDB() {
	fmt.Println("TODO save job info to db.")
}

func updateJobStatus() {
	fmt.Println("TODO updata job stat to db.")
}

func removeJobDB() {
	fmt.Println("TODO remove jobid from db")
}
