package daemon

import (
	"crypto/rand"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

//var DatahubJob = make(map[string]ds.JobInfo) //job[id]=JobInfo
const DatahubJobLenth = 16

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
			removeJobDB(&DatahubJob[idx])

			DatahubJob = append(DatahubJob[:idx], DatahubJob[idx+1:]...)
			msg, code, httpcode = fmt.Sprintf("job %s deleted.", jobid), 0, http.StatusOK
		}
	}

	r, _ := buildResp(code, msg, nil)
	w.WriteHeader(httpcode)
	w.Write(r)

	return

}

func jobRmAllHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	log.Trace("from", req.RemoteAddr, req.Method, req.URL.RequestURI(), req.Proto)

	DatahubJob = make([]ds.JobInfo, DatahubJobLenth)
	msg, code, httpcode := "Remove all jobs OK.", cmd.ResultOK, http.StatusOK
	if err := removeAllJobDB(); err != nil {
		msg, code, httpcode = fmt.Sprintln("Remove all jobs error.", err), cmd.ErrorRemoveAllJobs, http.StatusOK
	}

	r, _ := buildResp(code, msg, nil)
	w.WriteHeader(httpcode)
	w.Write(r)

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

func updateJobQueue(jobid, stat string, dlsize int64) {
	for k, j := range DatahubJob {
		if j.ID == jobid {
			DatahubJob[k].Stat = stat
			DatahubJob[k].Dlsize = dlsize
			updateJobStatus(&DatahubJob[k])
		}
	}
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

	saveJobDB(&job)

	return jobid
}

func LoadJobFromDB() (e error) {
	sLoad := `SELECT JOBID, TAG, FILEPATH, STATUS, DOWNSIZE, SRCSIZE FROM DH_JOB;`
	rows, e := g_ds.QueryRows(sLoad)
	if e != nil {
		l := log.Error(e)
		logq.LogPutqueue(l)
		return
	}
	defer rows.Close()

	for rows.Next() {
		job := ds.JobInfo{}
		rows.Scan(&job.ID, &job.Tag, &job.Path, &job.Stat, &job.Dlsize, &job.Srcsize)
		DatahubJob = append(DatahubJob, job)
	}
	return
}

func saveJobDB(job *ds.JobInfo) (e error) {
	log.Debug("TODO save job info to db.")
	sInsertJob := fmt.Sprintf(`INSERT INTO DH_JOB (JOBID, TAG, FILEPATH, STATUS, CREATE_TIME, STAT_TIME, DOWNSIZE, SRCSIZE)
		VALUES ('%s','%s','%s','%s', datetime('now'), datetime('now'),%d, %d);`,
		job.ID, job.Tag, job.Path, job.Stat, job.Dlsize, job.Srcsize)
	_, e = g_ds.Insert(sInsertJob)
	if e != nil {
		l := log.Error(e)
		logq.LogPutqueue(l)
	}
	return
}

func updateJobStatus(job *ds.JobInfo) (e error) {
	log.Debug("TODO updata job stat to db.")
	sUpdateJob := fmt.Sprintf(`UPDATE DH_JOB SET STATUS='%s', STAT_TIME=datetime('now'), DOWNSIZE=%d
		WHERE JOBID='%s';`, job.Stat, job.Dlsize, job.ID)
	_, e = g_ds.Update(sUpdateJob)
	if e != nil {
		l := log.Error(e)
		logq.LogPutqueue(l)
	}
	return
}

func removeJobDB(job *ds.JobInfo) (e error) {
	log.Debug("TODO remove jobid from db")
	sRmJob := fmt.Sprintf(`DELETE FROM DH_JOB WHERE JOBID=%d;`, job.ID)
	_, e = g_ds.Delete(sRmJob)
	if e != nil {
		l := log.Error(e)
		logq.LogPutqueue(l)
	}
	return
}

func removeAllJobDB() (e error) {
	log.Debug("TODO remove all jobs from db")
	sRmJobs := `DELETE FROM DH_JOB;`
	_, e = g_ds.Delete(sRmJobs)
	if e != nil {
		l := log.Error(e)
		logq.LogPutqueue(l)
	}
	return
}
