package daemon

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

var DPTYPES []string = cmd.DataPoolTypes

func dpPostOneHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	r.ParseForm()
	rw.WriteHeader(http.StatusOK)

	result, _ := ioutil.ReadAll(r.Body)
	struDp := cmd.FormatDpCreate{}
	err := json.Unmarshal(result, &struDp)
	if err != nil {
		l := log.Error("Invalid argument. json.Unmarshal error", err)
		logq.LogPutqueue(l)
		rw.Write([]byte(`{"msg":"Invalid argument."}`))
		return
	}

	log.Debug("Get FormatDpCreate obj ok.")

	var allowtype bool = false
	for _, v := range DPTYPES {
		if struDp.Type == v {
			allowtype = true
		}
	}
	if !allowtype {
		log.Println("DataHub : Datapool type need to be:", DPTYPES)
		rw.Write([]byte(fmt.Sprintf(`{"msg":"Datapool type need to be:%s"}`, DPTYPES)))
		return
	}

	if len(struDp.Name) == 0 {
		log.Println("Invalid argument")
		rw.Write([]byte(`{"msg":"Invalid argument"}`))
		return
	} else {
		log.Println("Creating datapool with name:", struDp.Name)
		msg := &ds.MsgResp{}
		var sdpDirName string
		if len(struDp.Conn) == 0 {
			struDp.Conn = g_strDpPath
			sdpDirName = g_strDpPath

		} else if strings.Contains(struDp.Conn, ":") == false && struDp.Type == DPFILE {
			sdpDirName = g_strDpPath + "\\" + struDp.Conn
			struDp.Conn = sdpDirName

		} else {
			sdpDirName = struDp.Conn
		}

		dpexist := CheckDataPoolExist(struDp.Name)
		if dpexist {
			msg.Msg = fmt.Sprintf("'%s' has been created already.", struDp.Name)
			resp, _ := json.Marshal(msg)
			rw.Write(resp)
			return
		}

		var err error

		if struDp.Type == DPS3 {
			struDp.Conn = strings.TrimLeft(struDp.Conn, "/")
			err = nil
		} else if struDp.Type == DPFILE {
			err = os.MkdirAll(sdpDirName, 0755)
			log.Debug("mkdir", sdpDirName)
		}

		if err != nil {
			l := log.Error(err, sdpDirName)
			logq.LogPutqueue(l)
			msg.Msg = err.Error()
		} else {
			msg.Msg = fmt.Sprintf("Datapool has been created successfully.Name:%s Type:%s Path:%s.", struDp.Name, struDp.Type, sdpDirName)
			struDp.Conn = strings.TrimRight(struDp.Conn, "/")
			sql_dp_insert := fmt.Sprintf(`insert into DH_DP (DPID, DPNAME, DPTYPE, DPCONN, STATUS)
					values (null, '%s', '%s', '%s', 'A')`, struDp.Name, struDp.Type, struDp.Conn)
			if _, e := g_ds.Insert(sql_dp_insert); err != nil {
				//os.Remove(sdpDirName)  //don't delete it. It is maybe used by others
				l := log.Error(e)
				logq.LogPutqueue(l)
				msg.Msg = e.Error()
			}
		}
		resp, _ := json.Marshal(msg)
		rw.Write(resp)
	}

}

func dpGetAllHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	r.ParseForm()
	rw.WriteHeader(http.StatusOK)

	dps := []cmd.FormatDp{}
	result := &ds.Result{Code: cmd.ResultOK, Data: &dps} //must use a pointer dps to initial Data
	onedp := cmd.FormatDp{}
	sqlDp := fmt.Sprintf(`SELECT DPNAME, DPTYPE FROM DH_DP WHERE STATUS = 'A'`)
	rows, err := g_ds.QueryRows(sqlDp)
	if err != nil {
		log.Error(err)
		SqlExecError(rw, result, err.Error())
		return
	}
	defer rows.Close()
	bresultflag := false
	for rows.Next() {
		bresultflag = true
		rows.Scan(&onedp.Name, &onedp.Type)
		dps = append(dps, onedp)
	}
	if bresultflag == false {
		result.Code = cmd.ErrorNoRecord
		result.Msg = "There isn't any datapool."
		log.Info(result.Msg)
	}

	resp, _ := json.Marshal(result)
	rw.Write(resp)
}

func dpGetOneHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	r.ParseForm()
	rw.WriteHeader(http.StatusOK)

	dpname := ps.ByName("dpname")

	//In future, we need to get dptype in Json to surpport FILE\ DB\ SDK\ API datapool

	onedp := cmd.FormatDpDetail{}
	result := &ds.Result{Code: cmd.ResultOK, Msg: "", Data: &onedp}

	sqlTotal := fmt.Sprintf(`SELECT COUNT(*) FROM DH_DP 
		WHERE STATUS = 'A' AND DPNAME = '%s'`, string(dpname))
	row, err := g_ds.QueryRow(sqlTotal)
	if err != nil {
		SqlExecError(rw, result, err.Error())
		return
	}
	var total int
	row.Scan(&total)
	if total == 0 {
		msg := fmt.Sprintf("Datapool '%v' not found.", dpname)
		WriteResp(rw, result, msg)
		log.Error("Datahub:", result.Code, "Msg:", result.Msg)
		return
	}

	sqlDp := fmt.Sprintf(`SELECT DPID, DPNAME, DPTYPE, DPCONN FROM DH_DP 
		WHERE STATUS = 'A' AND DPNAME = '%s'`, dpname)
	rowdp, err := g_ds.QueryRow(sqlDp)
	if err != nil {
		SqlExecError(rw, result, err.Error())
		return
	}

	var dpid int
	onedp.Items = make([]cmd.Item, 0, 16)
	rowdp.Scan(&dpid, &onedp.Name, &onedp.Type, &onedp.Conn)
	if dpid > 0 {
		//Use "left out join" to get repository/dataitem records, whether it has tags or not.
		//B.STATUS='A'
		sqlTag := fmt.Sprintf(`SELECT A.REPOSITORY, A.DATAITEM, A.ITEMDESC, A.PUBLISH ,strftime(A.CREATE_TIME), B.TAGNAME, B.DETAIL,strftime(B.CREATE_TIME)
				FROM DH_DP_RPDM_MAP A LEFT JOIN DH_RPDM_TAG_MAP B
				ON (A.RPDMID = B.RPDMID)
				WHERE A.DPID = %v AND A.STATUS='A' `, dpid)
		tagrows, err := g_ds.QueryRows(sqlTag)
		if err != nil {
			SqlExecError(rw, result, err.Error())
			return
		}
		defer tagrows.Close()
		for tagrows.Next() {
			item := cmd.Item{}
			var repoitemtime string
			tagrows.Scan(&item.Repository, &item.DataItem, &item.ItemDesc, &item.Publish, &repoitemtime, &item.Tag, &item.TagDetail, &item.Time)
			if len(item.Time) == 0 {
				item.Time = repoitemtime
			}
			//log.Println(item.Repository, item.DataItem, item.Tag, item.Time, item.Publish)
			onedp.Items = append(onedp.Items, item)
		}
	}

	resp, _ := json.Marshal(result)
	log.Println(string(resp))
	rw.Write(resp)

}

func SqlExecError(rw http.ResponseWriter, result *ds.Result, msg string) {
	result.Msg = msg
	result.Code = cmd.ErrorSqlExec
	resp, _ := json.Marshal(result)
	rw.WriteHeader(http.StatusInternalServerError)
	rw.Write(resp)
}

func WriteResp(rw http.ResponseWriter, result *ds.Result, msg string) {
	result.Msg = msg
	resp, _ := json.Marshal(result)
	rw.WriteHeader(http.StatusOK)
	rw.Write(resp)
}

func dpDeleteOneHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	r.ParseForm()

	dpname := ps.ByName("dpname")
	msg := &ds.MsgResp{}

	sqlDpRm := fmt.Sprintf(`SELECT DPID FROM DH_DP WHERE DPNAME ='%s' AND STATUS='A';`, dpname)
	dprow, err := g_ds.QueryRow(sqlDpRm)
	if err != nil {
		msg.Msg = err.Error()
		b, _ := json.Marshal(msg)
		rw.Write(b)
		log.Error(err)
		return
	}

	var dpid int
	dprow.Scan(&dpid)

	if dpid == 0 {
		msg.Msg = fmt.Sprintf("Datapool '%s' does not exist.", dpname)
		log.Error("DELETE : datapool", dpname, "does not exist.")
		resp, _ := json.Marshal(msg)
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write(resp)
		return
	} else {
		sqldpitem := fmt.Sprintf("SELECT COUNT(1) FROM DH_DP_RPDM_MAP WHERE DPID = %v AND PUBLISH='Y'; ", dpid)
		row, err := g_ds.QueryRow(sqldpitem)
		if err != nil {
			msg.Msg = err.Error()
			b, _ := json.Marshal(msg)
			rw.Write(b)
			return
		}

		var pubCount int
		row.Scan(&pubCount)
		if pubCount > 0 {
			msg.Msg = fmt.Sprintf(`Datapool %s with can't be removed , it contains published DataItem !`, dpname)
		} else {
			sqlUpdate := fmt.Sprintf("UPDATE DH_DP SET STATUS = 'N' WHERE DPID = %v;", dpid)
			_, err := g_ds.Update(sqlUpdate)
			if err != nil {
				msg.Msg = err.Error()
			} else {
				msg.Msg = fmt.Sprintf("Datapool %s removed successfully!", dpname)
			}
		}
		resp, _ := json.Marshal(msg)
		rw.Write(resp)
	}

}
