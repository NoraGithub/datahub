package daemon

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/daemon/dpdriver"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	dfs "github.com/colinmarc/hdfs"
	"github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
)

var DPTYPES []string = cmd.DataPoolTypes

const (
	MsgOfNoDatapool = "There isn't any datapool."
)

func dpPostOneHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// if false == authDaemon(rw, r) {
	// 	return
	// }
	r.ParseForm()

	result, _ := ioutil.ReadAll(r.Body)
	struDp := cmd.FormatDpCreate{}
	err := json.Unmarshal(result, &struDp)
	if err != nil {
		l := log.Error("Invalid argument. json.Unmarshal error", err)
		logq.LogPutqueue(l)
		JsonResult(rw, http.StatusBadRequest, cmd.ErrorInvalidPara, "Invalid argument.", nil)
		return
	}

	var allowtype bool = false
	for _, v := range DPTYPES {
		if struDp.Type == v {
			allowtype = true
		}
	}
	if !allowtype {
		log.Println("DataHub : Datapool type need to be:", DPTYPES)
		JsonResult(rw, http.StatusBadRequest, cmd.ErrorInvalidPara,
			fmt.Sprintf("Datapool type need to be:%v", DPTYPES), nil)
		return
	}

	if len(struDp.Name) == 0 || strings.Contains(struDp.Name, "/") == true {
		log.Println("Invalid argument")
		JsonResult(rw, http.StatusBadRequest, cmd.ErrorInvalidPara, "Invalid argument.", nil)
		return
	} else {
		log.Println("Creating datapool with name:", struDp.Name)

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
			JsonResult(rw, http.StatusConflict, cmd.ErrorDatapoolAlreadyExits,
				fmt.Sprintf("'%s' already exists, please change another name.", struDp.Name), nil)
			return
		}

		var err error

		if struDp.Type == DPS3 {
			struDp.Conn = strings.TrimLeft(struDp.Conn, "/")
			err = nil
		} else if struDp.Type == DPFILE {
			err = os.MkdirAll(sdpDirName, 0777)
		} else {
			connstr := struDp.Host + ":" + struDp.Port

			client, err := dfs.New(connstr)
			if err != nil {
				logq.LogPutqueue(log.Error("New err:", err))
				JsonResult(rw, http.StatusBadRequest, cmd.InternalError, "hdfs new client failed.", nil)
				return
			}

			info, err := client.Stat("/")
			if err != nil {
				logq.LogPutqueue(log.Error("Stat err:", err))
				JsonResult(rw, http.StatusBadRequest, cmd.InternalError, "hdfs get hadoop user failed.", nil)
				return
			}
			hadoopUser := *info.Sys().(*hadoop_hdfs.HdfsFileStatusProto).Owner

			client, err = dfs.NewForUser(connstr, hadoopUser)
			if err != nil {
				logq.LogPutqueue(log.Error("NewForUser err:", err))
				JsonResult(rw, http.StatusBadRequest, cmd.InternalError, "hdfs new client for user failed.", nil)
				return
			}

			err = client.MkdirAll(struDp.Conn, 0777)
			if err != nil {
				logq.LogPutqueue(log.Error("MkdirAll err:", err))
				JsonResult(rw, http.StatusBadRequest, cmd.InternalError, "hdfs make dir failed.", nil)
				return
			}
		}

		if err != nil {
			logq.LogPutqueue(log.Error(err, sdpDirName))
			JsonResult(rw, http.StatusBadRequest, cmd.InternalError, err.Error(), nil)
			return
		} else {
			struDp.Conn = strings.TrimRight(struDp.Conn, "/")
			sql_dp_insert := fmt.Sprintf(`insert into DH_DP (DPID, DPNAME, DPTYPE, DPCONN, STATUS)
					values (null, '%s', '%s', '%s', 'A')`, struDp.Name, struDp.Type, struDp.Conn)
			if _, e := g_ds.Insert(sql_dp_insert); err != nil {
				//os.Remove(sdpDirName)  //don't delete it. It is maybe used by others
				logq.LogPutqueue(log.Error(e))
				JsonResult(rw, http.StatusBadRequest, cmd.ErrorSqlExec, e.Error(), nil)
				return
			}

			JsonResult(rw, http.StatusOK, cmd.ResultOK,
				fmt.Sprintf("Datapool has been created successfully. Name:%s Type:%s Path:%s.", struDp.Name, struDp.Type, sdpDirName), nil)
			return
		}
	}
}

const sqlSelectDpCount = "SELECT COUNT(*) FROM DH_DP WHERE STATUS='A'"

func getDatapoolCount() (count int64, err error) {
	row, err := g_ds.QueryRow(sqlSelectDpCount)
	if err != nil {
		return
	}
	row.Scan(&count)
	return
}

func getDatapools(count int64, sqlstr string) (err error, existflag bool, dps []cmd.FormatDp) {
	rows, err := g_ds.QueryRows(sqlstr)
	if err != nil {
		log.Error(err)
		return
	}
	defer rows.Close()

	dps = []cmd.FormatDp{}
	onedp := cmd.FormatDp{}
	existflag = false
	for rows.Next() {
		existflag = true
		rows.Scan(&onedp.Name, &onedp.Type, &onedp.Conn)
		dps = append(dps, onedp)
	}
	return
}

func dpGetAllHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	r.ParseForm()

	count, err := getDatapoolCount()
	if err != nil {
		logq.LogPutqueue(log.Error(err))
		JsonResult(rw, http.StatusInternalServerError, cmd.ErrorSqlExec, err.Error(), nil)
		return
	}

	sqlstr := ""
	if size, _ := strconv.Atoi(r.Form.Get("size")); size == -1 {
		sqlstr = fmt.Sprintf(`SELECT DPNAME, DPTYPE, DPCONN FROM DH_DP WHERE STATUS = 'A'`)
	} else {
		offset, limit := optionalOffsetAndSize(r, 10, 1, 100)
		validateOffsetAndLimit(int64(count), &offset, &limit)

		sqlstr = fmt.Sprintf(`SELECT DPNAME, DPTYPE, DPCONN FROM DH_DP 
			WHERE STATUS = 'A' ORDER BY DPID DESC 
			LIMIT %v OFFSET %v`, limit, offset)
	}

	err, existflag, dps := getDatapools(count, sqlstr)
	if err != nil {
		log.Error(err)
		JsonResult(rw, http.StatusInternalServerError, cmd.ErrorSqlExec, "", nil)
		return
	}
	if existflag == false {
		log.Info(MsgOfNoDatapool)
		JsonResult(rw, http.StatusOK, cmd.ErrorNoRecord, MsgOfNoDatapool, nil)
	} else {
		JsonResult(rw, http.StatusOK, cmd.ResultOK, "", newQueryListResult(count, dps))
	}
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
		result.Code = cmd.ErrorNoRecord
		WriteResp(rw, result, msg)
		log.Error("Datahub:", result.Code, "Msg:", result.Msg)
		return
	}

	sqlDp := fmt.Sprintf(`SELECT DPID, DPTYPE, DPCONN FROM DH_DP 
		WHERE STATUS = 'A' AND DPNAME = '%s'`, dpname)
	rowdp, err := g_ds.QueryRow(sqlDp)
	if err != nil {
		SqlExecError(rw, result, err.Error())
		return
	}

	var dpid int
	onedp.Items = make([]cmd.Item, 0, 16)
	onedp.Name = dpname
	rowdp.Scan(&dpid, &onedp.Type, &onedp.Conn)
	if dpid > 0 {
		//Use "left out join" to get repository/dataitem records, whether it has tags or not.
		//B.STATUS='A'
		sqlTag := fmt.Sprintf(`SELECT A.REPOSITORY, A.DATAITEM, A.ITEMDESC, A.PUBLISH, A.CREATE_TIME,
				B.TAGNAME, B.DETAIL, B.CREATE_TIME, B.COMMENT
				FROM DH_DP_RPDM_MAP A LEFT JOIN DH_RPDM_TAG_MAP B
				ON (A.RPDMID = B.RPDMID)
				WHERE A.DPID = %v AND A.STATUS='A' `, dpid)
		tagrows, err := g_ds.QueryRows(sqlTag)
		if err != nil {
			SqlExecError(rw, result, err.Error())
			return
		}
		defer tagrows.Close()

		var repoitemtime string
		for tagrows.Next() {
			item := cmd.Item{}
			tagrows.Scan(&item.Repository, &item.DataItem, &item.ItemDesc, &item.Publish, &repoitemtime, &item.Tag, &item.TagDetail, &item.Time, &item.TagComment)
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

func dpGetOtherDataHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	dpname := ps.ByName("dpname")
	dpid, dptype, dpconn := GetDataPoolInfo(dpname)
	if len(dpconn) == 0 || dpid == 0 || len(dptype) == 0 {
		log.Info("Datapool:", dpname, "does not exist.")
		JsonResult(w, http.StatusOK, cmd.ErrorDatapoolNotExits, "The datapool does not exist.", nil)
		return
	}

	itemslocation := make(map[string]string)
	err := GetItemslocationInDatapool(itemslocation, dpname, dpid, dpconn)
	if err != nil {
		log.Error("The datapool does not exist.", err)
		JsonResult(w, http.StatusOK, cmd.ErrorItemNotExist, "Get dataitem location error.", nil)
		return
	}

	datapool, err := dpdriver.New(dptype)
	if err != nil {
		l := log.Error(err.Error())
		logq.LogPutqueue(l)
		HttpNoData(w, http.StatusInternalServerError, cmd.ErrorDatapoolNotExits, err.Error())
		return
	}

	allotherdata := make([]ds.DpOtherData, 0, 4)

	err = datapool.GetDpOtherData(&allotherdata, itemslocation, dpconn)
	if err != nil {
		JsonResult(w, http.StatusInternalServerError, cmd.ResultOK, err.Error(), allotherdata)
		return
	}

	log.Info("allotherdata", allotherdata)

	JsonResult(w, http.StatusOK, cmd.ResultOK, "OK", allotherdata)
}

func checkDpConnectHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	reqbody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	var paras ds.DpParas
	err = json.Unmarshal(reqbody, &paras)
	if err != nil {
		fmt.Println(err)
		return
	}

	var connstr string
	if paras.Dptype == "hdfs" {
		connstr = paras.Host + ":" + paras.Port
	}

	datapool, err := dpdriver.New(paras.Dptype)
	if err != nil {
		fmt.Println(err)
		return
	}

	isNormal, err := datapool.CheckDpConnect(paras.Dpconn, connstr)

	if err == nil && isNormal == true {
		JsonResult(w, http.StatusOK, cmd.ResultOK, "datapool连接正常", nil)
	} else {
		JsonResult(w, http.StatusOK, cmd.ErrorDpConnect, "datapool连接不正常", nil)
	}
}
