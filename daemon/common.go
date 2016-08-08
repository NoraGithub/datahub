package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

func Env(name string, required bool) string {
	s := os.Getenv(name)
	if required && s == "" {
		panic("env variable required, " + name)
	}
	log.Infof("[env][%s] %s\n", name, s)
	return s
}

func EnvDebug(name string, required bool) string {
	s := os.Getenv(name)
	if required && s == "" {
		panic("env variable required, " + name)
	}
	log.Debugf("[env][%s] %s\n", name, s)
	return s
}

func CheckDataPoolExist(datapoolname string) (bexist bool) {
	sqlcheck := fmt.Sprintf("SELECT COUNT(1) FROM DH_DP WHERE DPNAME='%s' AND STATUS='A'", datapoolname)
	row, err := g_ds.QueryRow(sqlcheck)
	if err != nil {
		log.Error("CheckDataPoolExist QueryRow error:", err.Error())
		return
	} else {
		var num int
		row.Scan(&num)
		if num == 0 {
			return false
		} else {
			return true
		}
	}
}

func CheckItemExist(repo, item string) (bool, error) {
	log.Println("check item exist in db")

	sql := fmt.Sprintf(`SELECT COUNT(1) FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, repo, item)
	row, err := g_ds.QueryRow(sql)
	if err != nil {
		log.Error("CheckItemExist QueryRow error:", err.Error())
		return false, err
	}
	var count int
	err = row.Scan(&count)
	if err != nil {
		log.Error("CheckItemExist Scan error:", err.Error())
		return false, err
	}
	if count == 0 {
		return false, err
	} else {
		return true, err
	}
}

func GetDataPoolDpconn(datapoolname string) (dpconn string) {
	sqlgetdpconn := fmt.Sprintf("SELECT DPCONN FROM DH_DP WHERE DPNAME='%s'  AND STATUS='A'", datapoolname)
	//fmt.Println(sqlgetdpconn)
	row, err := g_ds.QueryRow(sqlgetdpconn)
	if err != nil {
		l := log.Error("QueryRow error:", err)
		logq.LogPutqueue(l)
		return ""
	} else {
		row.Scan(&dpconn)
		return dpconn
	}
}

func GetDataPoolInfo(datapoolname string) (dpid int, dptype, dpconn string) {
	sqlget := fmt.Sprintf("SELECT DPID, DPTYPE, DPCONN FROM DH_DP WHERE DPNAME='%s'  AND STATUS='A'", datapoolname)
	//fmt.Println(sqlgetdpconn)
	row, err := g_ds.QueryRow(sqlget)
	if err != nil {
		l := log.Error("QueryRow error:", err)
		logq.LogPutqueue(l)
		return 0, "", ""
	} else {
		row.Scan(&dpid, &dptype, &dpconn)
		return dpid, dptype, dpconn
	}
}

func GetDataPoolDpconnAndDptype(datapoolname string) (dpconn, dptype string) {
	sqlgetdpconn := fmt.Sprintf("SELECT DPCONN, DPTYPE FROM DH_DP WHERE DPNAME='%s'  AND STATUS='A'", datapoolname)
	//fmt.Println(sqlgetdpconn)
	row, err := g_ds.QueryRow(sqlgetdpconn)
	if err != nil {
		l := log.Error("QueryRow error:", err)
		logq.LogPutqueue(l)
		return "", ""
	} else {
		row.Scan(&dpconn, &dptype)
		return dpconn, dptype
	}
}

func GetDpconnDpnameDptypeByDpid(dpid int) (dpconn, dpname, dptype string) {
	sSqlGetDpconn := fmt.Sprintf(`SELECT DPCONN, DPNAME, DPTYPE FROM DH_DP WHERE DPID=%d AND STATUS='A';`, dpid)
	row, err := g_ds.QueryRow(sSqlGetDpconn)
	if err != nil {
		l := log.Error("QueryRow error:", err)
		logq.LogPutqueue(l)
		return
	}
	row.Scan(&dpconn, &dpname, &dptype)
	return
}

func GetDataPoolDpid(datapoolname string) (dpid int) {
	sqlgetdpid := fmt.Sprintf("SELECT DPID FROM DH_DP WHERE DPNAME='%s'  AND STATUS='A'", datapoolname)
	//fmt.Println(sqlgetdpid)
	row, err := g_ds.QueryRow(sqlgetdpid)
	if err != nil {
		log.Println("GetDataPoolDpid QueryRow error:", err.Error())
		return
	} else {
		row.Scan(&dpid)
		return
	}
}

func InsertTagToDb(dpexist bool, p ds.DsPull, tagcom string) (err error) {
	if dpexist == false {
		return
	}
	DpId := GetDataPoolDpid(p.Datapool)
	if DpId == 0 {
		return
	}
	rpdmid := GetRepoItemId(p.Repository, p.Dataitem)

	if rpdmid == 0 {
		sqlInsertRpdm := fmt.Sprintf(`INSERT INTO DH_DP_RPDM_MAP
			(RPDMID ,REPOSITORY, DATAITEM, DPID, PUBLISH ,CREATE_TIME ,STATUS, ITEMDESC) 
		    VALUES (null, '%s', '%s', %d, 'N', datetime('now'), 'A', '%s')`,
			p.Repository, p.Dataitem, DpId, p.ItemDesc)
		g_ds.Insert(sqlInsertRpdm)
		rpdmid = GetRepoItemId(p.Repository, p.Dataitem)
	}
	sqlInsertTag := fmt.Sprintf(`INSERT INTO DH_RPDM_TAG_MAP(TAGID, TAGNAME ,RPDMID ,DETAIL,CREATE_TIME, STATUS, COMMENT) 
		VALUES (null, '%s', %d, '%s', datetime('now'), 'A', '%s')`,
		p.Tag, rpdmid, p.DestName, tagcom)
	log.Println(sqlInsertTag)
	_, err = g_ds.Insert(sqlInsertTag)
	return err
}

func GetRepoItemId(repository, dataitem string) (rpdmid int) {
	sqlgetrpdmId := fmt.Sprintf("SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A'",
		repository, dataitem)
	row, err := g_ds.QueryRow(sqlgetrpdmId)
	if err != nil {
		log.Println("GetRepoItemId QueryRow error:", err.Error())
		return
	} else {
		row.Scan(&rpdmid)
		return
	}
}

func InsertItemToDb(repo, item, datapool, itemdesc, createTime string) (err error) {
	log.Println("Insert item to db")
	dpid := GetDataPoolDpid(datapool)
	if dpid > 0 {
		//sqlInsertItem := fmt.Sprintf(`INSERT INTO DH_DP_RPDM_MAP (RPDMID, REPOSITORY, DATAITEM, ITEMDESC, DPID, PUBLISH, CREATE_TIME, STATUS)
		//	VALUES (null, '%s', '%s', '%s', %d, 'Y',  datetime('now'), 'A')`, repo, item, itemdesc, dpid)
		sqlInsertItem := fmt.Sprintf(`INSERT INTO DH_DP_RPDM_MAP (RPDMID, REPOSITORY, DATAITEM, ITEMDESC, DPID, PUBLISH, CREATE_TIME, STATUS)
			VALUES (null, '%s', '%s', '%s', %d, 'Y', '%s', 'A')`, repo, item, itemdesc, dpid, createTime)
		_, err = g_ds.Insert(sqlInsertItem)
		log.Println(sqlInsertItem)

	} else {
		err = errors.New("dpid is not found")
	}
	return err
}

func GetDataPoolStatusByID(dpid int) (status string) {
	sqlGetDpStatus := fmt.Sprintf("SELECT STATUS FROM DH_DP WHERE DPID=%d", dpid)
	row, err := g_ds.QueryRow(sqlGetDpStatus)
	if err != nil {
		log.Println(sqlGetDpStatus)
		log.Println(err.Error())
		return
	}
	row.Scan(&status)
	if status != "A" {
		log.Println("dpid:", dpid, " status:", status)
	}
	return
}

func GetRpdmidDpidItemdesc(repo, item string) (rpdmid, dpid int, Itemdesc string) {
	sqlGetRpdmidDpidItemdesc := fmt.Sprintf("SELECT RPDMID, DPID, ITEMDESC FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A'", repo, item)
	row, err := g_ds.QueryRow(sqlGetRpdmidDpidItemdesc)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	row.Scan(&rpdmid, &dpid, &Itemdesc)
	status := GetDataPoolStatusByID(dpid)
	if rpdmid == 0 || dpid == 0 || len(Itemdesc) == 0 {
		log.Println("rpdmid, dpid, Itemdesc :", rpdmid, dpid, Itemdesc)
		log.Println("datapool status:", status)
	}
	if status != "A" {
		return 0, 0, ""
	}
	return
}

func CheckTagExist(repo, item, tag string) (exits bool, err error) {
	rpdmid, dpid, _ := GetRpdmidDpidItemdesc(repo, item)
	if rpdmid == 0 || dpid == 0 {
		l := log.Errorf("dataitem is not exist, %s/%s, rpdmid:%d, dpid:%d", repo, item, rpdmid, dpid)
		logq.LogPutqueue(l)
		return false, errors.New(fmt.Sprintf("Dataitem '%s' not found.", item))
	}
	sqlCheckTag := fmt.Sprintf("SELECT COUNT(1) FROM DH_RPDM_TAG_MAP WHERE RPDMID=%d AND TAGNAME='%s' AND STATUS='A'", rpdmid, tag)
	row, err := g_ds.QueryRow(sqlCheckTag)
	var count int
	row.Scan(&count)
	if count > 0 {
		return true, nil
	}
	return
}

func GetDpnameDpconnItemdesc(repo, item string) (dpname, dpconn, dptype, itemDesc string) {
	_, dpid, itemDesc := GetRpdmidDpidItemdesc(repo, item)
	if dpid == 0 {
		log.Println(" dpid==0")
		return
	}
	dpname, dpconn, dptype = GetDpnameDpconnDptypeByDpid(dpid)
	return
}

func GetDpnameDpconnDptypeByDpid(dpid int) (dpname, dpconn, dptype string) {
	sqlgetdpcontent := fmt.Sprintf("SELECT DPNAME ,DPCONN, DPTYPE FROM DH_DP WHERE DPID=%d  AND STATUS='A'", dpid)

	row, err := g_ds.QueryRow(sqlgetdpcontent)
	if err != nil {
		log.Println("GetDpnameDpconnDptypeByDpid QueryRow error:", err.Error())
		return
	} else {
		row.Scan(&dpname, &dpconn, &dptype)
		return
	}
	return
}

func InsertPubTagToDb(repo, item, tag, fileName, comment string) (err error) {
	log.Println("Insert pub tag to db.")
	rpdmid := GetRepoItemId(repo, item)
	if rpdmid == 0 {
		return errors.New("Dataitem is not found which need to be published before publishing tag. ")
	}
	sqlInsertTag := fmt.Sprintf("INSERT INTO DH_RPDM_TAG_MAP (TAGID, TAGNAME, RPDMID, DETAIL, CREATE_TIME, STATUS, COMMENT) VALUES (null, '%s', %d, '%s', datetime('now'), 'A', '%s');", tag, rpdmid, fileName, comment)
	log.Println(sqlInsertTag)
	_, err = g_ds.Insert(sqlInsertTag)
	if err != nil {
		l := log.Error("Insert tag into db error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return
}

func rollbackInsertPubTagToDb(reponame, itemname, tagname string) error {
	log.Println("rollback insert pub tag from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)
	var rpdmId int

	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}
	row.Scan(&rpdmId)

	sql := fmt.Sprintf(`DELETE FROM DH_RPDM_TAG_MAP WHERE RPDMID=%d AND TAGNAME='%s' AND STATUS='A';`, rpdmId, tagname)
	_, err = g_ds.Delete(sql)
	if err != nil {
		l := log.Error("rollback pub tag delete tag from DH_RPDM_TAG_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return nil
}

func rollbackInsertPubItemToDb(reponame, itemname string) error {
	log.Println("rollback insert pub item from db")
	sql := fmt.Sprintf(`DELETE FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)
	_, err := g_ds.Delete(sql)
	if err != nil {
		l := log.Error("rollback pub tag delete item from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return err
}

func GetItemDesc(Repository, Dataitem string) (ItemDesc string, err error) {
	getItemDesc := fmt.Sprintf("SELECT ITEMDESC FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A'", Repository, Dataitem)
	//log.Println(ItemDesc)
	row, err := g_ds.QueryRow(getItemDesc)
	if err != nil {
		l := log.Error("QueryRow error:", err)
		logq.LogPutqueue(l)
		return "", err
	} else {
		row.Scan(&ItemDesc)
		return ItemDesc, err
	}
}

func CreateTable() (err error) {
	if g_ds.DbType == "mysql" {
		_, err = g_ds.Create(ds.Create_dh_dp_mysql)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.Create_dh_dp_repo_ditem_map_mysql)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.Create_dh_repo_ditem_tag_map_mysql)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.CreateDhDaemon_mysql)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.CreateDhJob_mysql)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}

		_, err = g_ds.Create(ds.CreateMsgTagAdded_mysql)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
	} else {
		_, err = g_ds.Create(ds.Create_dh_dp)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.Create_dh_dp_repo_ditem_map)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.Create_dh_repo_ditem_tag_map)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.CreateDhDaemon)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
		_, err = g_ds.Create(ds.CreateDhJob)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}

		_, err = g_ds.Create(ds.CreateMsgTagAdded)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return err
		}
	}

	return
}

func UpdateSql16To17() (err error) {
	sqlm := `ALTER TABLE DH_RPDM_TAG_MAP ADD COMMENT VARCHAR(256);`
	_, err = g_ds.Exec(sqlm)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	return nil
}

func GetAllTagDetails(monitList *map[string]string) (e error) {
	sqlDp := `SELECT DPID, DPCONN FROM DH_DP WHERE DPTYPE='file' AND STATUS = 'A';`
	rDps, e := g_ds.QueryRows(sqlDp)
	if e != nil {
		return e
	}
	defer rDps.Close()
	var conn string
	var dpid int
	for rDps.Next() {
		rDps.Scan(&dpid, &conn)
		sqlItem := fmt.Sprintf(`SELECT RPDMID, REPOSITORY, DATAITEM, ITEMDESC 
			FROM DH_DP_RPDM_MAP 
			WHERE STATUS='A' AND PUBLISH='Y' AND DPID = %v;`, dpid)
		rItems, e := g_ds.QueryRows(sqlItem)
		if e != nil {
			return e
		}
		defer rItems.Close()
		var id int
		var repo, item, desc string
		for rItems.Next() {
			rItems.Scan(&id, &repo, &item, &desc)
			k := repo + "/" + item + ":"
			v := conn + "/" + desc + "/"
			sqlTag := fmt.Sprintf(`SELECT TAGNAME, DETAIL FROM DH_RPDM_TAG_MAP 
				WHERE STATUS='A' AND RPDMID=%v`, id)
			rTags, e := g_ds.QueryRows(sqlTag)
			if e != nil {
				return e
			}
			defer rTags.Close()
			var tagname, detail string
			for rTags.Next() {
				rTags.Scan(&tagname, &detail)
				tag := k + tagname
				file := v + detail
				//log.Info("--------------->", tag, "----------------->", file)
				(*monitList)[file] = tag
			}
		}

	}
	return e
}

func GetTagDetail(rpdmid int, tag string) (detail string) {
	sSqlGetTagDetail := fmt.Sprintf(`SELECT DETAIL FROM DH_RPDM_TAG_MAP 
        WHERE RPDMID=%d AND TAGNAME = '%s' AND STATUS='A'`, rpdmid, tag)
	row, err := g_ds.QueryRow(sSqlGetTagDetail)
	if err != nil {
		l := log.Error(err.Error())
		logq.LogPutqueue(l)
	}
	row.Scan(&detail)
	log.Println("tagdetail", detail)
	return detail
}

func InsertToTagadded(EventTime time.Time, Repname, Itemname, Tag string, status int) (err error) {
	log.Debugf("Insert into MSG_TAGADDED time:%s, repo:%s, item:%s, tag:%s, status:%d",
		EventTime, Repname, Itemname, Tag, status)
	sql := fmt.Sprintf(`INSERT INTO MSG_TAGADDED (ID, REPOSITORY, DATAITEM, TAG, STATUS, CREATE_TIME, STATUS_TIME) 
		VALUES (null, '%s', '%s', '%s', %d, '%s',datetime('now'));`,
		Repname, Itemname, Tag, status, EventTime.Format("2006-01-02 15:04:05"))
	_, err = g_ds.Insert(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
	}
	return err
}

func GetTagFromMsgTagadded(Repository, DataItem string, Status int) (Tags map[int]string) {
	sql := fmt.Sprintf(`SELECT ID, TAG FROM MSG_TAGADDED 
		WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS=%d;`,
		Repository, DataItem, Status)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return
	}
	defer rows.Close()

	var tag string
	var ID int
	Tags = make(map[int]string)
	for rows.Next() {
		rows.Scan(&ID, &tag)
		log.Println("ID, tag:", ID, tag)
		Tags[ID] = tag
	}
	return Tags
}

func UpdateStatMsgTagadded(ID, Status int) (err error) {

	log.Info("update MSG_TAGADDED status")
	sql := fmt.Sprintf(`UPDATE MSG_TAGADDED SET STATUS=%d 
		WHERE ID=%d;`, Status, ID)
	_, err = g_ds.Update(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return
	}
	return
}

func getDaemonid() (id string) {
	log.Println("TODO get daemonid from db.")
	s := `SELECT DAEMONID FROM DH_DAEMON;`
	row, e := g_ds.QueryRow(s)
	if e != nil {
		l := log.Error(s, "error.", e)
		logq.LogPutqueue(l)
		return
	}
	row.Scan(&id)
	log.Info("daemon id is", id)
	return id
}

func saveDaemonID(id string) {
	log.Println("TODO save daemonid to db when srv returns code 0.")
	count := `SELECT COUNT(*) FROM DH_DAEMON;`
	row, err := g_ds.QueryRow(count)
	if err != nil {
		l := log.Error(count, "error.", err)
		logq.LogPutqueue(l)
	}
	var c int
	row.Scan(&c)
	if c > 0 {
		Update := fmt.Sprintf(`UPDATE DH_DAEMON SET DAEMONID='%s';`, id)
		log.Debug(Update)
		if _, e := g_ds.Update(Update); e != nil {
			l := log.Error(Update, "error.", e)
			logq.LogPutqueue(l)
		}
	} else {
		Insert := fmt.Sprintf(`INSERT INTO DH_DAEMON (DAEMONID) VALUES ('%s');`, id)
		log.Debug(c, Insert)
		if _, e := g_ds.Insert(Insert); e != nil {
			l := log.Error(Insert, "error.", e)
			logq.LogPutqueue(l)
		}
	}
}

func getEntryPoint() (ep string) {
	log.Println("TODO get ep from db")
	s := `SELECT ENTRYPOINT FROM DH_DAEMON;`
	r, e := g_ds.QueryRow(s)
	if e != nil {
		l := log.Error(s, "error.", e)
		logq.LogPutqueue(l)
		return
	}
	r.Scan(&ep)
	return ep
}

func saveEntryPoint(ep string) {
	log.Println("TODO save ep to db")
	count := `SELECT COUNT(*) FROM DH_DAEMON;`
	row, err := g_ds.QueryRow(count)
	if err != nil {
		l := log.Error(count, "error.", err)
		logq.LogPutqueue(l)
	}
	var c int
	row.Scan(&c)
	if c > 0 {
		Update := fmt.Sprintf(`UPDATE DH_DAEMON SET ENTRYPOINT='%s';`, ep)
		log.Debug(Update)
		if _, e := g_ds.Update(Update); e != nil {
			l := log.Error(Update, "error.", e)
			logq.LogPutqueue(l)
		}
	} else {
		Insert := fmt.Sprintf(`INSERT INTO DH_DAEMON (ENTRYPOINT) VALUES ('%s');`, ep)
		log.Debug(c, Insert)
		if _, e := g_ds.Insert(Insert); e != nil {
			l := log.Error(Insert, "error.", e)
			logq.LogPutqueue(l)
		}
	}
}

func delEntryPoint() {
	log.Println("TODO remove ep from db.")
	d := `UPDATE DH_DAEMON SET ENTRYPOINT = '';`
	if _, e := g_ds.Update(d); e != nil {
		l := log.Error(d, "error.", e)
		logq.LogPutqueue(l)
	}
}

func GetDaemonRoleByPubRecord() (role int) {
	sql := `SELECT COUNT(*) FROM DH_DP_RPDM_MAP WHERE PUBLISH='Y' AND STATUS='A' 
	        AND DPID IN (SELECT DPID FROM DH_DP WHERE STATUS='A');`
	row := g_ds.Db.QueryRow(sql)

	var count int
	row.Scan(&count)
	if count > 0 {
		role = PUBLISHER
		log.Debug("This datahub daemon is a publisher.")
	} else {
		role = PULLER
		log.Debug("This datahub daemon is a puller.")
	}
	return
}

func buildResp(code int, msg string, data interface{}) (body []byte, err error) {
	r := ds.Response{}

	r.Code = code
	r.Msg = msg
	r.Data = data

	return json.Marshal(r)

}

func GetLocalfilePath() (localfilepath []string) {
	sql := `SELECT DISTINCT DPCONN, ITEMDESC FROM DH_DP A, DH_DP_RPDM_MAP B
							WHERE A.DPID=B.DPID AND A.DPTYPE='file'
									    AND A.STATUS='A'
									    AND B.PUBLISH='Y'
									    AND B.STATUS='A';`
	//dpci = make(map[string] string)

	var conn string
	var desc string
	localfilepath = make([]string, 0)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error("QueryRow error:", err)
		logq.LogPutqueue(l)
		return
	} else {
		for rows.Next() {
			rows.Scan(&conn, &desc)
			path := conn + "/" + desc
			localfilepath = append(localfilepath, path)
		}
		return
	}
}

func delItem(reponame, itemname string) (err error) {
	log.Println("Begin to remove item from db")
	sql := fmt.Sprintf(`UPDATE DH_DP_RPDM_MAP SET STATUS = 'N' WHERE REPOSITORY='%s' AND DATAITEM='%s';`, reponame, itemname)
	if _, err := g_ds.Update(sql); err != nil {
		l := log.Error("delete item error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return nil
}

func delTagsForDelItem(reponame, itemname string) error {
	log.Println("Begin to remove tags for remove item from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)

	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}
	var rpdmId int
	row.Scan(&rpdmId)
	if rpdmId == 0 {
		log.Debug(reponame, itemname, "not exist.")
		return nil
	}
	sqldeltag := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='N' WHERE RPDMID=%d`, rpdmId)
	_, err = g_ds.Update(sqldeltag)
	log.Info("sqldeltag", sqldeltag)
	if err != nil {
		l := log.Error("delete tag error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return nil
}

func rollbackDelItem(reponame, itemname string) error {
	log.Println("TODO rollback delete item from db")
	sql := fmt.Sprintf(`UPDATE DH_DP_RPDM_MAP SET STATUS='A' WHERE REPOSITORY='%s' AND DATAITEM='%s';`, reponame, itemname)
	if _, err := g_ds.Update(sql); err != nil {
		l := log.Error("rollback delete item error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return nil
}

func rollbackDelTags(reponame, itemname string) error {
	log.Println("TODO rollback delete tags for item from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s';`, reponame, itemname)
	var rpdmId int
	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}
	row.Scan(&rpdmId)
	sqlrollback := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='A' WHERE RPDMID=%d`, rpdmId)
	_, err = g_ds.Update(sqlrollback)
	if err != nil {
		l := log.Error("rollback delete tags error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return nil
}

func delTag(reponame, itemname, tagname string) (int, error) {
	log.Println("TODO  delete tag from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)
	var rpdmId int
	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return 0, err
	}
	row.Scan(&rpdmId)

	sql := fmt.Sprintf(`SELECT TAGID FROM DH_RPDM_TAG_MAP WHERE STATUS='A' AND TAGNAME='%s' AND RPDMID=%d`, tagname, rpdmId)
	var tagid int
	row, err = g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error("select tagid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return 0, err
	}
	row.Scan(&tagid)

	sql = fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='N' WHERE TAGNAME='%s' AND RPDMID=%d`, tagname, rpdmId)
	_, err = g_ds.Update(sql)
	if err != nil {
		l := log.Error("delete tag from DH_RPDM_TAG_MAP error:", err)
		logq.LogPutqueue(l)
		return 0, err
	}

	return tagid, nil
}

func rollbackDelTag(tagid int) error {
	log.Println("TODO rollback delete tag from db")
	log.Println(tagid)
	sql := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='A' WHERE TAGID=%d`, tagid)
	_, err := g_ds.Update(sql)
	if err != nil {
		l := log.Error("rollback delete tag error:", err)
		logq.LogPutqueue(l)
		return err
	}
	return nil
}

func getBatchDelTagsName(reponame, itemname, tagname string) ([]string, error) {
	log.Println("Batch delete tags from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)
	var rpdmId int

	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return nil, err
	}
	row.Scan(&rpdmId)

	tagname = strings.Replace(tagname, "*", "%", -1)
	log.Println(tagname)
	sql := fmt.Sprintf(`SELECT TAGNAME FROM DH_RPDM_TAG_MAP WHERE TAGNAME LIKE '%s' AND RPDMID=%d AND STATUS='A';`, tagname, rpdmId)
	//var tagnames []string
	tagsname := make([]string, 0)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error("batch delete tag from DH_RPDM_TAG_MAP error:", err)
		logq.LogPutqueue(l)
		return nil, err
	}
	for rows.Next() {
		rows.Scan(&tagname)
		tagsname = append(tagsname, tagname)
	}
	log.Println(tagsname)
	/*if len(tagsname) == 0 {
		return nil, errors.New("没有匹配的tag")
	}*/
	return tagsname, nil

}

func batchDelTags(reponame, itemname, tagname string) (map[int]string, error) {
	log.Println("Batch delete tags from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)
	var rpdmId int

	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return nil, err
	}
	row.Scan(&rpdmId)

	tagname = strings.Replace(tagname, "*", "%", -1)
	log.Println(tagname)
	sql := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='N' WHERE TAGNAME LIKE '%s' AND RPDMID=%d`, tagname, rpdmId)
	_, err = g_ds.Update(sql)
	if err != nil {
		l := log.Error("batch delete tag from DH_RPDM_TAG_MAP error:", err)
		logq.LogPutqueue(l)
		return nil, err
	}

	sql = fmt.Sprintf(`SELECT TAGID, TAGNAME FROM DH_RPDM_TAG_MAP WHERE TAGNAME LIKE '%s' AND RPDMID=%d`, tagname, rpdmId)
	//var tagnames []string
	tagnameidmap := make(map[int]string)
	var tagid int
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error("batch delete tag from DH_RPDM_TAG_MAP error:", err)
		logq.LogPutqueue(l)
		return nil, err
	}
	for rows.Next() {
		rows.Scan(&tagid, &tagname)
		tagnameidmap[tagid] = tagname
	}
	log.Println(tagnameidmap)
	return tagnameidmap, nil
}

/*UpdateSql04To05()  Temporarily not use*/
func UpdateSql04To05() (err error) {
	//UPDATE DH_DP
	TrimRightDpconn := `update DH_DP set DPCONN =substr(DPCONN,0,length(DPCONN)) where DPCONN like '%/';`
	_, err = g_ds.Update(TrimRightDpconn)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	UpDhDp := `UPDATE DH_DP SET DPCONN=DPCONN||"/"||DPNAME;`
	_, err = g_ds.Update(UpDhDp)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}

	//UPDATE DH_DP_RPDM_MAP
	RenameDpRpdmMap := "ALTER TABLE DH_DP_RPDM_MAP RENAME TO OLD_DH_DP_RPDM_MAP;"
	_, err = g_ds.Exec(RenameDpRpdmMap)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	_, err = g_ds.Create(ds.Create_dh_dp_repo_ditem_map)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	InsertDpRpdmMap := `INSERT INTO DH_DP_RPDM_MAP(RPDMID, REPOSITORY, DATAITEM, DPID, ITEMDESC
						, PUBLISH, CREATE_TIME, STATUS) 
						SELECT RPDMID, REPOSITORY, DATAITEM, DPID, REPOSITORY||"/"||DATAITEM, 
						PUBLISH, CREATE_TIME, 'A' FROM OLD_DH_DP_RPDM_MAP;`
	DropOldDpRpdmMap := `DROP TABLE OLD_DH_DP_RPDM_MAP;`
	_, err = g_ds.Insert(InsertDpRpdmMap)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	_, err = g_ds.Drop(DropOldDpRpdmMap)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}

	//UPDATE DH_RPDM_TAG_MAP
	RenameTagMap := "ALTER TABLE DH_RPDM_TAG_MAP RENAME TO OLD_DH_RPDM_TAG_MAP;"
	_, err = g_ds.Exec(RenameTagMap)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	_, err = g_ds.Create(ds.Create_dh_repo_ditem_tag_map)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	InsertTagMap := `INSERT INTO DH_RPDM_TAG_MAP(TAGID, TAGNAME, RPDMID, DETAIL, CREATE_TIME, STATUS) 
					SELECT NULL, TAGNAME, RPDMID, DETAIL, CREATE_TIME, 'A' FROM OLD_DH_RPDM_TAG_MAP;`
	DropOldTagMap := `DROP TABLE OLD_DH_RPDM_TAG_MAP;`
	_, err = g_ds.Insert(InsertTagMap)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	_, err = g_ds.Drop(DropOldTagMap)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	log.Info("update db successfully!")
	return
}

/*UpgradeSql07To08()  Temporarily not use*/
func UpgradeSql07To08() (err error) {
	var RetDhJob string
	row, err := g_ds.QueryRow(ds.SQLIsExistTableDhJob)
	if err != nil {
		l := log.Error("Get TABLE Dh_Job error!")
		logq.LogPutqueue(l)
		return err
	}
	row.Scan(&RetDhJob)
	if len(RetDhJob) > 1 {
		if false == strings.Contains(RetDhJob, "ACCESSTOKEN") {
			return AlterDhJob()
		}
	}
	return nil
}

/*AlterDhJob()  Temporarily not use*/
func AlterDhJob() (err error) {
	sqltoken := `ALTER TABLE DH_JOB ADD ACCESSTOKEN VARCHAR(20);`
	_, err = g_ds.Exec(sqltoken)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	sqlep := `ALTER TABLE DH_JOB ADD ENTRYPOINT VARCHAR(128);`
	_, err = g_ds.Exec(sqlep)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return err
	}
	return nil
}

func GetItemslocationInDatapool(itemslocation map[string]string, dpname string, dpid int, dpconn string) error {

	sql := fmt.Sprintf("SELECT DISTINCT ITEMDESC, REPOSITORY, DATAITEM FROM DH_DP_RPDM_MAP WHERE DPID=%v AND STATUS='A';", dpid)
	log.Debug(sql)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Errorf("datapool name %s, dpid %v, dpconn %v, error:%v", dpname, dpid, dpconn, err)
		logq.LogPutqueue(l)
		return err
	}

	var location, repo, item string
	for rows.Next() {
		rows.Scan(&location, &repo, &item)
		log.Debug(location, repo, item)
		itemslocation[location] = repo + "/" + item
	}
	log.Trace(itemslocation)
	return err
}

func getRepoCountByDp(datapool, status string) int64 {
	if status == "published" {
		status = "Y"
	} else {
		status = "N"
	}

	sql := fmt.Sprintf(`SELECT COUNT(DISTINCT REPOSITORY) 
		FROM DH_DP_RPDM_MAP 
		WHERE DPID IN
		(SELECT DPID FROM DH_DP WHERE DPNAME = '%s' AND STATUS='A')
		AND PUBLISH= '%s' 
		AND STATUS = 'A';`, datapool, status)

	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return 0
	}

	var count int64
	row.Scan(&count)
	log.Debug("Published repository count:", count)
	return count
}

func GetRepoInfo(dpName, status string, offset int64, limit int) ([]ds.RepoInfo, error) {

	if status == "published" {
		status = "Y"
	} else {
		status = "N"
	}

	sql := fmt.Sprintf(`SELECT DPID FROM DH_DP WHERE DPNAME = '%s' AND STATUS = 'A';`, dpName)
	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var dpid int
	row.Scan(&dpid)

	sql = fmt.Sprintf(`SELECT DISTINCT REPOSITORY 
		FROM DH_DP_RPDM_MAP 
		WHERE DPID = %d AND PUBLISH = '%s' AND STATUS = 'A'
		ORDER BY RPDMID  
		LIMIT %v OFFSET %v;`, dpid, status, limit, offset)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var repository string
	var itemCount int
	repoinfo := ds.RepoInfo{}
	repoInfos := make([]ds.RepoInfo, 0)
	for rows.Next() {
		rows.Scan(&repository)

		repoinfo.RepositoryName = repository
		sql = fmt.Sprintf(`SELECT COUNT(*) FROM DH_DP_RPDM_MAP WHERE REPOSITORY = '%s' AND PUBLISH = '%s' AND STATUS = 'A';`, repository, status)
		row, err := g_ds.QueryRow(sql)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return nil, err
		}

		row.Scan(&itemCount)
		repoinfo.ItemCount = itemCount

		repoInfos = append(repoInfos, repoinfo)
	}

	return repoInfos, err
}

func getItemCountByDpRepo(datapool, repo, isPublished string) (count int64) {
	sql := fmt.Sprintf(`SELECT COUNT(*) 
		FROM DH_DP_RPDM_MAP 
		WHERE DPID IN
		(SELECT DPID FROM DH_DP WHERE DPNAME = '%s' AND STATUS='A')
		AND REPOSITORY = '%s'
		AND PUBLISH = '%s' 
		AND STATUS = 'A';`, datapool, repo, isPublished)

	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return 0
	}

	row.Scan(&count)
	log.Debugf("Dataitems count of repository %v: %v", repo, count)

	return
}

func GetPublishedRepoInfo(dpName, repoName string, offset int64, limit int) ([]ds.PublishedItemInfo, error) {

	var publishedItemInfo ds.PublishedItemInfo
	publishedItemInfos := make([]ds.PublishedItemInfo, 0)

	sql := fmt.Sprintf(`SELECT DPID, DPCONN FROM DH_DP WHERE DPNAME = '%s' AND STATUS = 'A';`, dpName)

	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var dpconn string
	var dpid int
	row.Scan(&dpid, &dpconn)

	sql = fmt.Sprintf(`SELECT DATAITEM, CREATE_TIME, ITEMDESC 
		FROM DH_DP_RPDM_MAP 
		WHERE DPID = %d AND REPOSITORY = '%s' AND PUBLISH = 'Y' AND STATUS = 'A' 
		ORDER BY RPDMID 
		LIMIT %v OFFSET %v;`, dpid, repoName, limit, offset)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	for rows.Next() {
		rows.Scan(&publishedItemInfo.ItemName, &publishedItemInfo.CreateTime, &publishedItemInfo.Location)
		publishedItemInfos = append(publishedItemInfos, publishedItemInfo)
	}

	return publishedItemInfos, err
}

func GetPulledRepoInfo(dpName, repoName string, offset int64, limit int) ([]ds.PulledItemInfo, error) {

	var pulledItemInfo ds.PulledItemInfo
	pulledItemInfos := make([]ds.PulledItemInfo, 0)

	sql := fmt.Sprintf(`SELECT DPID FROM DH_DP WHERE DPNAME = '%s' AND STATUS = 'A';`, dpName)
	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var dpid int
	row.Scan(&dpid)

	sql = fmt.Sprintf(`SELECT DATAITEM ,ITEMDESC
		FROM DH_DP_RPDM_MAP 
		WHERE DPID = %d AND REPOSITORY = '%s' AND PUBLISH = 'N' AND STATUS = 'A' 
		ORDER BY RPDMID 
		LIMIT %v OFFSET %v;`, dpid, repoName, limit, offset)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var dataitem string
	result := ds.Result{}
	pages := ds.ResultPages{}
	orderInfoSlice := []ds.OrderInfo{}
	pages.Results = &orderInfoSlice
	result.Data = &pages

	for rows.Next() {
		rows.Scan(&pulledItemInfo.ItemName, &pulledItemInfo.Location)

		path := "/api/subscriptions/pull/" + repoName + "/" + dataitem
		resp, err := commToServerGetRsp("get", path, nil)
		if err != nil {
			l := log.Error(err)
			logq.LogPutqueue(l)
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusUnauthorized {
			pulledItemInfo.SignTime = nil
			log.Debug("resp.StatusCode == http.StatusUnauthorized")
		} else if resp.StatusCode != http.StatusOK {
			err = errors.New("request subscriptions api failed.")
			l := log.Error(err)
			logq.LogPutqueue(l)
			return nil, err
		} else {
			respbody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				l := log.Error(err)
				logq.LogPutqueue(l)
				return nil, err
			} else {
				err = json.Unmarshal(respbody, &result)
				if err != nil {
					err = errors.New("unmarshal failed.")
					l := log.Error(err)
					logq.LogPutqueue(l)
					return nil, err
				}
				for _, v := range orderInfoSlice {
					pulledItemInfo.SignTime = &v.Signtime

					log.Debug("pulledItemInfo.SignTime:", pulledItemInfo.SignTime)
				}
			}
		}
		pulledItemInfos = append(pulledItemInfos, pulledItemInfo)
	}

	log.Debug(pulledItemInfos)

	return pulledItemInfos, err
}

func GetPulledTagsOfItemInfo(dpname, repo, item string, offset int64, limit int) ([]ds.PulledTagsOfItem, error) {

	pulledTagOfItem := ds.PulledTagsOfItem{}
	pulledTagsOfItem := make([]ds.PulledTagsOfItem, 0)

	sql := fmt.Sprintf(`SELECT DPID FROM DH_DP WHERE DPNAME = '%s' AND STATUS = 'A';`, dpname)
	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var dpid int
	row.Scan(&dpid)

	sql = fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP
				WHERE REPOSITORY  = '%s'
				AND DATAITEM = '%s'
				AND DPID = %d
				AND PUBLISH = 'N'
				AND STATUS = 'A';`, repo, item, dpid)

	row, err = g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var rpdmid int
	row.Scan(&rpdmid)

	sql = fmt.Sprintf(`SELECT TAGNAME, CREATE_TIME, COMMENT FROM DH_RPDM_TAG_MAP WHERE RPDMID = %d AND STATUS = 'A' LIMIT %v OFFSET %v;`, rpdmid, limit, offset)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	for rows.Next() {
		rows.Scan(&pulledTagOfItem.TagName, &pulledTagOfItem.DownloadTime, &pulledTagOfItem.Content)
		pulledTagsOfItem = append(pulledTagsOfItem, pulledTagOfItem)
	}

	return pulledTagsOfItem, err
}

func getPulledTagCount(datapool, repo, item string) (int64, error) {

	sql := fmt.Sprintf(`SELECT COUNT(*)
		FROM DH_RPDM_TAG_MAP
		WHERE RPDMID = (SELECT RPDMID FROM DH_DP_RPDM_MAP
					WHERE REPOSITORY  = '%s'
					AND DATAITEM = '%s'

					AND PUBLISH = 'N'
					AND STATUS = 'A'
					AND DPID = (SELECT DPID FROM DH_DP WHERE DPNAME = '%s' AND STATUS='A'))
		AND STATUS = 'A';`, repo, item, datapool)

	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return 0, err
	}

	var count int64
	row.Scan(&count)
	log.Debug("Published repository count:", count)
	return count, err
}

func GetPublishedTagsOfItemInfo(dpname, repo, item string, offset int64, limit int) ([]ds.PublishedTagsOfItem, error) {

	publishedTagOfItem := ds.PublishedTagsOfItem{}
	publishedTagsOfItem := make([]ds.PublishedTagsOfItem, 0)

	sql := fmt.Sprintf(`SELECT DPID, DPCONN FROM DH_DP WHERE DPNAME = '%s' AND STATUS = 'A';`, dpname)
	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var dpid int
	var dpconn string
	row.Scan(&dpid, &dpconn)

	sql = fmt.Sprintf(`SELECT RPDMID, ITEMDESC FROM DH_DP_RPDM_MAP
				WHERE REPOSITORY  = '%s'
				AND DATAITEM = '%s'
				AND DPID = %d
				AND PUBLISH = 'Y'
				AND STATUS = 'A';`, repo, item, dpid)

	row, err = g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	var rpdmid int
	var itemdesc string
	row.Scan(&rpdmid, &itemdesc)

	sql = fmt.Sprintf(`SELECT DETAIL, TAGNAME, CREATE_TIME FROM DH_RPDM_TAG_MAP WHERE RPDMID = %d AND STATUS = 'A';`, rpdmid)
	rows, err := g_ds.QueryRows(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return nil, err
	}

	for rows.Next() {
		rows.Scan(&publishedTagOfItem.FileName, &publishedTagOfItem.TagName, &publishedTagOfItem.PublishTime)
		publishedTagOfItem.Location = dpconn + "/" + itemdesc + "/" + publishedTagOfItem.FileName
		publishedTagOfItem.Status = "已发布"
		publishedTagsOfItem = append(publishedTagsOfItem, publishedTagOfItem)
	}

	publishedTagOfItem = ds.PublishedTagsOfItem{}
	localfiles := ScanLocalFile(dpconn + "/" + itemdesc)
	var i int
	for j := 0; j <  len(localfiles); j++ {
		for i = 0; i < len(publishedTagsOfItem); i++ {
			if publishedTagsOfItem[i].Location == localfiles[j] {
				break
			}
		}
		if i >= len(publishedTagsOfItem) {
			dirs := strings.Split(localfiles[j], "/")
			publishedTagOfItem.Location = localfiles[j]
			publishedTagOfItem.FileName = dirs[len(dirs) - 1]
			publishedTagOfItem.Status = "未发布"
			publishedTagsOfItem = append(publishedTagsOfItem, publishedTagOfItem)
		}
	}

	if offset+int64(limit) < int64(len(publishedTagsOfItem)) {
		return publishedTagsOfItem[offset:offset+int64(limit)], err
	} else {
		if offset < int64(limit) {
			return publishedTagsOfItem[(int64(len(publishedTagsOfItem)) - offset):], err
		} else {
			return publishedTagsOfItem[(len(publishedTagsOfItem) - limit):], err
		}
	}
}

func getPublishedTagCount(datapool, repo, item string) (int64, error) {

	sql := fmt.Sprintf(`SELECT DPID, DPCONN FROM DH_DP WHERE DPNAME = '%s' AND STATUS = 'A';`, datapool)
	row, err := g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return 0, err
	}

	var dpid int
	var dpconn string
	row.Scan(&dpid, &dpconn)

	sql = fmt.Sprintf(`SELECT RPDMID, ITEMDESC FROM DH_DP_RPDM_MAP
				WHERE REPOSITORY  = '%s'
				AND DATAITEM = '%s'
				AND DPID = %d
				AND PUBLISH = 'Y'
				AND STATUS = 'A';`, repo, item, dpid)

	row, err = g_ds.QueryRow(sql)
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return 0, err
	}

	var rpdmid int
	var itemdesc string
	row.Scan(&rpdmid, &itemdesc)

	path := dpconn + "/" + itemdesc

	return 	int64(len(ScanLocalFile(path))), err
}
