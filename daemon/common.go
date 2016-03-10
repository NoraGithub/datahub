package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
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

func GetDpconnByDpid(dpid int) (dpconn string) {
	sSqlGetDpconn := fmt.Sprintf(`SELECT DPCONN FROM DH_DP WHERE DPID='%d'`, dpid)
	row, err := g_ds.QueryRow(sSqlGetDpconn)
	if err != nil {
		l := log.Error("QueryRow error:", err)
		logq.LogPutqueue(l)
		return ""
	}
	row.Scan(&dpconn)
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

func InsertTagToDb(dpexist bool, p ds.DsPull) (err error) {
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
	sqlInsertTag := fmt.Sprintf(`INSERT INTO DH_RPDM_TAG_MAP(TAGID, TAGNAME ,RPDMID ,DETAIL,CREATE_TIME, STATUS) 
		VALUES (null, '%s', '%d', '%s', datetime('now'), 'A')`,
		p.Tag, rpdmid, p.DestName)
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

func InsertItemToDb(repo, item, datapool, itemdesc string) (err error) {
	dpid := GetDataPoolDpid(datapool)
	if dpid > 0 {
		sqlInsertItem := fmt.Sprintf(`INSERT INTO DH_DP_RPDM_MAP (RPDMID, REPOSITORY, DATAITEM, ITEMDESC, DPID, PUBLISH, CREATE_TIME, STATUS)
			VALUES (null, '%s', '%s', '%s', %d, 'Y',  datetime('now'), 'A')`, repo, item, itemdesc, dpid)
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
	sqlCheckTag := fmt.Sprintf("SELECT COUNT(1) FROM DH_RPDM_TAG_MAP WHERE RPDMID='%d' AND TAGNAME='%s' AND STATUS='A'", rpdmid, tag)
	row, err := g_ds.QueryRow(sqlCheckTag)
	var count int
	row.Scan(&count)
	if count > 0 {
		return true, nil
	}
	return
}

func GetDpnameDpconnItemdesc(repo, item string) (dpname, dpconn, ItemDesc string) {
	_, dpid, ItemDesc := GetRpdmidDpidItemdesc(repo, item)
	if dpid == 0 {
		log.Println(" dpid==0")
		return "", "", ""
	}
	dpname, dpconn = GetDpnameDpconnByDpidAndStatus(dpid, "A")
	return
}

func GetDpnameDpconnByDpidAndStatus(dpid int, status string) (dpname, dpconn string) {
	sqlgetdpconn := fmt.Sprintf("SELECT DPNAME ,DPCONN FROM DH_DP WHERE DPID='%d'  AND STATUS='%s'", dpid, status)
	//fmt.Println(sqlgetdpconn)
	row, err := g_ds.QueryRow(sqlgetdpconn)
	if err != nil {
		log.Println("GetDpnameDpconnByDpidAndStatus QueryRow error:", err.Error())
		return
	} else {
		row.Scan(&dpname, &dpconn)
		return
	}
	return
}

func InsertPubTagToDb(repo, item, tag, FileName string) (err error) {
	rpdmid := GetRepoItemId(repo, item)
	if rpdmid == 0 {
		return errors.New("Dataitem is not found which need to be published before publishing tag. ")
	}
	sqlInsertTag := fmt.Sprintf("INSERT INTO DH_RPDM_TAG_MAP (TAGID, TAGNAME, RPDMID, DETAIL, CREATE_TIME, STATUS) VALUES (null, '%s', %d, '%s', datetime('now'), 'A')",
		tag, rpdmid, FileName)
	log.Println(sqlInsertTag)
	_, err = g_ds.Insert(sqlInsertTag)
	if err != nil {
		return err
	}
	return
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
	return
}

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
        WHERE RPDMID = '%d' AND TAGNAME = '%s' AND STATUS='A'`, rpdmid, tag)
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
			path := conn+"/"+desc
			localfilepath = append(localfilepath, path)
		}
		return
	}
}

func delItem(reponame, itemname string) (err error) {
	log.Println("TODO remove item from db")
	sql := fmt.Sprintf(`UPDATE DH_DP_RPDM_MAP SET STATUS = 'N' WHERE REPOSITORY='%s' AND DATAITEM='%s';`, reponame, itemname)
	if _, err := g_ds.Update(sql); err != nil {
		l := log.Error("delete item error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return nil
}

func delTagsForDelItem(reponame, itemname string) error {
	log.Println("TODO remove tags for remove item from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s';`, reponame, itemname)
	var rpdmId int

	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}
	row.Scan(&rpdmId)
	sqldeltag := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='N' WHERE RPDMID='%d'`, rpdmId)
	_, err = g_ds.Update(sqldeltag)
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
	sqlrollback := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='A' WHERE RPDMID='%d'`, rpdmId)
	_, err = g_ds.Update(sqlrollback)
	if err != nil {
		l := log.Error("rollback delete tags error:", err)
		logq.LogPutqueue(l)
		return err
	}

	return nil
}

func delTag(reponame, itemname, tagname string) error {
	log.Println("TODO  delete tag from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)
	var rpdmId int

	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}
	row.Scan(&rpdmId)
	sql := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='N' WHERE TAGNAME='%s' AND RPDMID='%d'`, tagname, rpdmId)
	_, err = g_ds.Update(sql)
	if err != nil {
		l := log.Error("delete tag from DH_RPDM_TAG_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}
	return nil
}

func rollbackDelTag(reponame, itemname, tagname string) error {
	log.Println("TODO rollback delete tag from db")
	sqlrpdmid := fmt.Sprintf(`SELECT RPDMID FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s' AND STATUS='A';`, reponame, itemname)
	var rpdmId int

	row, err := g_ds.QueryRow(sqlrpdmid)
	if err != nil {
		l := log.Error("select rpdmid from DH_DP_RPDM_MAP error:", err)
		logq.LogPutqueue(l)
		return err
	}
	row.Scan(&rpdmId)
	sql := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS='A' WHERE TAGNAME='%s' AND RPDMID='%d'`, tagname, rpdmId)
	_, err = g_ds.Update(sql)
	if err != nil {
		l := log.Error("rollback delete tag error:", err)
		logq.LogPutqueue(l)
		return err
	}
	return nil
}