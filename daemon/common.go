package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
)

func CheckDataPoolExist(datapoolname string) (bexist bool) {
	sqlcheck := fmt.Sprintf("SELECT COUNT(1) FROM DH_DP WHERE DPNAME='%s' AND STATUS='A'", datapoolname)
	row, err := g_ds.QueryRow(sqlcheck)
	//fmt.Println(sqlcheck)
	if err != nil {
		log.Println("CheckDataPoolExist QueryRow error:", err.Error())
		return
	} else {
		var num int
		row.Scan(&num)
		//fmt.Println("num:", num)
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
		log.Errorf(" QueryRow error:%s\n", err.Error())
		return
	} else {
		row.Scan(&dpconn)
		return dpconn
	}
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
	//fmt.Println("GetRepoItemId1", rpdmid, DpId)
	if rpdmid == 0 {
		sqlInsertRpdm := fmt.Sprintf(`INSERT INTO DH_DP_RPDM_MAP
			(RPDMID ,REPOSITORY, DATAITEM, DPID, PUBLISH ,CREATE_TIME ,STATUS, ITEMDESC) 
		    VALUES (null, '%s', '%s', %d, 'N', datetime('now'), 'A', '%s')`,
			p.Repository, p.Dataitem, DpId, p.ItemDesc)
		g_ds.Insert(sqlInsertRpdm)
		rpdmid = GetRepoItemId(p.Repository, p.Dataitem)
		//fmt.Println("GetRepoItemId2", rpdmid, DpId)
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
		fmt.Println("rpdmid, dpid ", rpdmid, dpid)
		return false, errors.New("repo and dataitem not exist")
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
		log.Errorf(" QueryRow error:%s\n", err.Error())
		return "", err
	} else {
		row.Scan(&ItemDesc)
		return ItemDesc, err
	}
}

func CreateTable() (err error) {
	_, err = g_ds.Create(ds.Create_dh_dp)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = g_ds.Create(ds.Create_dh_dp_repo_ditem_map)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = g_ds.Create(ds.Create_dh_repo_ditem_tag_map)
	if err != nil {
		log.Error(err)
		return err
	}
	return
}

func UpdateSql04To05() (err error) {
	//UPDATE DH_DP
	TrimRightDpconn := `update DH_DP set DPCONN =substr(DPCONN,0,length(DPCONN)) where DPCONN like '%/';`
	_, err = g_ds.Update(TrimRightDpconn)
	if err != nil {
		log.Error(err)
		return err
	}
	UpDhDp := `UPDATE DH_DP SET DPCONN=DPCONN||"/"||DPNAME;`
	_, err = g_ds.Update(UpDhDp)
	if err != nil {
		log.Error(err)
		return err
	}

	//UPDATE DH_DP_RPDM_MAP
	RenameDpRpdmMap := "ALTER TABLE DH_DP_RPDM_MAP RENAME TO OLD_DH_DP_RPDM_MAP;"
	_, err = g_ds.Exec(RenameDpRpdmMap)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = g_ds.Create(ds.Create_dh_dp_repo_ditem_map)
	if err != nil {
		log.Error(err)
		return err
	}
	InsertDpRpdmMap := `INSERT INTO DH_DP_RPDM_MAP(RPDMID, REPOSITORY, DATAITEM, DPID, ITEMDESC
						, PUBLISH, CREATE_TIME, STATUS) 
						SELECT RPDMID, REPOSITORY, DATAITEM, DPID, REPOSITORY||"/"||DATAITEM, 
						PUBLISH, CREATE_TIME, 'A' FROM OLD_DH_DP_RPDM_MAP;`
	DropOldDpRpdmMap := `DROP TABLE OLD_DH_DP_RPDM_MAP;`
	_, err = g_ds.Insert(InsertDpRpdmMap)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = g_ds.Drop(DropOldDpRpdmMap)
	if err != nil {
		log.Error(err)
		return err
	}

	//UPDATE DH_RPDM_TAG_MAP
	RenameTagMap := "ALTER TABLE DH_RPDM_TAG_MAP RENAME TO OLD_DH_RPDM_TAG_MAP;"
	_, err = g_ds.Exec(RenameTagMap)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = g_ds.Create(ds.Create_dh_repo_ditem_tag_map)
	if err != nil {
		log.Error(err)
		return err
	}
	InsertTagMap := `INSERT INTO DH_RPDM_TAG_MAP(TAGID, TAGNAME, RPDMID, DETAIL, CREATE_TIME, STATUS) 
					SELECT NULL, TAGNAME, RPDMID, DETAIL, CREATE_TIME, 'A' FROM OLD_DH_RPDM_TAG_MAP;`
	DropOldTagMap := `DROP TABLE OLD_DH_RPDM_TAG_MAP;`
	_, err = g_ds.Insert(InsertTagMap)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = g_ds.Drop(DropOldTagMap)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Info("update db successfully!")
	return
}

func GetTagDetails(monitList *map[string]string) (e error) {
	sqlDp := `SELECT DPID, DPCONN FROM DH_DP WHERE DPTYPE='file' AND STATUS = 'A';`
	rDps, e := g_ds.QueryRows(sqlDp)
	if e != nil {
		return e
	}
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
			var tagname, detail string
			for rTags.Next() {
				rTags.Scan(&tagname, &detail)
				k += tagname
				v += detail
				(*monitList)[k] = v
			}
		}

	}
	return e
}

func buildResp(code int, msg string, data interface{}) (body []byte, err error) {
	r := ds.Response{}

	r.Code = code
	r.Msg = msg
	r.Data = data

	return json.Marshal(r)

}
