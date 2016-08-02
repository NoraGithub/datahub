package daemon

import (
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"os"
	"testing"
)

var (
	Name  = "DatahubUnitTest"
	Type  = "file"
	Conn  = "/var/lib/datahub/datahubUnitTest"
	testP = ds.DsPull{
		Tag:        "tagTest",
		Datapool:   Name,
		DestName:   "tagTestD.txt",
		Repository: "repotest",
		Dataitem:   "itemtest",
		ItemDesc:   "dirrepoitemtest",
	}
)

func TestCheckDataPoolExist(t *testing.T) {
	b := CheckDataPoolExist("DatahubUnitTest")
	if b {
		t.Errorf("1.test CheckDataPoolExist fail--------")
	} else {
		t.Log("1.test CheckDataPoolExist success--------")
	}

	sqldpinsert := fmt.Sprintf(`insert into DH_DP (DPID, DPNAME, DPTYPE, DPCONN, STATUS)
					values (null, '%s', '%s', '%s', 'A')`, Name, Type, Conn)
	if _, err := g_ds.Insert(sqldpinsert); err != nil {
		os.Remove("/var/lib/datahub/datahubUnitTest")
		t.Error(err)
	}
	b = CheckDataPoolExist("DatahubUnitTest")
	if !b {
		t.Error("2.test CheckDataPoolExist fail--------")
	} else {
		t.Log("2.test CheckDataPoolExist success--------")
	}
}

func TestGetDataPoolDpconn(t *testing.T) {
	conn := GetDataPoolDpconn("DatahubUnitTest")
	if conn == Conn {
		t.Logf("1.GetDataPoolDpconn %s success--------\n", conn)
	} else {
		t.Errorf("1.test GetDataPoolDpconn %s fail--------\n", conn)
	}
}

func TestGetDataPoolDpid(t *testing.T) {
	if id := GetDataPoolDpid(Name); id == 0 {
		t.Errorf("1.GetDataPoolDpid fail--------dpid:%d\n", id)
	} else {
		t.Log("1.GetDataPoolDpid success--------dpid:", id)
	}
}

func TestInsertTagToDb(t *testing.T) {
	if e := InsertTagToDb(true, testP, "comment test"); e != nil {
		t.Error("1.InsertTagToDb fail--------", e)
	} else {
		t.Log("1.InsertTagToDb success--------")
	}
}

func TestGetRepoItemId(t *testing.T) {
	if id := GetRepoItemId(testP.Repository, testP.Dataitem); id == 0 {
		t.Errorf("1.GetRepoItemId fail-------- repo:%s, item:%s\n", testP.Repository, testP.Dataitem)
	} else {
		t.Logf("1.GetRepoItemId success-------- repo:%s, item:%s\n", testP.Repository, testP.Dataitem)
	}
}

func TestInsertItemToDb(t *testing.T) {
	if e := DeleteRepoItemHard(testP.Repository, testP.Dataitem); e != nil {
		t.Errorf("Recover db for TestInsertItemToDb test fail. Delete %s, %s error, %v", testP.Repository, testP.Dataitem, e)
	}
	if e := DeleteTagHard(testP.Tag); e != nil {
		t.Errorf("Recover db for TestInsertItemToDb test fail. Delete %s error, %v", testP.Tag, e)
	}
	err := InsertItemToDb(testP.Repository, testP.Dataitem, testP.Datapool, testP.ItemDesc, "")
	if err != nil {
		t.Error("1.InsertItemToDb fail-------- ", err)
	} else {
		t.Log("1.InsertItemToDb success---------")
	}
	if e := InsertTagToDb(true, testP, "comment lll"); e != nil {
		t.Error("InsertTagToDb fail--------", e)
	}

	err = InsertItemToDb(testP.Repository, testP.Dataitem, "No_this_datapool", testP.ItemDesc, "")
	if err != nil {
		t.Log("2.InsertItemToDb success:insert item to a non-existent datapool---------")
	} else {
		t.Error("2.InsertItemToDb fail:insert item to a non-existent datapool---------")
	}
}

func TestGetRpdmidDpidItemdesc(t *testing.T) {
	r, d, desc := GetRpdmidDpidItemdesc(testP.Repository, testP.Dataitem)
	if r == 0 || d == 0 {
		t.Errorf("1.TestGetRpdmidDpidItemdesc fail-------- rpdmid:%d, dpid:%d, desc:%s\n", r, d, desc)
	} else {
		t.Logf("1.TestGetRpdmidDpidItemdesc success-------- rpdmid:%d, dpid:%d, desc:%s\n", r, d, desc)
	}

	r, d, desc = GetRpdmidDpidItemdesc("No_this_repo", testP.Dataitem)
	if r == 0 && d == 0 {
		t.Logf("2.TestGetRpdmidDpidItemdesc success-------- rpdmid:%d, dpid:%d, desc:%s\n", r, d, desc)
	} else {
		t.Errorf("2.TestGetRpdmidDpidItemdesc fail-------- rpdmid:%d, dpid:%d, desc:%s\n", r, d, desc)
	}
}

func TestCheckTagExist(t *testing.T) {
	exist, e := CheckTagExist(testP.Repository, testP.Dataitem, testP.Tag)
	if exist == false || e != nil {
		t.Errorf("1.CheckTagExist fail-------- exist:%v, e:%v\n", exist, e)
	} else {
		t.Log("1.CheckTagExist success--------")
	}

	exist, e = CheckTagExist("No_this_repo", testP.Dataitem, testP.Tag)
	if exist == false && e != nil {
		t.Logf("2.CheckTagExist with a non-existent repository success-------- exist:%v, e:%v\n", exist, e)
	} else {
		t.Error("2.CheckTagExist with a non-existent repository fail--------")
	}
}

func TestGetDpnameDpconnItemdesc(t *testing.T) {
	dpname, dpconn, dptype, desc := GetDpnameDpconnItemdesc(testP.Repository, testP.Dataitem)
	if dpname == testP.Datapool && dpconn == Conn && desc == testP.ItemDesc && dptype == Type {
		t.Log("1.GetDpnameDpconnItemdesc success--------")
	} else {
		t.Error("1.GetDpnameDpconnItemdesc fail--------", dpname, dpconn, desc)
	}

	dpname, dpconn, dptype, desc = GetDpnameDpconnItemdesc("No_this_repo", testP.Dataitem)
	if dpname == "" && dpconn == "" && desc == "" && dptype == "" {
		t.Log("2.GetDpnameDpconnItemdesc with a non-existent repository success--------")
	} else {
		t.Error("2.GetDpnameDpconnItemdesc with a non-existent repository fail--------", dpname, dpconn, desc)
	}
}

func TestInsertPubTagToDb(t *testing.T) {
	tag := "tagTest2"
	file := "tagTest2file.csv"
	e := InsertPubTagToDb(testP.Repository, testP.Dataitem, tag, file, "comment TestInsertPubTagToDb")
	if e == nil {
		t.Log("1.InsertPubTagToDb success--------")
	} else {
		t.Error("1.InsertPubTagToDb fail--------", e)
	}
	if e := DeleteTagHard(tag); e != nil {
		t.Errorf("Recover db for InsertPubTagToDb test fail. Delete %s error, %v", tag, e)
	}

	e = InsertPubTagToDb("No_this_repo", testP.Dataitem, tag, file, "comment aaa")
	if e != nil {
		t.Log("2.InsertPubTagToDb with a non-existent repository success--------")
	} else {
		t.Error("2.InsertPubTagToDb with a non-existent repository fail--------", e)
	}
}

func TestGetItemDesc(t *testing.T) {
	desc, e := GetItemDesc(testP.Repository, testP.Dataitem) //+`'AND STATUS='`
	if desc == testP.ItemDesc && e == nil {
		t.Log("1.GetItemDesc success--------")
	} else {
		t.Error("1.GetItemDesc fail--------", desc, e)
	}
}

func TestGetAllTagDetails(t *testing.T) {
	var list map[string]string = make(map[string]string)
	e := GetAllTagDetails(&list)
	if e != nil {
		t.Error("1.GetAllTagDetails fail--------", e)
	} else {
		t.Log("1.GetAllTagDetails success-------- list:", list)
	}
}

func TestUpdateSql04To05(t *testing.T) {
	if e := UpdateSql04To05(); e != nil {
		t.Error("1.UpdateSql04To05 fail--------", e)
	} else {
		t.Log("1.UpdateSql04To05 success--------")
	}
}

func Test_saveAndgetDaemonID(t *testing.T) {
	id := "decfr49fj3nd8ek8"
	saveDaemonID(id)
	retid := getDaemonid()
	if id != retid {
		t.Errorf("1.saveAndgetDaemonID fail-------- id:%v, retid:%v", id, retid)
	}

	upid := "asdfghjkqwertyu3"
	saveDaemonID(upid)
	retid = getDaemonid()
	if upid != retid {
		t.Errorf("2.saveAndgetDaemonID fail-------- upid:%v, retid:%v", upid, retid)
	}
}

func Test_saveEntryPoint(t *testing.T) {
	ep := "http://127.0.0.1:34567"
	saveEntryPoint(ep)
	retep := getEntryPoint()
	if ep == retep {
		t.Log("1.save and get entrypoint success--------")
	} else {
		t.Errorf("1.save and get entrypoint fail-------- ep:%v, retep:%v", ep, retep)
	}

	ep = "http://192.168.8.12:34444"
	saveEntryPoint(ep)
	retep = getEntryPoint()
	if ep == retep {
		t.Log("2.save and get entrypoint success--------")
	} else {
		t.Errorf("2.save and get entrypoint fail-------- ep:%v, retep:%v", ep, retep)
	}

	delEntryPoint()
}

func TestRecover(t *testing.T) {
	DeleteAllHard(Name, testP.Repository, testP.Dataitem, testP.Tag)
	t.Log("Recover over")
}

func DeleteAllHard(dp, repo, item, tag string) {
	if e := DeleteDpHard(dp); e != nil {
		log.Errorf("Recover db for common.go test fail. Delete %s error, %v", dp, e)
	}
	if e := DeleteRepoItemHard(repo, item); e != nil {
		log.Errorf("Recover db for common.go test fail. Delete %s, %s error, %v", repo, item, e)
	}
	if e := DeleteTagHard(tag); e != nil {
		log.Errorf("Recover db for common.go test fail. Delete %s error, %v", tag, e)
	}
}

func DeleteRepoItemHard(repo, item string) (e error) {
	sql := fmt.Sprintf("DELETE FROM DH_DP_RPDM_MAP WHERE REPOSITORY='%s' AND DATAITEM='%s'", repo, item)
	_, e = g_ds.Delete(sql)
	return e
}

func DeleteTagHard(tag string) (e error) {
	sql := fmt.Sprintf("DELETE FROM DH_RPDM_TAG_MAP WHERE TAGNAME='%s'", tag)
	_, e = g_ds.Delete(sql)
	return e
}
