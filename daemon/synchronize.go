package daemon

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"io/ioutil"
	"time"
)

var (
	ItemSliceA    = make([]string, 0)
	ItemSliceB    = make([]string, 0)
	TagSliceA     = make([]string, 0)
	TagSliceB     = make([]string, 0)
	diffItemSlice = make([]string, 0)
	diffTagSlice  = make([]string, 0)
)

func GetItemandTagFromServer() {
	path1 := "/api/repositories?myRelease=1&size=-1"

	resp1, err := commToServerGetRsp("get", path1, nil)
	if err != nil {
		log.Error(err)
		return
	}
	repo := ds.Repositories{}
	respbody1, _ := ioutil.ReadAll(resp1.Body)

	json.Unmarshal(respbody1, &repo)

	RepoSlice := []string{repo.RepositoryName}
	defer resp1.Body.Close()
	a := len(RepoSlice)
	for i := 0; i < a; i++ {
		path2 := "/api/repositories/" + RepoSlice[i] + "?myRelease=1&size=-1"
		resp2, err := commToServerGetRsp("get", path2, nil)
		if err != nil {
			log.Error(err)
			return
		}
		item := ds.Repository{}

		respbody2, _ := ioutil.ReadAll(resp2.Body)

		json.Unmarshal(respbody2, &item)

		ItemSliceA = item.DataItems
		defer resp2.Body.Close()
		b := len(ItemSliceA)
		for j := 0; j < b; j++ {
			path3 := "/api/repositories/" + RepoSlice[i] + "/" + ItemSliceA[j] + "?size=-1"
			resp3, err := commToServerGetRsp("get", path3, nil)
			if err != nil {
				log.Error(err)
				return
			}
			tag := ds.Tag{}

			respbody3, _ := ioutil.ReadAll(resp3.Body)

			json.Unmarshal(respbody3, &tag)

			TagSliceA = []string{tag.Tag}
			defer resp3.Body.Close()
		}
	}
}
func GetItemandTagFromSqlite() (e error) {
	sqlDp := `SELECT DPID, DPCONN FROM DH_DP WHERE DPTYPE='file' AND STATUS = 'A';`
	rDps, e := g_ds.QueryRows(sqlDp)
	if e != nil {
		return e
	}
	defer rDps.Close()
	var dpid int
	for rDps.Next() {
		rDps.Scan(&dpid)
		sqlItem := fmt.Sprintf(`SELECT DATAITEM FROM DH_DP_RPDM_MAP 
			WHERE STATUS='A' AND PUBLISH='Y' AND DPID = %v;`, dpid)
		rItems, e := g_ds.QueryRows(sqlItem)
		if e != nil {
			return e
		}
		defer rItems.Close()
		var id int
		var item string
		for rItems.Next() {
			rItems.Scan(&id, &item)
			ItemSliceB = []string{item}
			sqlTag := fmt.Sprintf(`SELECT TAGNAME FROM DH_RPDM_TAG_MAP 
				WHERE STATUS='A' AND RPDMID=%v`, id)
			rTags, e := g_ds.QueryRows(sqlTag)
			if e != nil {
				return e
			}
			defer rTags.Close()
			var tagname string
			for rTags.Next() {
				rTags.Scan(&tagname)
				TagSliceB = []string{tagname}
			}
		}
	}
	return e
}

func CompareItemSlice(a []string, b []string) {
	lengthA := len(a)
	lengthB := len(b)
	var j int
	//diffItemSlice := make([]string, 0)
	for i := 0; i < lengthB; i++ {
		for j = 0; j < lengthA; j++ {
			if b[i] == a[j] {
				break
			}
		}
		if j == lengthA {
			diffItemSlice = append(diffItemSlice, b[i])
		}

	}
	return
}

func CompareTagSlice(a []string, b []string) {
	lengthA := len(a)
	lengthB := len(b)
	var j int
	for i := 0; i < lengthB; i++ {
		for j = 0; j < lengthA; j++ {
			if b[i] == a[j] {
				break
			}

		}
		if j == lengthA {
			diffTagSlice = append(diffTagSlice, b[i])
		}

	}

	return
}
func AlterItemStatus(c []string) (err error) {
	sqlItem := fmt.Sprintf(`UPDATE DH_DP_RPDM_MAP SET STATUS = 'N'
			WHERE STATUS='A' AND DATAITEM=%v`, c)
	_, e := g_ds.Update(sqlItem)
	if e != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return
	}
	return
}

func AlterTagStatus(d []string) (err error) {
	sqlTag := fmt.Sprintf(`UPDATE DH_RPDM_TAG_MAP SET STATUS = 'N'
				WHERE STATUS='A' AND TAGNAME=%v`, d)
	_, e := g_ds.Update(sqlTag)
	if e != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
		return
	}
	return
}

func Synchronization() {

	log.Debug("BEGIN")

	timer := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-timer.C:
			now := time.Now()
			if now.Hour() == 1 {
				log.Debug("Time:", now)
				GetItemandTagFromServer()
				GetItemandTagFromSqlite()
				CompareItemSlice(ItemSliceA, ItemSliceB)
				CompareTagSlice(TagSliceA, TagSliceB)
				AlterItemStatus(diffItemSlice)
				AlterTagStatus(diffTagSlice)
				time.Sleep(time.Hour * 24)
			}
		}
	}
	log.Debug("END")
}
