package daemon

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	g_dbfileU string = "/tmp/datahubUtest.db"
)

type Context struct {
	desc        string
	requestBody string
	ps          httprouter.Params
	rw          *httptest.ResponseRecorder
	out         string
	code        int
	d           interface{}
}

func init() {

	fmt.Println("connect to db sqlite3----unit-test", g_dbfileU)
	db, err := sql.Open("sqlite3", g_dbfileU)
	if err != nil {
		fmt.Println(err)
	}
	//defer db.Close()
	chk(err)
	g_ds.Db = db
	CreateTable()
}

var con = []Context{
	Context{
		desc: "1.create datapool--------",
		requestBody: `{
									"dpname":"datahub_unit_test",
									"dptype":"file",
									"dpconn":"/var/lib/datahub/datahub-Unit-Test"						
							 }`,
		d: cmd.FormatDpCreate{
			Name: "datahub_unit_test",
			Type: "file",
			Conn: "/var/lib/datahub/datahub-Unit-Test",
		},
		ps:  httprouter.Params{{"dpname", "datahub_unit_test"}},
		out: "Datapool has been created successfully. Name:datahub_unit_test Type:file Path:/var/lib/datahub/datahub-Unit-Test.",
	},
	Context{
		desc: "2.create dup datapool err--------",
		requestBody: `{
									"dpname":"datahub_unit_test",
									"dptype":"file",
									"dpconn":"/var/lib/datahub/datahub-Unit-Test"						
							 }`,
		d: cmd.FormatDpCreate{
			Name: "datahub_unit_test",
			Type: "file",
			Conn: "/var/lib/datahub/datahub-Unit-Test",
		},
		ps:  httprouter.Params{{"dpname", "datahub_unit_test"}},
		out: "'datahub_unit_test' already exists, please change another name.",
	},
	Context{
		desc: "3.create datapool with err json format--------",
		requestBody: `{
									"err json ",
									"dptype":"file",
									"dpconn":"/var/lib/datahub/datahub-Unit-Test"						
							 }`,
		out: "Invalid argument.",
	},
	Context{
		desc: "4.create datapool without dpname--------",
		requestBody: `{
									"nnn":"datahub_unit_test",
									"dptype":"file",
									"dpconn":"/var/lib/datahub/datahub-Unit-Test"						
							 }`,
		out: "Invalid argument.",
	},
	Context{
		desc: "5.create datapool no conn --------",
		requestBody: `{
									"dpname":"datahub_unit_test2",
									"dptype":"file",
									"dpconn":""				
							 }`,
		d: cmd.FormatDpCreate{
			Name: "datahub_unit_test2",
			Type: "file",
		},
		ps:  httprouter.Params{{"dpname", "datahub_unit_test2"}},
		out: "Datapool has been created successfully. Name:datahub_unit_test2 Type:file Path:/var/lib/datahub.",
	},
	Context{
		desc: "6.create datapool with relative path--------",
		requestBody: `{
									"dpname":"datahub_unit_test_6",
									"dptype":"file",
									"dpconn":"datahub-Unit-Test-6"						
							 }`,
		d: cmd.FormatDpCreate{
			Name: "datahub_unit_test_6",
			Type: "file",
			Conn: "/var/lib/datahub/datahub-Unit-Test-6",
		},
		ps:  httprouter.Params{{"dpname", "datahub_unit_test_6"}},
		out: "Datapool has been created successfully. Name:datahub_unit_test_6 Type:file Path:/var/lib/datahub/datahub-Unit-Test-6.",
	},
}

func Test_dpPostOneHandler(t *testing.T) {

	for _, v := range con {
		req, _ := http.NewRequest("POST", "/datapools", strings.NewReader(v.requestBody))
		rw := httptest.NewRecorder()
		dpPostOneHandler(rw, req, v.ps)
		if !Expect(t, rw, v.out) {
			t.Logf("%s fail!", v.desc)
		} else {
			t.Logf("%s success.", v.desc)
		}
	}

}

func Test_dpGetAllHandler(t *testing.T) {
	req, _ := http.NewRequest("GET", "/datapools", strings.NewReader(""))
	rw := httptest.NewRecorder()
	dpGetAllHandler(rw, req, nil)
	if !ExpectResult(t, rw, "OK", cmd.ResultOK) {
		t.Logf("1.Get all datapools -------- fail")
	} else {
		t.Logf("1.Get all datapools -------- success")
	}
}

func Test_dpGetOneHandler(t *testing.T) {
	for _, v := range con {
		dpc, ok := v.d.(cmd.FormatDpCreate)
		if ok {
			req, _ := http.NewRequest("Get", fmt.Sprintf("/datapools/%s", dpc.Name), strings.NewReader(""))
			rw := httptest.NewRecorder()
			dpGetOneHandler(rw, req, v.ps)
			if !ExpectResult(t, rw, "", 0) {
				t.Logf("Get dp %s fail!", dpc.Name)
			} else {
				t.Logf("Get dp %s success.", dpc.Name)
			}
		}
	}
}

func Test_dpDeleteOneHandler(t *testing.T) {
	for _, v := range con {
		dpc, ok := v.d.(cmd.FormatDpCreate)
		if ok && v.desc != con[1].desc {
			req, _ := http.NewRequest("DELETE", fmt.Sprintf("/datapools/%s", dpc.Name), strings.NewReader(""))
			rw := httptest.NewRecorder()
			dpDeleteOneHandler(rw, req, v.ps)
			if !Expect(t, rw, fmt.Sprintf("Datapool %s removed successfully!", dpc.Name)) {
				t.Logf("delete dp %s fail!", dpc.Name)
			} else {
				t.Logf("delete dp %s success.", dpc.Name)
			}
		}
	}

	//remove all the datapools , to avoid breaking the unit test next time
	for _, v := range con {
		dpc, ok := v.d.(cmd.FormatDpCreate)
		if ok {
			if e := DeleteDpHard(dpc.Name); e != nil {
				t.Logf("DeleteDpHard error. %v", e)
			}
			fmt.Println("-----delete----", dpc.Name)
		}
	}
}

func Test_dpGetAllHandlerNoDp(t *testing.T) {
	req, _ := http.NewRequest("GET", "/datapools", strings.NewReader(""))
	rw := httptest.NewRecorder()
	dpGetAllHandler(rw, req, nil)
	if !ExpectResult(t, rw, "There isn't any datapool.", cmd.ErrorNoRecord) {
		t.Logf("1.Get all datapools when no datapool-------- fail")
	} else {
		t.Logf("1.Get all datapools when no datapool-------- success")
	}
}

func DeleteDpHard(dp string) (e error) {
	sqlDelDp := fmt.Sprintf("DELETE FROM DH_DP WHERE DPNAME='%s'", dp)
	_, e = g_ds.Delete(sqlDelDp)
	return e
}

func Expect(t *testing.T, rw *httptest.ResponseRecorder, out string) bool {
	msg := ds.MsgResp{}
	body, _ := ioutil.ReadAll(rw.Body)

	if err := json.Unmarshal(body, &msg); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(msg.Msg)
	}
	if msg.Msg != out {
		t.Errorf("expected http.Msg(%s) != return http.Msg(%s)", out, msg.Msg)
		return false
	}
	return true
}

func ExpectResult(t *testing.T, rw *httptest.ResponseRecorder, out string, code int) bool {
	var r = struct {
		Code int    `json:"code,omitempty"`
		Msg  string `json:"msg,omitempty"`
	}{}
	body, _ := ioutil.ReadAll(rw.Body)
	//fmt.Println("******after Readall", string(body))

	if err := json.Unmarshal(body, &r); err != nil {
		fmt.Println("ExpectResult", err)
	} else {
		fmt.Println(r.Msg)
	}
	if r.Code != code {
		t.Errorf("expected http.Code(%d) != return http.Code(%d)", code, r.Code)
		return false
	}
	if r.Msg != out {
		t.Errorf("expected http.Msg(%s) != return http.Msg(%s)", out, r.Msg)
		return false
	}
	return true
}
