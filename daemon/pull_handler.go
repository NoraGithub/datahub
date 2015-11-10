package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/julienschmidt/httprouter"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

func pullHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Println(r.URL.Path + "(pull)\n")
	result, _ := ioutil.ReadAll(r.Body)
	reqJson := ds.DsPull{}

	if err := json.Unmarshal(result, &reqJson); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	url := "/transaction/" + ps.ByName("repo") + "/" + ps.ByName("item") + "/" + reqJson.Tag
	//fmt.Fprintln(w, url)
	/*
		if token, err := getAccessToken(url, w); err != nil {

			return
		} else {
			url = "/pull/" + ps.ByName("repo") + "/" + ps.ByName("item") + "/" + reqJson.Tag + "?token=" + token
		}
	*/
	go dl(url, reqJson)
	return

}

func dl(uri string, p ds.DsPull) error {
	ip := os.Getenv("DAEMON_IP_PEER")
	fmt.Println(ip)
	if len(ip) == 0 {
		ip = "http://54.223.244.55:35800"
	}

	target := ip + uri
	fmt.Println(target)
	n, err := download(target, p)
	if err != nil {
		fmt.Printf("[%d bytes returned.]\n", n)
		fmt.Println(err)
	}
	return err
}

/*download routine, supports resuming broken downloads.*/
func download(url string, p ds.DsPull) (int64, error) {
	fmt.Printf("we are going to download %s, save to dp=%s,name=%s\n", url, p.Datapool, p.DestName)
	out, err := os.OpenFile("/var/lib/datahub/"+p.DestName, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return 0, err
	}
	defer out.Close()

	stat, err := out.Stat()
	if err != nil {
		return 0, err
	}
	out.Seek(stat.Size(), 0)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "go-downloader")
	/* Set download starting position with 'Range' in HTTP header*/
	req.Header.Set("Range", "bytes="+strconv.FormatInt(stat.Size(), 10)+"-")
	fmt.Printf("%v bytes had already been downloaded.\n", stat.Size())

	resp, err := http.DefaultClient.Do(req)

	/*Save response body to file only when HTTP 2xx received. TODO*/
	if err != nil || (resp != nil && resp.StatusCode/100 != 2) {
		if resp != nil {
			fmt.Println("http status code:", resp.StatusCode, err)
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println("response Body:", string(body))
		}
		return 0, err
	}
	defer resp.Body.Close()
	fname := resp.Header.Get("Source-Filename")
	if len(fname) > 0 {
		p.DestName = fname
	}

	n, err := io.Copy(out, resp.Body)
	if err != nil {
		return 0, err
	}
	fmt.Printf("%d bytes downloaded.", n)
	return n, nil
}

func getAccessToken(url string, w http.ResponseWriter) (token string, err error) {

	fmt.Println("daemon: connecting to", DefaultServer+url)
	req, err := http.NewRequest("POST", DefaultServer+url, nil)
	if len(loginAuthStr) > 0 {
		req.Header.Set("Authorization", loginAuthStr)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		//w.WriteHeader(http.StatusServiceUnavailable)
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		w.WriteHeader(resp.StatusCode)
		body, _ := ioutil.ReadAll(resp.Body)
		w.Write(body)
		return "", errors.New(string(body))
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		type tk struct {
			t string `json:"accesstoken,omitempty"`
		}
		t := tk{}
		if err = json.Unmarshal(body, &t); err != nil {
			return "", err
		} else {
			if len(t.t) > 0 {
				return t.t, nil
			}

		}

	}
	return "", errors.New("get access token error.")

}