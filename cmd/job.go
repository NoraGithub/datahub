package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
)

func Job(needLogin bool, args []string) (err error) {

	f := mflag.NewFlagSet("datahub job", mflag.ContinueOnError)
	fListall := f.Bool([]string{"-all", "a"}, false, "list all jobs")

	path := "/job"
	if len(args) > 0 && len(args[0]) > 0 && args[0][0] != '-' {
		path += "/" + args[0]
	} else {
		if err := f.Parse(args); err != nil {
			return err
		}
		if *fListall {
			path += "?opt=all"
		}
	}

	resp, err := commToDaemon("GET", path, nil)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		showResponse(resp)
	} else {
		//body, _ := ioutil.ReadAll(resp.Body)
		//fmt.Println(string(body))
		jobResp(resp)
	}

	return err
}

func JobRm(needLogin bool, args []string) (err error) {

	f := mflag.NewFlagSet("datahub job rm", mflag.ContinueOnError)
	fForce := f.Bool([]string{"-force", "f"}, false, "force cancel a pulling job.")

	path := "/job"
	if len(args) > 0 && len(args[0]) > 0 && args[0][0] != '-' {
		path += "/" + args[0]

	}
	if len(args) > 1 && len(args[1]) > 0 {
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *fForce {
			path += "?opt=force"
		}
	}

	resp, err := commToDaemon("DELETE", path, nil)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		showResponse(resp)
	} else {
		showError(resp)
	}
	return err
}

func jobUsage() {
	fmt.Println("Usage: datahub job [-a]")
}

func jobResp(resp *http.Response) {

	body, _ := ioutil.ReadAll(resp.Body)
	d := []ds.JobInfo{}
	result := ds.Result{Data: &d}
	err := json.Unmarshal(body, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		n, _ := fmt.Printf("%-8s\t%-10s\t%-10s\t%-10s\t%-10s\t%-10s\n", "JOBID", "STATUS", "DOWN", "TOTAL", "PERCENT", "TAG")
		printDash(n + 24)
		for _, job := range d {
			if job.Srcsize == 0 {
				fmt.Printf("%-8s\t%-10s\t%-10d\t%-10v\t%v\t%s\n", job.ID, job.Stat, job.Dlsize, "-", "-", job.Tag)
			} else {
				fmt.Printf("%-8s\t%-10s\t%-10d\t%-10d\t%.1f%%\t%s\n", job.ID, job.Stat, job.Dlsize, job.Srcsize, 100*float64(job.Dlsize)/float64(job.Srcsize), job.Tag)
			}

		}
	}
}
