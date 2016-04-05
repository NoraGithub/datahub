package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
)

func Job(needLogin bool, args []string) (err error) {

	f := mflag.NewFlagSet("datahub job", mflag.ContinueOnError)
	//fListall := f.Bool([]string{"-all", "a"}, false, "list all jobs")
	f.Usage = jobUsage
	path := "/job"
	if len(args) > 0 && len(args[0]) > 0 && args[0][0] != '-' {
		path += "/" + args[0]
	} else {
		if err := f.Parse(args); err != nil {
			return err
		}
		//if *fListall {
		//	path += "?opt=all"
		//}
	}

	resp, err := commToDaemon("GET", path, nil)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		showError(resp)
	} else {
		//body, _ := ioutil.ReadAll(resp.Body)
		//fmt.Println(string(body))
		jobResp(resp)
	}

	return err
}

func JobRm(needLogin bool, args []string) (err error) {

	f := mflag.NewFlagSet("datahub job rm", mflag.ContinueOnError)
	//fForce := f.Bool([]string{"-force", "f"}, false, "force cancel a pulling job.")
	fRmAll := f.Bool([]string{"-all"}, false, "rm all the jobs.")

	path := "/job"
	if len(args) > 0 && len(args[0]) > 0 && args[0][0] != '-' {
		path += "/" + args[0]

	} else if len(args) == 0 {
		jobUsage()
		return errors.New("Invalid arguments.")
	}

	//if len(args) > 0 && len(args[0]) > 0 {
	if err := f.Parse(args); err != nil {
		return err
	}
	//if *fForce {
	//	path += "?opt=force"
	//}
	//}

	if (path == "/job") && (*fRmAll == false) {
		jobUsage()
		return errors.New("Invalid arguments.")
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
	//mt.Println(resp.Header)
	return err
}

func jobUsage() {
	fmt.Println("Usage: datahub job [JOBID]")
	fmt.Println("List jobs")
	fmt.Println("\nUsage: datahub job rm [JOBID][--all]")
	fmt.Println("Remove a job")
}

func jobResp(resp *http.Response) {

	body, _ := ioutil.ReadAll(resp.Body)
	d := []ds.JobInfo{}
	result := ds.Result{Data: &d}
	err := json.Unmarshal(body, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		n, _ := fmt.Printf("%-8s\t%-20s\t%-10s\t%-10s\t%-10s\t%-10s\n", "JOBID", "STATUS", "DOWN", "TOTAL", "PERCENT", "TAG")
		printDash(n + 24)
		for _, job := range d {
			if job.Srcsize == 0 {
				fmt.Printf("%-8s\t%-20s\t%-10d\t%-10v\t%v\t%s\n", job.ID, job.Stat, job.Dlsize, "-", "-", job.Tag)
			} else {
				fmt.Printf("%-8s\t%-20s\t%-10d\t%-10d\t%.1f%%\t%s\n", job.ID, job.Stat, job.Dlsize, job.Srcsize, 100*float64(job.Dlsize)/float64(job.Srcsize), job.Tag)
			}

		}
	}
}
