package cmd

import (
	"fmt"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"net/http"
)

func DpRm(needLogin bool, args []string) (err error) {
	f := mflag.NewFlagSet("dp rm", mflag.ContinueOnError)
	f.Usage = dprUseage
	if err = f.Parse(args); err != nil {
		return err
	}

	if len(args) > 0 && args[0][0] != '-' {
		for _, v := range args {
			dp := v
			if v[0] != '-' {

				resp, err := commToDaemon("DELETE", "/datapools/"+dp, nil)
				if err != nil {
					fmt.Println(err)
					return err
				}
				if resp.StatusCode == http.StatusOK {
					showResponse(resp)
				} else {
					showError(resp)
				}
				resp.Body.Close()
			}
		}
	}
	if len(args) == 0{
		fmt.Println("please write the dpname which you want to delete")
	}
	return nil
}

func dprUseage() {
	fmt.Println("Usage of datahub dp rm:")
	fmt.Println("  datahub dp rm DATAPOOL")
	fmt.Println("Remove a datapool")
}
