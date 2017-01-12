package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/utils/mflag"
	"io/ioutil"
	"net/http"
	"strings"
)

func Whoami(needlogin bool, args []string) (err error) {
	f := mflag.NewFlagSet("datahub whoami", mflag.ContinueOnError)
	f.Usage = whoamiUsage

	if len(args) <= 0 {
		ami()
		return
	}
	token := f.Bool([]string{"-token", "t"}, true, "get token")

	if err := f.Parse(args); err != nil {
		return err
	}
	if len(args) == 1 {
		if strings.TrimSpace(args[0]) == "-t" || strings.TrimSpace(args[0]) == "--token" || strings.TrimSpace(args[0]) == "--token=false" || strings.TrimSpace(args[0]) == "--token=true" {
			if *token {
				resp, err := commToDaemon("get", "/whoami/token", nil)
				if err != nil {
					fmt.Println(err)
					return err
				}
				defer resp.Body.Close()
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				if resp.StatusCode == http.StatusOK {
					tokenstr := string(body)
					i := strings.Index(tokenstr, " ")
					lastWord := tokenstr[i+1 : len(tokenstr)-1]
					fmt.Println(lastWord)
				} else {
					fmt.Println("Please login !")
				}
			} else {
				ami()
			}
		} else {
			fmt.Println(ErrMsgArgument)
			whoamiUsage()
			return errors.New(ErrMsgArgument)
		}
	} else {
		fmt.Println(ErrMsgArgument)
		whoamiUsage()
		return errors.New(ErrMsgArgument)
	}
	return
}

func ami() error {
	resp, err := commToDaemon("get", "/whoami", nil)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	type Data struct {
		Data interface{} `json:"data,omitempty"`
	}
	data := Data{}
	json.Unmarshal(body, &data)

	if resp.StatusCode == http.StatusOK {
		fmt.Println((data.Data).(map[string]interface{})["username"])
	} else {
		fmt.Println("Please login !")
	}
	return nil
}

func whoamiUsage() {
	fmt.Println("Show information about the current session\n")
	fmt.Println("The default options for this command will return the currently authenticated user name or an empty string.\nOther flags support returning the currently used token or the user context.\n")
	fmt.Println("Usage:")
	fmt.Println("\tdatahub whoami [options]")
	fmt.Println("Options:")
	//fmt.Println("\t-c, --context=false: Print the current user context name")
	fmt.Println("\t-t, --token=false: Print the token the current session is using. This will return an error if you are using a different form of authentication.")
}
