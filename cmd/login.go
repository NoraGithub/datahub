package cmd

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

type UserForJson struct {
	Username string `json:"username", omitempty`
}

type Loginerr struct {
	Retrytimes string `json:"retry_times"`
	Ttltimes   string `json:"ttl_times"`
}

func Login(login bool, args []string) (err error) {
	fmt.Printf("login: ")
	reader := bufio.NewReader(os.Stdin)
	//loginName, _ := reader.ReadString('\n')
	loginName, _ := reader.ReadBytes('\n')

	loginName = bytes.TrimRight(loginName, "\r\n")
	fmt.Printf("password: ")
	pass := utils.GetPasswd(true) // Silent, for *'s use gopass.GetPasswdMasked()
	//fmt.Printf("[%s]:[%s]\n", string(loginName), string(pass))

	User.userName = string(loginName)
	//User.password = string(pass)
	User.password = fmt.Sprintf("%x", md5.Sum(pass))

	User.b64 = base64.StdEncoding.EncodeToString([]byte(User.userName + ":" + User.password))
	//fmt.Printf("%s\n%s:%s\n", User.b64, User.userName, User.password)

	//req.Header.Set("Authorization", "Basic "+os.Getenv("DAEMON_USER_AUTH_INFO"))
	userJson := UserForJson{Username: User.userName}
	jsondata, _ := json.Marshal(userJson)

	resp, err := commToDaemon("get", "/users/auth", jsondata) //users/auth
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()
	//fmt.Println("login return", resp.StatusCode)
	if resp.StatusCode == http.StatusOK {
		Logged = true
		if login {
			fmt.Println("DataHub : login success.")
		}
		return
	} else if resp.StatusCode == http.StatusForbidden {

		result := &ds.Result{}
		objloginerr := &Loginerr{}
		result.Data = objloginerr
		body, _ := ioutil.ReadAll(resp.Body)
		if err = json.Unmarshal(body, result); err != nil {
			return err
		} else {
			retrytimes, _ := strconv.Atoi(objloginerr.Retrytimes)
			leftchance := 5 - retrytimes
			switch leftchance {
			case 0:
				fmt.Printf("%s\nno chance left.\n", result.Msg)
			case 1:
				fmt.Printf("%s\n1 chance left.\n", result.Msg)
			default:
				fmt.Printf("%s\n%v chances left.\n", result.Msg, leftchance)
			}
			fmt.Println("Error : login failed.")
			return
		}
	} else {
		if /*resp.StatusCode == 401 &&*/ login {
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println("login failed. ", string(body))
		}
		fmt.Println("Error : login failed.")
		return
	}
	/*
		body, _ := ioutil.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			//fmt.Println(resp.StatusCode, ShowMsgResp(body, false))
			fmt.Println(resp.StatusCode)
		}

		if resp.StatusCode == 401 {
			return fmt.Errorf(string(body))
		}
		return fmt.Errorf("ERROR %d: login failed.", resp.StatusCode)
	*/
}
func loginUsage() {
	fmt.Printf("Usage: %s no parameter\n\nSend a login request to the datahub server using your user name and password\n", os.Args[0])
}
