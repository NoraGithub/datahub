package cmd

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils"
	"github.com/asiainfoLDP/datahub/utils/mflag"
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
	f := mflag.NewFlagSet("datahub login", mflag.ContinueOnError)
	f.Usage = loginUsage
	if err := f.Parse(args); err != nil {
		return err
	}
	if len(args) >= 1 {
		fmt.Println(ErrMsgArgument)
		loginUsage()
		return errors.New(ErrMsgArgument)
	}

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
			fmt.Println("DataHub : Login success.")
		}
		return
	} else if resp.StatusCode == http.StatusForbidden {

		result := &ds.Result{}
		objloginerr := &Loginerr{}
		result.Data = objloginerr
		body, _ := ioutil.ReadAll(resp.Body)
		if err = json.Unmarshal(body, result); err != nil {
			fmt.Println("Error :", err)
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
			fmt.Println(ErrMsgLogin)
			return errors.New(ErrMsgLogin)
		}
	} else {
		if /*resp.StatusCode == 401 &&*/ login {
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println("Login failed.", string(body))
		}
		fmt.Println(ErrMsgLogin)
		return errors.New(ErrMsgLogin)
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

func Logout(login bool, args []string) error {
	f := mflag.NewFlagSet("logout", mflag.ContinueOnError)
	f.Usage = logoutUsage
	if err := f.Parse(args); err != nil {
		return err
	}
	if len(args) >= 1 {
		fmt.Println(ErrMsgArgument)
		logoutUsage()
		return errors.New(ErrMsgArgument)
	}

	resp, err := commToDaemon("get", "/users/logout", nil)
	if err != nil {
		fmt.Println("Error :", err)
		return err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Println("DataHub : You already logout.")
	}
	if resp.StatusCode == http.StatusOK {
		fmt.Println("DataHub : Logout success.")
	}
	return nil
}

func loginUsage() {
	fmt.Printf("Usage: %s login\nSend a login request to the datahub server using your user name and password.\n", os.Args[0])
}

func logoutUsage() {
	fmt.Printf("Usage: %s logout\nSend a logout request to the datahub.\n", os.Args[0])
}
