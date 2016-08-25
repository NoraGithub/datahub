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

var (
	WHHTTPSERVER      = "http://www.cjbigdata.com"
	WHSERVER          = "www.cjbigdata.com"
	HEBHTTPSERVER     = "http://www.hrbdataex.com"
	HEBSERVER         = "www.hrbdataex.com"
	GZHTTPSERVER      = "http://www.gzbdex.com"
	GZSERVER          = "www.gzbdex.com"
	DATAHTTPHUBSERVER = "https://hub.dataos.io"
	DATAHUBSERVER     = "hub.dataos.io"
	WHprefix          = "WH+"
	HEBprefix         = "HEB+"
	GZprefix          = "GZ+"
	datahubprefix     = "datahub+"
)

var ServerPrefix = make(map[string]string)

type UserForJson struct {
	Username string `json:"username", omitempty`
}

type Loginerr struct {
	Retrytimes string `json:"retry_times"`
	Ttltimes   string `json:"ttl_times"`
}

func Login(login bool, args []string) (err error) {

	f := mflag.NewFlagSet("datahub login", mflag.ContinueOnError)
	f.Usage = loginUsage
	if err := f.Parse(args); err != nil {
		//fmt.Println(err)
		return err
	}
	if len(args) == 0 {
		fmt.Println(ErrMsgArgument)
		loginUsage()
		return errors.New(ErrMsgArgument)
	}

	var prefix string
	if p, ok := ServerPrefix[args[0]]; ok {
		prefix = p
	} else {
		fmt.Println(ErrMsgArgument)
		loginUsage()
		return errors.New(ErrMsgArgument)
	}

	fmt.Printf("login as: ")
	reader := bufio.NewReader(os.Stdin)
	//loginName, _ := reader.ReadString('\n')
	loginName, _ := reader.ReadBytes('\n')

	loginName = append([]byte(prefix), bytes.TrimRight(loginName, "\r\n")...)

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
			fmt.Println(ErrLoginFailed)
			return errors.New(ErrLoginFailed)
		}
	} else {

		fmt.Println(ErrLoginFailed)
		if /*resp.StatusCode == 401 &&*/ login {
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println(string(body))
		}
		return errors.New(ErrLoginFailed)
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
		fmt.Println("DataHub : logout success.")
	}
	return nil
}

func loginUsage() {
	fmt.Printf("Usage:\n%s login URL\n\nSend a login request to the server using your user name and password.\n", os.Args[0])
}

func logoutUsage() {
	fmt.Printf("Usage:\n%s logout\n\nSend a logout request to the datahub.\n", os.Args[0])
}

func init() {
	ServerPrefix[WHHTTPSERVER] = WHprefix
	ServerPrefix[WHSERVER] = WHprefix
	ServerPrefix[HEBHTTPSERVER] = HEBprefix
	ServerPrefix[HEBSERVER] = HEBprefix
	ServerPrefix[GZHTTPSERVER] = GZprefix
	ServerPrefix[GZSERVER] = GZprefix
	ServerPrefix[DATAHTTPHUBSERVER] = datahubprefix
	ServerPrefix[DATAHUBSERVER] = datahubprefix
}
