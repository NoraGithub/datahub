package daemon

import (
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"github.com/asiainfoLDP/datahub/cmd"
	"github.com/asiainfoLDP/datahub/daemon/daemonigo"
	"github.com/asiainfoLDP/datahub/ds"
	"github.com/asiainfoLDP/datahub/utils"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	g_ds = new(ds.Ds)

	wg sync.WaitGroup

	staticFileDir string

	DaemonAuthrization string

	DaemonCliServer string = "127.0.0.1:35600"

	CLIEntrypoint string = "127.0.0.1:35600"
)

const (
	g_dbfile    string = "/var/lib/datahub/datahub.db"
	g_strDpPath string = cmd.GstrDpPath
	DPFILE      string = "file"
	DPS3        string = "s3"
)

type StoppableListener struct {
	*net.UnixListener          //Wrapped listener
	stop              chan int //Channel used only to indicate listener should shutdown
}

type StoppabletcpListener struct {
	*net.TCPListener          //Wrapped listener
	stop             chan int //Channel used only to indicate listener should shutdown
}

func dbinit() {

	DB_TYPE := os.Getenv("DB_TYPE")
	if strings.ToUpper(DB_TYPE) == "MYSQL" {
		for i := 0; i < 3; i++ {
			connectMysql()
			if g_ds.Db == nil {
				select {
				case <-time.After(time.Second * 5):
					continue
				}
			} else {
				break
			}
		}
		if g_ds.Db == nil {
			return
		}
	} else {
		log.Println("connect to db sqlite3")
		db, err := sql.Open("sqlite3", g_dbfile)
		//defer db.Close()
		chk(err)
		g_ds.Db = db
		g_ds.DbType = "sqlite"
	}

	var RetDhRpdmTagMap string
	row, err := g_ds.QueryRow(ds.SQLIsExistRpdmTagMap)
	if err != nil {
		l := log.Error("Get Dh_Rpdm_Tag_Map error!")
		logq.LogPutqueue(l)
		return
	}
	row.Scan(&RetDhRpdmTagMap)
	if len(RetDhRpdmTagMap) > 1 {
		if false == strings.Contains(RetDhRpdmTagMap, "COMMENT") {
			//	UpdateSql04To05()
			UpdateSql16To17()
		}
	}
	//if err := UpgradeSql07To08(); err != nil {
	//	panic(err)
	//}
	if err := CreateTable(); err != nil {
		l := log.Error("Get CreateTable error!", err)
		logq.LogPutqueue(l)
		panic(err)
	}
}

func connectMysql() {
	DB_ADDR := os.Getenv("MYSQL_PORT_3306_TCP_ADDR")
	DB_PORT := os.Getenv("MYSQL_PORT_3306_TCP_PORT")
	DB_DATABASE := os.Getenv("MYSQL_ENV_MYSQL_DATABASE")
	DB_USER := os.Getenv("MYSQL_ENV_MYSQL_USER")
	DB_PASSWORD := os.Getenv("MYSQL_ENV_MYSQL_PASSWORD")
	DB_URL := fmt.Sprintf(`%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true`, DB_USER, DB_PASSWORD, DB_ADDR, DB_PORT, DB_DATABASE)
	db, err := sql.Open("mysql", DB_URL)
	if err != nil {
		log.Errorf("error: %s\n", err)
	} else {
		g_ds.Db = db
		g_ds.DbType = "mysql"
		log.Println("Connect to Mysql successfully!")
	}
}

func chk(err error) {
	if err != nil {
		panic(err)
	}
}
func get(err error) {
	if err != nil {
		log.Println(err)
	}
}

func New(l net.Listener) (*StoppableListener, error) {
	unixL, ok := l.(*net.UnixListener)

	if !ok {
		return nil, errors.New("Cannot wrap listener")
	}

	retval := &StoppableListener{}
	retval.UnixListener = unixL
	retval.stop = make(chan int)

	return retval, nil
}
func tcpNew(l net.Listener) (*StoppabletcpListener, error) {
	tcpL, ok := l.(*net.TCPListener)

	if !ok {
		return nil, errors.New("Cannot wrap listener")
	}

	retval := &StoppabletcpListener{}
	retval.TCPListener = tcpL
	retval.stop = make(chan int)

	return retval, nil
}

var StoppedError = errors.New("Listener stopped")
var sl = new(StoppabletcpListener)
var p2psl = new(StoppabletcpListener)

func (sl *StoppableListener) Accept() (net.Conn, error) {

	for {
		//Wait up to one second for a new connection
		sl.SetDeadline(time.Now().Add(time.Second))

		newConn, err := sl.UnixListener.Accept()

		//Check for the channel being closed
		select {
		case <-sl.stop:
			return nil, StoppedError
		default:
			//If the channel is still open, continue as normal
		}

		if err != nil {
			netErr, ok := err.(net.Error)

			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
		}

		return newConn, err
	}
}

func (sl *StoppableListener) Stop() {

	close(sl.stop)
}

func (tcpsl *StoppabletcpListener) Accept() (net.Conn, error) {

	for {
		//Wait up to one second for a new connection
		tcpsl.SetDeadline(time.Now().Add(time.Second))

		newConn, err := tcpsl.TCPListener.Accept()

		//Check for the channel being closed
		select {
		case <-tcpsl.stop:
			return nil, StoppedError
		default:
			//If the channel is still open, continue as normal
		}

		if err != nil {
			netErr, ok := err.(net.Error)

			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
		}

		return newConn, err
	}
}

func (tcpsl *StoppabletcpListener) Stop() {

	close(tcpsl.stop)
}

func helloHttp(rw http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	rw.WriteHeader(http.StatusOK)
	body, _ := ioutil.ReadAll(req.Body)
	fmt.Fprintf(rw, "%s Hello HTTP!\n", req.URL.Path)
	fmt.Fprintf(rw, "%s \n", string(body))
}

func stopHttp(rw http.ResponseWriter, req *http.Request) {
	rw.WriteHeader(http.StatusOK)
	//fmt.Fprintf(rw, "Hello HTTP!\n")
	sl.Close()
	p2psl.Close()
	log.Println("connect close")
}

func isDirExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		log.Println(err.Error())
		return os.IsExist(err)
	} else {
		//log.Println(fi.IsDir())
		return fi.IsDir()
	}
	//panic("not reached")
	return false
}
func isFileExists(file string) bool {
	fi, err := os.Stat(file)
	if err == nil {
		log.Println("exist", file)
		return !fi.IsDir()
	}
	return os.IsExist(err)
}

func RunDaemon() {
	//fmt.Println("Run daemon..")
	// Daemonizing echo server application.

	switch isDaemon, err := daemonigo.Daemonize(); {
	case !isDaemon:
		return
	case err != nil:
		log.Fatal("main(): could not start daemon, reason -> %s", err.Error())
	}
	//fmt.Printf("server := http.Server{}\n")

	if false == isDirExists(g_strDpPath) {
		err := os.MkdirAll(g_strDpPath, 0755)
		if err != nil {
			log.Printf("mkdir %s error! %v ", g_strDpPath, err)
		}

	}

	DaemonAuthrization = utils.Getguid()
	log.Println("DaemonAuthrization", DaemonAuthrization)

	dbinit()

	if len(DaemonID) == 40 {
		log.Println("daemonid", DaemonID)
		saveDaemonID(DaemonID)
	} else {
		log.Println("get daemonid from db")
		DaemonID = getDaemonid()
	}

	LoadJobFromDB()

	os.Chdir(g_strDpPath)
	originalListener, err := net.Listen("tcp", DaemonCliServer)
	if err != nil {
		log.Fatal(err)
	} //else {
	// 	if err = os.Chmod(cmd.UnixSock, os.ModePerm); err != nil {
	// 		l := log.Error(err)
	// 		logq.LogPutqueue(l)
	// 	}
	// }

	sl, err = tcpNew(originalListener)
	if err != nil {
		panic(err)
	}

	router := httprouter.New()
	router.GET("/", serverFileHandler)
	router.POST("/api/datapools", dpPostOneHandler)
	router.GET("/api/datapools", dpGetAllHandler)
	router.GET("/api/datapools/:dpname", dpGetOneHandler)
	router.DELETE("/api/datapools/:dpname", dpDeleteOneHandler)

	router.GET("/api/ep", epGetHandler)
	router.POST("/api/ep", epPostHandler)
	router.DELETE("/api/ep", epDeleteHandler)

	router.GET("/api/repositories/:repo/:item/:tag", repoTagHandler)
	router.GET("/api/repositories/:repo/:item", repoItemHandler)
	router.GET("/api/repositories/:repo", repoRepoNameHandler)
	router.GET("/api/repositories", repoHandler)
	router.GET("/api/repositories/:repo/:item/:tag/judge", judgeTagExistHandler)
	router.DELETE("/api/repositories/:repo/:item", repoDelOneItemHandler)
	router.DELETE("/api/repositories/:repo/:item/:tag", repoDelTagHandler)

	router.GET("/api/subscriptions/dataitems", subsHandler)
	router.GET("/api/subscriptions/pull/:repo/:item", subsHandler)

	router.POST("/api/repositories/:repo/:item", pubItemHandler)
	router.POST("/api/repositories/:repo/:item/:tag", pubTagHandler)

	router.POST("/api/subscriptions/:repo/:item/pull", pullHandler)

	router.GET("/api/job", jobHandler)
	router.GET("/api/job/:id", jobDetailHandler)
	router.DELETE("/api/job/:id", jobRmHandler)
	router.DELETE("/api/job", jobRmAllHandler)

	router.GET("/api/daemon/:repo/:item/:tag", tagStatusHandler)
	router.GET("/api/daemon/:repo/:item", tagOfItemStatusHandler)

	router.GET("/api/heartbeat/status/:user", userStatusHandler)

	http.Handle("/", router)
	http.HandleFunc("/api/stop", stopHttp)
	http.HandleFunc("/api/users/auth", loginHandler)
	http.HandleFunc("/api/users/logout", logoutHandler)

	router.GET("/api/users/whoami", whoamiHandler)
	router.GET("/api/pulled/:repo/:item", itemPulledHandler)

	router.GET("/api/datapool/published/:dpname", publishedOfDatapoolHandler)
	router.GET("/api/datapool/pulled/:dpname", pulledOfDatapoolHandler)
	router.GET("/api/datapool/published/:dpname/:repo", publishedOfRepoHandler)
	router.GET("/api/datapool/pulled/:dpname/:repo", pulledOfRepoHandler)
	router.POST("/api/datapool/check", checkDpConnectHandler)
	router.GET("/api/datapool/other/:dpname", dpGetOtherDataHandler)

	router.NotFound = &mux{}

	server := http.Server{}

	go func() {

		stop := make(chan os.Signal)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		select {
		case signal := <-stop:
			log.Printf("Got signal:%v", signal)
		}

		sl.Stop()
		if len(DaemonID) > 0 {
			p2psl.Stop()
		}

	}()

	if len(DaemonID) > 0 {
		go startP2PServer()
		go HeartBeat()
		go CheckHealthClock()
		//go datapoolMonitor()  //Temporarily not use
		go GetMessages()
		go PullTagAutomatic()
	} else {
		l := log.Error("no daemonid specificed.")
		logq.LogPutqueue(l)
		fmt.Println("You don't have a daemonid specificed.")
	}

	/*
		wg.Add(1)
		defer wg.Done()
	*/
	log.Info("starting daemon listener...")
	server.Serve(sl)
	log.Info("Stopping daemon listener...")

	if len(DaemonID) > 0 {
		wg.Wait()
	}

	daemonigo.UnlockPidFile()
	g_ds.Db.Close()

	log.Info("daemon exit....")
	log.CloseLogFile()

}

type mux struct {
}

func (m *mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	staticFileDir = os.Getenv("STATIC_FILE_DIR")
	if len(staticFileDir) == 0 {
		http.ServeFile(w, r, "/var/lib/datahub/dist/"+r.URL.Path[1:])
	} else {
		http.ServeFile(w, r, strings.TrimRight(staticFileDir, "/")+"/"+r.URL.Path[1:])
	}
}

func serverFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	//http.ServeFile(w, r, "/home/yy/dev/src/github.com/asiainfoLDP/datahub/dist/index.html")
	staticFileDir = os.Getenv("STATIC_FILE_DIR")
	if len(staticFileDir) == 0 {
		http.ServeFile(w, r, "/var/lib/datahub/dist/index.html")
	} else {
		http.ServeFile(w, r, strings.TrimRight(staticFileDir, "/")+"/index.html")
	}
}

func init() {
	if srv := os.Getenv("DATAHUB_SERVER"); len(srv) > 0 {
		DefaultServer = srv
		DefaultServerAPI = DefaultServer + "/api"
	}

	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	log.SetLogLevel(log.LOG_LEVEL_INFO)

	if daemonServer := os.Getenv("DATAHUB_DAEMON_SERVER"); len(daemonServer) > 0 {
		DaemonCliServer = daemonServer
	}

	if cliEp := os.Getenv("DATAHUB_CLI_ENTRYPOINT"); len(cliEp) > 0 {
		CLIEntrypoint = cliEp
	}

}
