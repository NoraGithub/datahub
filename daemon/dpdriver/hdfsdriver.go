package dpdriver

import (
	"errors"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	dfs "github.com/colinmarc/hdfs"
	"github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	//"github.com/asiainfoLDP/datahub/utils/logq"
	"net/url"
	"os"
)

type Client struct {
	namenode *rpc.NamenodeConnection
	defaults *hadoop_hdfs.FsServerDefaultsProto
}

type hdfsdriver struct {
}

func (hdfs *hdfsdriver) GetDestFileName(dpconn, itemlocation, filename string) (destfilename, tmpdir, tmpfile string) {

	destfilename = "/var/lib/datahub/" + itemlocation + "/" + filename
	tmpdir = "/var/lib/datahub/" + itemlocation + "/tmp"
	tmpfile = tmpdir + "/" + filename
	return
}

func (hdfs *hdfsdriver) StoreFile(status, filename, dpconn, dp, itemlocation, destfile string) string {

	log.Infof("Begin to upload %v to %v\n", filename, dp)

	client, err := getClient(dpconn)
	if err != nil {
		log.Error("Failed to get a client", err)
		status = "put to hdfs err"
		return status
	}
	defer client.Close()

	err = client.MkdirAll("/"+itemlocation, 1777)
	if err != nil {
		log.Error("Failed to mkdirall in hdfs", err)
		status = "put to hdfs err"
		return status
	}

	hdfsfile := "/" + itemlocation + "/" + destfile
	err = client.CopyToRemote(filename, hdfsfile)
	if err != nil {
		log.Error("Failed to CopyToRemote", err)
		status = "put to hdfs err"
		return status
	}

	status = "put to hdfs ok"
	log.Info("Successfully uploaded to", itemlocation, "in hdfs")
	return status
}

func (hdfs *hdfsdriver) GetFileTobeSend(dpconn, dpname, itemlocation, tagdetail string) (filepathname string) {

	e := os.MkdirAll("/var/lib/datahub/"+itemlocation, 1777)
	if e != nil {
		log.Error(e)
		return
	}

	filepathname = "/var/lib/datahub/" + itemlocation + "/" + tagdetail
	log.Println("filepathname:", filepathname)

	if true == isFileExists(filepathname) {
		return
	}

	client, err := getClient(dpconn)
	if err != nil {
		log.Error("Failed to get a client", err)
		return
	}
	defer client.Close()

	hdfsfile := "/" + itemlocation + "/" + tagdetail
	err = client.CopyToLocal(hdfsfile, filepathname)
	if err != nil {
		log.Println("Failed to download file.", err)
		return
	}

	cs, err := client.GetContentSummary(hdfsfile)
	if err != nil {
		log.Error("Failed to get contentsummary.", err)
		return
	}

	log.Println("Downloaded file", tagdetail, cs.Size(), "bytes")

	return
}

func (hdfs *hdfsdriver) CheckItemLocation(datapoolname, dpconn, itemlocation string) (err error) {

	client, err := getClient(dpconn)
	if err != nil {
		log.Error("Failed to get a client", err)
		return
	}
	defer client.Close()

	err = client.MkdirAll("/"+itemlocation, 1777)
	if err != nil {
		log.Error(err)
	}

	return
}

func (hdfs *hdfsdriver) CheckDataAndGetSize(dpconn, itemlocation, fileName string) (exist bool, size int64, err error) {

	destFullPathFileName := "/" + itemlocation + "/" + fileName
	log.Info(destFullPathFileName)

	exist = false

	client, err := getClient(dpconn)
	if err != nil {
		log.Error("Failed to get a client", err)
		return
	}
	defer client.Close()

	fileinfo, _ := client.Stat(destFullPathFileName)

	if fileinfo != nil {
		exist = true
		cs, _ := client.GetContentSummary(destFullPathFileName)
		size = cs.Size()
	} else {
		err = errors.New("文件不存在")
		return
	}

	return
}

func getClient(dpconn string) (client *dfs.Client, err error) {
	log.Info("dpconn:", dpconn)
	u, err := url.Parse("hdfs://" + dpconn)
	if err != nil {
		return
	}
	userinfo := u.User
	username := userinfo.Username()
	//password :=userinfo.Password()
	host := u.Host
	client, err = dfs.NewForUser(host, username)
	if err != nil {
		return
	}

	return
}

func init() {
	register("hdfs", &hdfsdriver{})
}
