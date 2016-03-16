package dpdriver

import (
	"fmt"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"os"
)

type fsdriver struct {
}

func (fs *fsdriver) GetDestFileName(dpconn, itemlocation, filename string) (destfilename, tmpdir, tmpfile string) {
	destfilename = dpconn + "/" + itemlocation + "/" + filename
	tmpdir = dpconn + "/" + itemlocation + "/tmp"
	tmpfile = tmpdir + "/" + filename
	return
}

func (fs *fsdriver) StoreFile(status, filename, dpconn, dp, itemlocation, destfile string) string {
	return status
}

func (fs *fsdriver) GetFileTobeSend(dpconn, dpname, itemlocation, tagdetail string) (filepathname string) {
	filepathname = dpconn + "/" + itemlocation + "/" + tagdetail
	return
}

func (fs *fsdriver) CheckItemLocation(datapoolname, dpconn, itemlocation string) error {
	log.Println(dpconn + "/" + itemlocation)
	err := os.MkdirAll(dpconn+"/"+itemlocation, 0777)
	if err != nil {
		log.Error(err)
	}
	return err
}

func (fs *fsdriver) CheckDataAndGetSize(dpconn, itemlocation, fileName string) (exist bool, size int64, err error) {
	destFullPathFileName := dpconn + "/" + itemlocation + "/" + fileName
	if isFileExists(destFullPathFileName) == false {
		exist = false
		err = fmt.Errorf("%s not exist.", destFullPathFileName)
		return
	}
	exist = true
	size, err = GetFileSize(destFullPathFileName)
	if err != nil {
		l := log.Errorf("Get %s size error, %v", destFullPathFileName, err)
		logq.LogPutqueue(l)
		return exist, 0, err
	}
	return
}

func init() {
	//fmt.Println("fs")
	register("file", &fsdriver{})
}

func GetFileSize(file string) (size int64, e error) {
	f, e := os.Stat(file)
	if e != nil {
		return 0, e
	}
	return f.Size(), nil
}
