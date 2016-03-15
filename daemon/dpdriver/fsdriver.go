package dpdriver

import (
	//"fmt"
	log "github.com/asiainfoLDP/datahub/utils/clog"
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

func init() {
	//fmt.Println("fs")
	register("file", &fsdriver{})
}
