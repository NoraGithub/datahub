package daemon

import (
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/go-fsnotify/fsnotify"
)

//map[file]tag
var (
	monitList = make(map[string]string)
	watcher   *fsnotify.Watcher
)

func datapoolMonitor() {
	var err error
	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		l := log.Error(err)
		logq.LogPutqueue(l)
	}
	defer watcher.Close()

	initMonitList()

	done := make(chan bool)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				log.Debug("event:", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					l := log.Warn("modified file:", event.Name, monitList[event.Name])
					logq.LogPutqueue(l)
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					l := log.Warn("deleted file:", event.Name, monitList[event.Name])
					logq.LogPutqueue(l)
					watcher.Remove(event.Name)
					err = watcher.Add(event.Name)
					if err != nil {
						l := log.Errorf("checking %v error: %v", event.Name, err)
						logq.LogPutqueue(l)
					}
				}
				if event.Op&fsnotify.Rename == fsnotify.Rename {
					l := log.Warn("renamed file:", event.Name, monitList[event.Name])
					logq.LogPutqueue(l)
				}
			case err := <-watcher.Errors:
				l := log.Error("error:", err)
				logq.LogPutqueue(l)
			}
		}
	}()

	for filecheck, tag := range monitList {
		l := log.Debug("monitoring", filecheck, tag)
		logq.LogPutqueue(l)
		err = watcher.Add(filecheck)
		if err != nil {
			l := log.Errorf("checking %v %v error: %v", filecheck, tag, err)
			logq.LogPutqueue(l)
		}
	}

	<-done
}

func AddtoMonitor(filecheck, tag string) {
	if watcher != nil {
		err := watcher.Add(filecheck)

		l := log.Info("monitoring", filecheck, tag)
		logq.LogPutqueue(l)
		if err != nil {
			l := log.Errorf("checking %v error: %v", filecheck, err)
			logq.LogPutqueue(l)
		}
		monitList[filecheck] = tag
	}

	//log.Info(monitList)
}

func initMonitList() {
	//monitList = make(map[string]string)
	if e := GetAllTagDetails(&monitList); e != nil {
		l := log.Errorf("GetAllTagDetails error. %v", e)
		logq.LogPutqueue(l)
	}
	//log.Debug(monitList)
}
