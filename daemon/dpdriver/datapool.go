package dpdriver

import (
	"fmt"
)

type DatapoolDriver interface {
	GetDestFileName(dpconn, itemlocation, filename string) (destfilename, tmpdir, tmpfile string)
}

type Datapool struct {
	driver DatapoolDriver
}

var datapooldrivers = make(map[string]DatapoolDriver)

func register(name string, datapooldriver DatapoolDriver) {
	if datapooldriver == nil {
		panic("dpdriver: Register datapooldriver is nil")
	}
	if _, dup := datapooldrivers[name]; dup {
		panic("dpdriver: Register called twice for datapooldriver " + name)
	}
	datapooldrivers[name] = datapooldriver
}

func New(name string) (*Datapool, error) {
	datapooldriver, ok := datapooldrivers[name]
	if !ok {
		return nil, fmt.Errorf("Can't find datapooldriver %s", name)
	}
	return &Datapool{driver: datapooldriver}, nil
}

func (datapool *Datapool) GetDestFileName(dpconn, itemlocation, filename string) (destfilename, tmpdir, tmpfile string) {
	return datapool.driver.GetDestFileName(dpconn, itemlocation, filename)
}

/*func (handler *Handler) DoUnbind(myServiceInfo *ServiceInfo, mycredentials *Credentials) error {
	return handler.driver.DoUnbind(myServiceInfo, mycredentials)
}
*/
