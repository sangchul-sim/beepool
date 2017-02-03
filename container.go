package beepool

import (
	"github.com/astaxie/beego/orm"
)

type dbContainer struct {
	AliasName  string
	Ormer      *orm.Ormer
	DriverName string
	UniqName   string
	Closed     bool
}

func newDatabaseContainer(alias string, ormer *orm.Ormer, driver string, uniq string) *dbContainer {
	return &dbContainer{
		AliasName:  alias,
		Ormer:      ormer,
		DriverName: driver,
		UniqName:   uniq,
		Closed:     false,
	}
}

func (dc *dbContainer) GetConn(transaction bool) orm.Ormer {
	if transaction == false {
		(*dc.Ormer).Using(dc.UniqName)
	}

	return *dc.Ormer
}

func (dc *dbContainer) Connections() (int, error) {
	// sql.DB
	db, err := orm.GetDB(dc.UniqName)
	if err != nil {
		return 0, err
	}
	stats := db.Stats()

	return stats.OpenConnections, nil
}

func (dc *dbContainer) Close() error {
	// sql.DB
	db, err := orm.GetDB(dc.UniqName)
	if err != nil {
		return err
	}
	err = db.Close()
	if err != nil {
		return err
	}

	dc.Closed = true
	return nil
}

func (dc *dbContainer) Ping() error {
	// sql.DB
	db, err := orm.GetDB(dc.UniqName)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}

	dc.Closed = false
	return nil
}
