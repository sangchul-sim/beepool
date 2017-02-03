package beepool

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/orm"
)

const (
	EnvDevelopment = "dev"
	EnvStaging     = "stg"
	EnvProduction  = "prod"
)

var (
	driverMap    map[string]orm.DriverType
	regDriverMap map[string]string
)

type mapConfig map[string]string

type dbSection struct {
	SectionName string
	Address     string
	Charset     string
	DbName      string `json:"db_name"`
	Driver      string
	Id          string
	Loc         string
	Port        string
	Pw          string
	Sslmode     string
	Connection  int `json:",string"`
}

func init() {
	driverMap = make(map[string]orm.DriverType)
	driverMap = map[string]orm.DriverType{
		MySQLDriver:      orm.DRMySQL,
		PostgreSQLDriver: orm.DRPostgres,
	}
	regDriverMap = make(map[string]string)
	regDriverMap = map[string]string{
		MySQLDriver:      MySQLDriver,
		PostgreSQLDriver: PostgreSQLDriverVal,
	}
}

func NewDbSection(sectionName string, config mapConfig) (*dbSection, error) {
	s := new(dbSection)
	s.SectionName = sectionName

	b, err := json.Marshal(config)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, s); err != nil {
		return s, err
	}

	return s, nil
}

func (s *dbSection) getDsn() (string, error) {
	return s.getHostDsn()
}

func (s *dbSection) getHostDsn() (dsn string, err error) {
	switch s.Driver {
	case PostgreSQLDriver:
		dsn = s.getPostreSQLHostDsn()
	case MySQLDriver:
		dsn = s.getMySQLHostDsn()
	default:
		err = fmt.Errorf("unsported driver: %s", s.Driver)
	}
	return
}

func (s *dbSection) getPostreSQLHostDsn() string {
	return fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s?charset=%s&loc=%s&sslmode=%s",
		s.Driver,
		s.Id,
		s.Pw,
		s.Address,
		s.Port,
		s.DbName,
		s.Charset,
		s.Loc,
		s.Sslmode,
	)
}

func (s *dbSection) getMySQLHostDsn() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=%s&loc=%s",
		s.Id,
		s.Pw,
		s.Address,
		s.Port,
		s.DbName,
		s.Charset,
		s.Loc,
	)
}

func (s *dbSection) Clone() *dbSection {
	sec := new(dbSection)
	*sec = *s
	return sec
}

func (s *dbSection) setConnection(defaultConn bool, lock bool) (*dbContainer, error) {
	err := orm.RegisterDriver(regDriverMap[s.Driver], driverMap[s.Driver])
	if err != nil {
		return nil, err
	}

	dsn, err := s.getDsn()
	if err != nil {
		return nil, err
	}
	regAlias := randStringBytesMaskImpr(32, s.SectionName, "::")
	// 최초 database connection 시 default 가 반드시 있어야 한다.
	if defaultConn && s.SectionName == DefaultSection {
		regAlias = s.SectionName
	}

	err = orm.RegisterDataBase(regAlias, s.Driver, dsn)
	//orm.SetMaxIdleConns(regAlias, 1)
	if err != nil {
		return nil, err
	}

	if beego.BConfig.RunMode == EnvDevelopment {
		orm.Debug = true
	}

	newDb := orm.NewOrm()
	dbconn := newDatabaseContainer(s.SectionName, &newDb, s.Driver, regAlias)
	pool.addUnused(s.SectionName, dbconn, lock)

	return dbconn, nil
}

func randStringBytesMaskImpr(n int, prefix ...string) string {
	rand.Seed(time.Now().UnixNano())
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	prefixStr := ""
	for _, pre := range prefix {
		prefixStr += pre
	}

	return prefixStr + string(b)
}
