package beecon

import (
	"encoding/json"
	"fmt"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	pool      *dbPool
	connector *dbConnector
)

func init() {
	pool = NewDbPool()
}

func Pool() *dbPool {
	return pool
}

func Connector() *dbConnector {
	return connector
}

func LoadConnector(dbMap mapSection) *dbConnector {
	connector = NewDbConnector(dbMap)
	return connector
}

func LoadConnectorJson(configStr string) *dbConnector {
	var mapDbSections mapSection
	mapDbSections = make(mapSection)

	var sectionMap map[string]interface{}
	json.Unmarshal([]byte(configStr), &sectionMap)
	for sectionName, sectionIf := range sectionMap {
		if b, err := json.Marshal(sectionIf); err == nil {
			sectionConfig := mapConfig{}
			if err := json.Unmarshal(b, &sectionConfig); err == nil {
				if section, err := NewDbSection(sectionName, sectionConfig); err == nil {
					mapDbSections[sectionName] = section
				}
			}
		}
	}
	connector = NewDbConnector(mapDbSections)
	return connector
}

func ReleaseConnections(sessionID string) {
	pool.releaseConn(sessionID)
}

// 각 repository 에서 db query 를 사용하기 위해 master, slave connnection 을 요청할때 사용하는 용도
// "packets.go:33: unexpected EOF" 에러는 DB 서버쪽 wait_timeout 이 너무 짧을때 reconnect 하면서 발생하는 로그
func GetDb(sectionName, sessionID string) orm.Ormer {
	if sectionName == "" {
		panic("sectionName is blank")
	}
	if sessionID == "" {
		panic("sessionID is blank")
	}

	if sectionName == "master" {
		sectionName = DefaultSection
	}

	// 해당 sessionID 로 transaction 중이라면 transaction에 사용된 connection 을 리턴
	if dc, err := pool.transactionConn(sessionID); err == nil {
		return dc.GetConn(true)
	}

	// 해당 sessionID 로 master connection 중이라면 master에 사용된 connection 을 리턴
	if dc, err := pool.masterConn(sessionID); err == nil {
		return dc.GetConn(false)
	}

	// transaction 중이 아니고, 현재 사용중인 connection 이 있다면 재활용한다.
	if dc, err := pool.reuseConn(sectionName, sessionID); err == nil {
		return dc.GetConn(false)
	}

	dc, err := pool.pickOne(sectionName, sessionID)
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}

	return dc.GetConn(false)
}

func MasterBegin(sessionID string) error {
	/**
	1. transaction 중이라면 transaction중인 connection 을 이용한다.
	2. transaction 중이 아니라면 master connection 이 있는지 확인해서 사용중인 master connection 이 있으면 사용한다. 없으면 새로 얻는다.
		2.1. 사용중인 master connection 이 있으면 그걸 이용한다.
		2.2. 없으면 새로 얻어서 사용한다.
	*/
	// 1. 해당 sessionID 로 transaction 중이라면 transaction에 사용된 connection 을 리턴
	if _, err := pool.transactionConn(sessionID); err == nil {
		return nil
	}

	// transaction 중이 아니고, 현재 사용중인 connection 이 있다면 재활용한다.
	if _, err := pool.masterConn(sessionID); err == nil {
		return nil
	}

	_, err := pool.makeMasterConn(sessionID)
	if err != nil {
		return err
	}

	return nil
}

func MasterEnd(sessionID string) error {
	pool.deleteMasterConn(sessionID)
	return nil
}

func TransactionBegin(sessionID string) error {
	dc, err := pool.makeTransactionConn(sessionID)
	if err != nil {
		return err
	}
	if err := (*dc.Ormer).Begin(); err != nil {
		return err
	}

	return nil
}

func Rollback(sessionID string) (err error) {
	dc, err := pool.transactionConn(sessionID)
	if err != nil {
		return fmt.Errorf("Rollback() dbContainer has not transaction status: %v", err)
	}

	pool.deleteTransactionConn(sessionID)
	return (*dc.Ormer).Rollback()
}

func Commit(sessionID string) (err error) {
	dc, err := pool.transactionConn(sessionID)
	if err != nil {
		return fmt.Errorf("Commit() dbContainer has not transaction status: %v", err)
	}

	pool.deleteTransactionConn(sessionID)
	return (*dc.Ormer).Commit()
}
