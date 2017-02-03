package beecon

import (
	"errors"
	"fmt"
	"sync"
)

type mapDbContainer map[*dbContainer]*dbContainer

type dbPool struct {
	used        map[string]map[string]mapDbContainer
	unused      map[string]mapDbContainer
	transaction map[string]*dbContainer
	master      map[string]*dbContainer
	lock        sync.RWMutex
}

func NewDbPool() *dbPool {
	var pool *dbPool = new(dbPool)
	pool.used = make(map[string]map[string]mapDbContainer)
	pool.unused = make(map[string]mapDbContainer)
	pool.transaction = make(map[string]*dbContainer)
	pool.master = make(map[string]*dbContainer)

	return pool
}

// pickOne 는 사용중이지 않은 connection 을 하나 얻어서, ds.used[sessionID][sectionName]에 추가하고 ds.unused[sectionName] 에서는 제거한다.
func (ds *dbPool) pickOne(sectionName, sessionID string) (dc *dbContainer, err error) {
	ds.lock.Lock()

	var unUsedConns mapDbContainer
	defer func() {
		if err == nil {
			unUsedConns = ds.unused[sectionName]
			ds.relocation(sectionName, sessionID, unUsedConns, dc, false)
		}
		ds.lock.Unlock()
	}()

	unUsedConns, ok := ds.unused[sectionName]
	if !ok {
		return nil, fmt.Errorf("not available connection pool:%s", sectionName)
	}

	if len(unUsedConns) == 0 {
		// make another connection
		if dc, err := connector.addConnection(sectionName, false); err == nil {
			return dc, err
		}
		return nil, fmt.Errorf("no more connections to use:%s", sectionName)
	}

	for _, unUsedDc := range unUsedConns {
		dc = unUsedDc
		break
	}
	return
}

func (ds *dbPool) makeMasterConn(sessionID string) (dc *dbContainer, err error) {
	ds.lock.RLock()
	if _, ok := ds.master[sessionID]; ok {
		ds.lock.RUnlock()
		return nil, errors.New("master connection is already in progress")
	} else {
		ds.lock.RUnlock()
	}

	dc, err = ds.pickOne(DefaultSection, sessionID)
	if err == nil {
		ds.setMasterConn(sessionID, dc)
		return dc, nil
	}

	return nil, err
}

func (ds *dbPool) setMasterConn(sessionID string, dc *dbContainer) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	ds.master[sessionID] = dc
}

// makeTransactionConn 는 Transaction 을 시작할때 master connection 을 sessionID 로 발급한다.
// 만약, 해당 sessionID 로 기록된 transaction 이 있다면 error 리턴한다.
// 즉, sessionID 당 동시에 하나의 transaction 을 사용할 수 있고, transaction 이 종료되면 반드시 sessionID 로 기록된 transaction 을 지워야 한다.
func (ds *dbPool) makeTransactionConn(sessionID string) (dc *dbContainer, err error) {
	ds.lock.RLock()
	if _, ok := ds.transaction[sessionID]; ok {
		ds.lock.RUnlock()
		return nil, errors.New("transaction is already in progress")
	} else {
		ds.lock.RUnlock()
	}

	if dc, err := ds.masterConn(sessionID); err == nil {
		ds.setTransactionConn(sessionID, dc)
		return dc, nil
	}

	dc, err = ds.pickOne(DefaultSection, sessionID)
	if err == nil {
		ds.setTransactionConn(sessionID, dc)
		return dc, nil
	}

	return nil, err
}

func (ds *dbPool) setTransactionConn(sessionID string, dc *dbContainer) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	ds.transaction[sessionID] = dc
}

// transactionConn 는 sessionID 로 사용되고 있는 transaction connection 이 있으면 리턴해준다.
// 즉, 현재 transaction 이 진행중이라면 transaction 에 사용된 connection을 이용하려는 의도
func (ds *dbPool) transactionConn(sessionID string) (*dbContainer, error) {
	ds.lock.RLock()
	defer ds.lock.RUnlock()

	dc, ok := ds.transaction[sessionID]
	if !ok {
		return nil, fmt.Errorf("dbContainer has not transaction status")
	}
	return dc, nil
}

func (ds *dbPool) masterConn(sessionID string) (*dbContainer, error) {
	ds.lock.RLock()
	defer ds.lock.RUnlock()

	dc, ok := ds.master[sessionID]
	if !ok {
		return nil, fmt.Errorf("dbContainer has no master connection")
	}
	return dc, nil
}

// deleteTransactionConn 는 transaction 에 사용된 connection 기록을 삭제한다.
func (ds *dbPool) deleteTransactionConn(sessionID string) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	delete(ds.transaction, sessionID)
}

// deleteMasterConn 는 master connection 에 사용된 connection 기록을 삭제한다.
func (ds *dbPool) deleteMasterConn(sessionID string) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	delete(ds.master, sessionID)
}

// refactoring => OK
// reuseConn 는 sectionName 와 sessionID 로 사용되고 있는 connection 이 있다면 해당 connection 을 재사용하게 된다.
func (ds *dbPool) reuseConn(sectionName, sessionID string) (*dbContainer, error) {
	ds.lock.RLock()
	defer ds.lock.RUnlock()

	if _, ok := ds.used[sessionID]; ok {
		if _, ok := ds.used[sessionID][sectionName]; ok {
			usedConns := ds.used[sessionID][sectionName]
			for _, dc := range usedConns {
				return dc, nil
			}
		}
	}

	return nil, errors.New("no connnections")
}

// releaseConn 는 sessionID 로 발급된 모든 connection 을 반납하게 된다.
// transaction 이 아직 진행중이라면 반납하지 않는다.
func (ds *dbPool) releaseConn(sessionID string) error {
	if _, err := ds.transactionConn(sessionID); err == nil {
		return errors.New("transaction is still in progress")
	}
	ds.deleteMasterConn(sessionID)

	ds.lock.Lock()
	defer ds.lock.Unlock()

	if usedConns, ok := ds.used[sessionID]; ok {
		for sectionName, conns := range usedConns {
			for _, dc := range conns {
				if _, ok := ds.unused[sectionName]; !ok {
					ds.unused[sectionName] = make(mapDbContainer)
				}
				ds.unused[sectionName][dc] = dc
			}
		}
		delete(ds.used, sessionID)
	}
	return nil
}

// relocation 는 사용중인것과 사용중이지 않은 array를 재구성한다.
func (ds *dbPool) relocation(
	sectionName string,
	sessionID string,
	unused mapDbContainer,
	dc *dbContainer,
	lock bool,
) {
	if lock {
		ds.lock.Lock()
		defer ds.lock.Unlock()
	}

	if _, ok := ds.used[sessionID]; !ok {
		ds.used[sessionID] = make(map[string]mapDbContainer)
	}
	if _, ok := ds.used[sessionID][sectionName]; !ok {
		ds.used[sessionID][sectionName] = make(mapDbContainer)
	}
	if _, ok := ds.used[sessionID][sectionName][dc]; !ok {
		ds.used[sessionID][sectionName][dc] = dc
	}

	if _, ok := ds.unused[sectionName][dc]; ok {
		delete(ds.unused[sectionName], dc)
	}
}

// addUnused 는 사용중이지 않는 리스트에 추가한다.
func (ds *dbPool) addUnused(sectionName string, dc *dbContainer, lock bool) {
	if lock {
		ds.lock.Lock()
		defer ds.lock.Unlock()
	}

	if _, ok := ds.unused[sectionName]; !ok {
		ds.unused[sectionName] = make(mapDbContainer)
	}

	ds.unused[sectionName][dc] = dc
}

func (ds *dbPool) ToMap() map[string]interface{} {
	unused := map[string][]dbContainer{}
	used := map[string]map[string][]dbContainer{}

	for sectionName, dbContainers := range ds.unused {
		dbContainer := []dbContainer{}
		for _, dc := range dbContainers {
			dbContainer = append(dbContainer, *dc)
		}
		unused[sectionName] = dbContainer
	}

	for sessionID, dbContainersMap := range ds.used {
		sess := map[string][]dbContainer{}
		for sectionName, dbContainers := range dbContainersMap {
			dbContainer := []dbContainer{}
			for _, dc := range dbContainers {
				dbContainer = append(dbContainer, *dc)
			}
			sess[sectionName] = dbContainer
		}
		used[sessionID] = sess
	}

	data := map[string]interface{}{
		"used":   used,
		"unused": unused,
	}
	return data
}
