package beepool

const (
	DefaultSection      string = "default"
	MySQLDriver         string = "mysql"
	PostgreSQLDriver    string = "postgres"
	PostgreSQLDriverVal string = "pgsql"
)

// Connection 시 default 를 가장 처음 connection 해야 하기 때문에, 키값으로 ordering 한다음 해당 key 로 section을 다시 찾아야 한다.
type mapSection map[string]*dbSection

type dbConnector struct {
	sections mapSection
}

func NewDbConnector(sectionMap mapSection) *dbConnector {
	if _, ok := sectionMap[DefaultSection]; !ok {
		panic("It must have a default value.")
	}
	c := &dbConnector{
		sections: sectionMap,
	}
	return c
}

// driver : "mysql" or "postgres"
func (c *dbConnector) Connect() error {
	var orders []string
	orders = append(orders, DefaultSection)
	for sectionName := range c.sections {
		if sectionName == DefaultSection {
			continue
		}
		orders = append(orders, sectionName)
	}

	// default 부터 connnection을 맺어야 한다.
	// dbConfig 에 default 를 가장 빠른 순으로 명시해야 한다.
	var defaultConn bool = true
	for _, sectionName := range orders {
		if section, ok := c.sections[sectionName]; ok {
			initCount := section.Connection
			if initCount == 0 {
				initCount = 1
			}

			for i := 0; i < initCount; i++ {
				section.setConnection(defaultConn, true)
				if defaultConn {
					defaultConn = false
				}
			}
		}
	}
	return nil
}

func (c *dbConnector) addConnection(sectionName string, lock bool) (dc *dbContainer, err error) {
	if section, ok := c.sections[sectionName]; ok {
		dc, err = section.setConnection(false, lock)
		if err != nil {
			return nil, err
		}
	}
	return nil, err
}
