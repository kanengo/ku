package distributedx

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
)

var PgConn = map[string]*pgx.Conn{}
var connMu sync.Mutex

func GetConn(dsn string) (*pgx.Conn, error) {
	connMu.Lock()
	defer connMu.Unlock()
	if conn, ok := PgConn[dsn]; ok {
		return conn, nil
	}
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dsn: %w", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	PgConn[dsn] = conn

	return conn, nil
}
