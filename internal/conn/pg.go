package conn

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var PgConn = map[string]*pgxpool.Pool{}
var connMu sync.Mutex

func GetPgConn(dsn string) (*pgxpool.Pool, error) {
	connMu.Lock()
	defer connMu.Unlock()
	if conn, ok := PgConn[dsn]; ok {
		return conn, nil
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dsn: %w", err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	PgConn[dsn] = conn

	return conn, nil
}
