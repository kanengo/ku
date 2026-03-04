package distributedx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kanengo/ku/snowflakex"
)

const (
	defaultTableName        = "snowflake_workers"
	defaultHeartbeat        = 3 * time.Second
	defaultWorkerTimeout    = 6 * time.Second
	maxWorkerID             = snowflakex.MaxWorkerID
	workerAllocationRetries = 3
)

// Snowflake represents a distributed snowflake ID generator backed by PostgreSQL
type Snowflake struct {
	*snowflakex.Node
	conn      *pgx.Conn
	workerID  int64
	epoch     int64
	tableName string
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
}

// Config configuration for distributed snowflake
type Config struct {
	DSN       string
	TableName string
	Epoch     int64
}

// New creates a new distributed snowflake instance
func New(ctx context.Context, config Config) (*Snowflake, error) {
	if config.DSN == "" {
		return nil, errors.New("dsn is required")
	}
	if config.TableName == "" {
		config.TableName = defaultTableName
	}

	connConfig, err := pgx.ParseConfig(config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dsn: %w", err)
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := ensureTable(ctx, conn, config.TableName); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to ensure table: %w", err)
	}

	workerID, lastTimestamp, err := allocateWorkerID(ctx, conn, config.TableName)
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to allocate worker id: %w", err)
	}

	// Check for clock rollback if lastTimestamp is available
	currentTimestamp := time.Now().UnixMilli()
	if lastTimestamp > currentTimestamp {
		// Wait until clock catches up
		waitTime := time.Duration(lastTimestamp-currentTimestamp) * time.Millisecond
		if waitTime > time.Second*5 { // If wait time is too long, fail
			conn.Close(ctx)
			return nil, fmt.Errorf("clock moved backwards by %v, refusing to start", waitTime)
		}
		time.Sleep(waitTime + time.Millisecond)
	}

	node, err := snowflakex.NewNode(workerID, config.Epoch)
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to create snowflake node: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	sf := &Snowflake{
		Node:      node,
		conn:      conn,
		workerID:  workerID,
		epoch:     config.Epoch,
		tableName: config.TableName,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	go sf.heartbeat()

	return sf, nil
}

// Close releases resources
func (s *Snowflake) Close() {
	s.cancel()
	<-s.done
	// We need a context to close the connection, but Close() is usually deferred
	// and context might be cancelled. Use a background context with timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.conn.Close(ctx)
}

// GetWorkerID returns the current worker ID
func (s *Snowflake) GetWorkerID() int64 {
	return s.workerID
}

func ensureTable(ctx context.Context, conn *pgx.Conn, tableName string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			worker_id INTEGER PRIMARY KEY,
			last_timestamp BIGINT NOT NULL,
			ip_address TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL
		);
	`, tableName)
	_, err := conn.Exec(ctx, query)
	return err
}

func allocateWorkerID(ctx context.Context, conn *pgx.Conn, tableName string) (int64, int64, error) {
	ip, _ := getLocalIP()
	now := time.Now()

	// Try to find an expired worker ID or a new one
	// Strategy:
	// 1. Try to find an expired worker (updated_at < now - timeout)
	// 2. If none, try to find a free ID (not in table)

	// We use a transaction to ensure atomicity
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	// Clean up very old entries if needed, or just reuse
	// Find expired worker
	var workerID int64
	var lastTimestamp int64

	// Try to reclaim an expired worker ID
	queryReclaim := fmt.Sprintf(`
		SELECT worker_id, last_timestamp FROM %s 
		WHERE updated_at < $1 
		ORDER BY updated_at ASC 
		LIMIT 1 
		FOR UPDATE SKIP LOCKED
	`, tableName)

	err = tx.QueryRow(ctx, queryReclaim, now.Add(-defaultWorkerTimeout)).Scan(&workerID, &lastTimestamp)

	if err == nil {
		// Found expired worker, claim it
		updateQuery := fmt.Sprintf(`
			UPDATE %s 
			SET last_timestamp = $1, ip_address = $2, updated_at = $3 
			WHERE worker_id = $4
		`, tableName)
		_, err = tx.Exec(ctx, updateQuery, now.UnixMilli(), ip, now, workerID)
		if err != nil {
			return 0, 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, 0, err
		}
		return workerID, lastTimestamp, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, err
	}

	// No expired worker found, try to allocate a new one
	// Find the smallest unused worker ID
	// This is a bit tricky in SQL without a sequence table or checking all.
	// Since maxWorkerID is small (1023), we can try to find gaps or just append.

	// Simple approach: find max worker_id and increment
	var maxID *int64
	err = tx.QueryRow(ctx, fmt.Sprintf("SELECT MAX(worker_id) FROM %s", tableName)).Scan(&maxID)
	if err != nil {
		return 0, 0, err
	}

	nextID := int64(0)
	if maxID != nil {
		nextID = *maxID + 1
	}

	if nextID > maxWorkerID {
		// All IDs taken and none expired.
		// Fallback: try to find a gap (optional, but good for robustness)
		// For now, return error
		return 0, 0, errors.New("no available worker IDs")
	}

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (worker_id, last_timestamp, ip_address, updated_at)
		VALUES ($1, $2, $3, $4)
	`, tableName)

	_, err = tx.Exec(ctx, insertQuery, nextID, now.UnixMilli(), ip, now)
	if err != nil {
		return 0, 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}

	return nextID, 0, nil
}

func (s *Snowflake) heartbeat() {
	defer close(s.done)
	ticker := time.NewTicker(defaultHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.updateHeartbeat()
		}
	}
}

func (s *Snowflake) updateHeartbeat() {
	// We update last_timestamp to the current max time we might have generated
	// Actually, we should update it to the current time, because we are alive.
	// But more importantly, we should record the MAX time we could have generated IDs for?
	// The requirement says "store latest used timestamp".
	// Since we generate IDs in memory, we don't persist every ID.
	// We can just persist the current time, implying "we are alive at T, so any ID < T is valid".
	// But if we crash and restart, we need to know what was the last time we *might* have generated.
	// The snowflakex node keeps track of time.
	// It's safer to persist time.Now() periodically.

	now := time.Now()
	query := fmt.Sprintf(`
		UPDATE %s 
		SET last_timestamp = $1, updated_at = $2 
		WHERE worker_id = $3
	`, s.tableName)

	_, err := s.conn.Exec(s.ctx, query, now.UnixMilli(), now, s.workerID)
	if err != nil {
		// Log error?
		// If heartbeat fails repeatedly, we might want to stop generating IDs?
		// For now, just ignore transient errors.
		fmt.Fprintf(os.Stderr, "snowflake heartbeat error: %v\n", err)
	}
}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "unknown", nil
}
