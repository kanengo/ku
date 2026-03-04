package distributedx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// SQL Table Schema:
/*
CREATE TABLE IF NOT EXISTS id_generator (
    biz_tag VARCHAR(128) PRIMARY KEY,
    max_id BIGINT NOT NULL DEFAULT 0,
    step INT NOT NULL DEFAULT 1000,
    description VARCHAR(256),
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
*/

const (
	defaultStep = 1000
)

// Segment represents a range of IDs
type Segment struct {
	Start   int64 // Inclusive start ID
	End     int64 // Inclusive end ID (max_id)
	Current int64 // Current ID being distributed
}

// SegmentBuffer manages double buffering of segments
type SegmentBuffer struct {
	BizTag int32
	Step   int32

	Current *Segment
	Next    *Segment

	isNextReady bool
	isLoading   bool          // flag to prevent multiple async loads
	loadCh      chan struct{} // channel to coordinate async loads
	mu          sync.RWMutex

	conn *pgx.Conn
	dbMu *sync.Mutex // Shared mutex for DB operations across all buffers using the same conn
}

// SegmentIDGen is the main struct for distributed ID generation
type SegmentIDGen struct {
	conn    *pgx.Conn
	dbMu    sync.Mutex
	buffers map[int32]*SegmentBuffer
	mu      sync.RWMutex
}

// NewSegmentIDGen creates a new ID generator instance
// dsn: Postgres connection string
func NewSegmentIDGen(dsn string) (*SegmentIDGen, error) {
	conn, err := GetConn(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	return &SegmentIDGen{
		conn:    conn,
		buffers: make(map[int32]*SegmentBuffer),
	}, nil
}

// Init initializes the ID generator for a specific business tag
// bizTag: Business identifier
// startId: The starting ID (if not exists)
// step: The step size for each segment allocation
func (g *SegmentIDGen) Init(ctx context.Context, bizTag int32, startId int64, step int32) error {
	if step <= 0 {
		step = defaultStep
	}

	g.mu.Lock()
	buffer, exists := g.buffers[bizTag]
	if !exists {
		buffer = &SegmentBuffer{
			BizTag: bizTag,
			Step:   step,
			conn:   g.conn,
			dbMu:   &g.dbMu,
		}
		g.buffers[bizTag] = buffer
	} else {
		buffer.Step = step
	}
	g.mu.Unlock()

	// If already initialized, we might just update step or ensure DB record exists?
	// For simplicity, we always run ensureTableAndRecord and loadCurrentSegment.
	// If buffer was already running, loadCurrentSegment is safe (locks internally).

	if err := buffer.ensureTableAndRecord(ctx, startId, step); err != nil {
		return err
	}

	// Pre-load the first segment synchronously
	return buffer.loadCurrentSegment(ctx)
}

// ensureTableAndRecord handles DB setup
func (b *SegmentBuffer) ensureTableAndRecord(ctx context.Context, startId int64, step int32) error {
	b.dbMu.Lock()
	defer b.dbMu.Unlock()

	createTableQuery := `
		CREATE TABLE IF NOT EXISTS id_generator (
			biz_tag INT PRIMARY KEY,
			max_id BIGINT NOT NULL DEFAULT 0,
			step INT NOT NULL DEFAULT 1000,
			description VARCHAR(256),
			update_time TIMESTAMP WITH TIME ZONE NOT NULL
		);
	`
	if _, err := b.conn.Exec(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	query := `
		INSERT INTO id_generator (biz_tag, max_id, step, update_time)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (biz_tag) DO UPDATE 
		SET step = $3, update_time = NOW()
	`
	if _, err := b.conn.Exec(ctx, query, b.BizTag, startId, step); err != nil {
		return fmt.Errorf("failed to initialize id generator: %w", err)
	}
	return nil
}

// GetID retrieves the next unique ID for the given bizTag
func (g *SegmentIDGen) GetID(ctx context.Context, bizTag int32) (int64, error) {
	g.mu.RLock()
	buffer, exists := g.buffers[bizTag]
	g.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("biz_tag %d not initialized, call Init first", bizTag)
	}

	return buffer.NextID(ctx)
}

// Close is a no-op as connection is managed globally
func (g *SegmentIDGen) Close() {
	// Do nothing
}

// loadCurrentSegment loads the current segment synchronously
func (b *SegmentBuffer) loadCurrentSegment(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If current is valid, return
	if b.Current != nil && b.Current.Current <= b.Current.End {
		return nil
	}

	// If next is ready, switch to next
	if b.isNextReady && b.Next != nil {
		b.Current = b.Next
		b.Next = nil
		b.isNextReady = false
		return nil
	}

	// Otherwise, fetch from DB
	seg, err := b.fetchSegment(ctx)
	if err != nil {
		return err
	}
	b.Current = seg
	return nil
}

// NextID gets the next ID with double buffering logic
func (b *SegmentBuffer) NextID(ctx context.Context) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. Check if current segment is initialized
	if b.Current == nil {
		// Try to load
		seg, err := b.fetchSegment(ctx)
		if err != nil {
			return 0, err
		}
		b.Current = seg
	}

	// 2. Check if current segment is exhausted
	if b.Current.Current > b.Current.End {
		// Try to switch to next segment
		if b.isNextReady && b.Next != nil {
			b.Current = b.Next
			b.Next = nil
			b.isNextReady = false
		} else if b.isLoading {
			// If async load is pending, wait for it to avoid reordering
			loadCh := b.loadCh
			b.mu.Unlock()

			select {
			case <-loadCh:
				// Wait finished
			case <-ctx.Done():
				b.mu.Lock()
				return 0, ctx.Err()
			}

			b.mu.Lock()
			// Re-check after waiting
			if b.isNextReady && b.Next != nil {
				b.Current = b.Next
				b.Next = nil
				b.isNextReady = false
			} else {
				// Async load failed, try sync fetch
				seg, err := b.fetchSegment(ctx)
				if err != nil {
					return 0, err
				}
				b.Current = seg
			}
		} else {
			// Next segment not ready and not loading, fetch synchronously
			seg, err := b.fetchSegment(ctx)
			if err != nil {
				return 0, err
			}
			b.Current = seg
		}
	}

	// 3. Get ID
	id := b.Current.Current
	b.Current.Current++

	// 4. Check for async pre-loading condition
	if !b.isNextReady && !b.isLoading {
		total := b.Current.End - b.Current.Start + 1
		used := id - b.Current.Start + 1

		// If used >= 20%
		if total > 0 && float64(used)/float64(total) >= 0.2 {
			b.isLoading = true
			b.loadCh = make(chan struct{})
			go b.asyncLoadNextSegment(b.loadCh)
		}
	}

	return id, nil
}

// fetchSegment fetches a new segment from the database
func (b *SegmentBuffer) fetchSegment(ctx context.Context) (*Segment, error) {
	b.dbMu.Lock()
	defer b.dbMu.Unlock()

	var maxId int64
	var step int

	// Update max_id and return new values
	// We increment max_id by step
	query := `
		UPDATE id_generator
		SET max_id = max_id + step, update_time = NOW()
		WHERE biz_tag = $1
		RETURNING max_id, step
	`

	// Use QueryRow directly instead of explicit transaction for single update
	err := b.conn.QueryRow(ctx, query, b.BizTag).Scan(&maxId, &step)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("biz_tag %d not found", b.BizTag)
		}
		return nil, fmt.Errorf("failed to update and fetch segment: %w", err)
	}

	// Calculate segment range
	// Previous max_id was (maxId - step)
	// New segment is from (maxId - step + 1) to maxId
	start := maxId - int64(step) + 1

	return &Segment{
		Start:   start,
		End:     maxId,
		Current: start,
	}, nil
}

// asyncLoadNextSegment loads the next segment in a background goroutine
func (b *SegmentBuffer) asyncLoadNextSegment(loadCh chan struct{}) {
	// Create a detached context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	defer func() {
		b.mu.Lock()
		b.isLoading = false
		close(loadCh)
		b.mu.Unlock()
	}()

	seg, err := b.fetchSegment(ctx)
	if err != nil {
		// Log error (should use a proper logger)
		// For now just print to stdout to avoid dependency on specific logger
		fmt.Printf("Error asynchronously loading next segment for biz_tag %d: %v\n", b.BizTag, err)
		return
	}

	b.mu.Lock()
	// Only update if we are still loading (safety check)
	if b.isLoading {
		b.Next = seg
		b.isNextReady = true
	}
	b.mu.Unlock()
}
