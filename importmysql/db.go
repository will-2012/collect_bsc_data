package importmysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"bsc_stats/common"

	"github.com/go-sql-driver/mysql"
)

// schema is created on startup with IF NOT EXISTS, so a fresh DB is usable
// without a separate migration step.
//
// Design notes (see also README):
//   - addresses BINARY(20), hashes BINARY(32): ~2x smaller than hex strings, so
//     the hot tx indexes fit more of the buffer pool.
//   - block_time is unix seconds (BIGINT), denormalized onto tx so the
//     from/to + time-range query is a single covering index range scan, no join.
//   - tx covering indexes (addr, block_time, block_number, gas_used) answer
//     COUNT(DISTINCT block_number) and SUM(gas_used) index-only; the block index
//     (block_time, gas_used, gas_limit) answers the range denominators.
//   - idx_block_number on tx keeps tx<->block reverse lookups index-driven.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS block (
		number      BIGINT UNSIGNED NOT NULL,
		hash        BINARY(32)      NOT NULL,
		block_time  BIGINT UNSIGNED NOT NULL,
		gas_used    BIGINT UNSIGNED NOT NULL,
		gas_limit   BIGINT UNSIGNED NOT NULL,
		tx_count    INT    UNSIGNED NOT NULL,
		PRIMARY KEY (number),
		KEY idx_block_time_gas (block_time, gas_used, gas_limit),
		-- gas_limit leading, so "which gas-limit eras exist and their time spans"
		-- (GROUP BY gas_limit, MIN/MAX(block_time)) is a loose index scan over the
		-- handful of distinct values, not a full-year table scan.
		KEY idx_gas_limit (gas_limit, block_time)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS tx (
		hash          BINARY(32)      NOT NULL,
		block_number  BIGINT UNSIGNED NOT NULL,
		block_hash    BINARY(32)      NOT NULL,
		block_time    BIGINT UNSIGNED NOT NULL,
		gas_used      BIGINT UNSIGNED NOT NULL,
		from_addr     BINARY(20)      NOT NULL,
		to_addr       BINARY(20)      NULL,
		PRIMARY KEY (hash),
		KEY idx_from_time (from_addr, block_time, block_number, gas_used),
		KEY idx_to_time   (to_addr,   block_time, block_number, gas_used),
		KEY idx_block_number (block_number)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS import_progress (
		chunk_start BIGINT UNSIGNED NOT NULL,
		chunk_end   BIGINT UNSIGNED NOT NULL,
		done_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (chunk_start)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS import_failed (
		block_number BIGINT UNSIGNED NOT NULL,
		reason       VARCHAR(500)    NOT NULL,
		failed_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (block_number)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
}

// txBatchSize bounds how many tx rows go in one multi-row INSERT (keeps the
// statement well under max_allowed_packet / placeholder limits).
const txBatchSize = 1000

// DB wraps the MySQL sink with retry parameters.
type DB struct {
	sql      *sql.DB
	maxRetry int
	baseWait time.Duration
}

// OpenDB opens the pool, caps concurrent connections, and verifies connectivity.
func OpenDB(ctx context.Context, dsn string, maxConns int) (*DB, error) {
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(maxConns)
	sqlDB.SetMaxIdleConns(maxConns)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &DB{sql: sqlDB, maxRetry: 6, baseWait: 200 * time.Millisecond}, nil
}

func (db *DB) Close() error { return db.sql.Close() }

// CreateSchema creates the tables if they do not exist.
func (db *DB) CreateSchema(ctx context.Context) error {
	for _, stmt := range schema {
		if _, err := db.sql.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	return nil
}

// WriteBlock persists one block and its transactions in a single transaction.
// The whole BEGIN..COMMIT is retried on transient errors, so a deadlock retry
// restarts a fresh transaction rather than reusing an aborted one.
func (db *DB) WriteBlock(ctx context.Context, bd *BlockData) error {
	return db.withRetry(ctx, func() error {
		tx, err := db.sql.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := upsertBlock(ctx, tx, bd); err != nil {
			tx.Rollback()
			return err
		}
		if err := upsertTxs(ctx, tx, bd.Txs); err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

func upsertBlock(ctx context.Context, tx *sql.Tx, bd *BlockData) error {
	// ON DUPLICATE KEY UPDATE with a no-op self-assign makes re-import idempotent
	// (suppresses only duplicate-key; truncation/bad-data still error out, unlike
	// INSERT IGNORE).
	_, err := tx.ExecContext(ctx,
		`INSERT INTO block (number, hash, block_time, gas_used, gas_limit, tx_count)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE hash=hash`,
		bd.Number, bd.Hash, bd.Time, bd.GasUsed, bd.GasLimit, bd.TxCount)
	return err
}

func upsertTxs(ctx context.Context, tx *sql.Tx, txs []TxData) error {
	for start := 0; start < len(txs); start += txBatchSize {
		end := start + txBatchSize
		if end > len(txs) {
			end = len(txs)
		}
		batch := txs[start:end]

		var b strings.Builder
		b.WriteString(`INSERT INTO tx (hash, block_number, block_hash, block_time, gas_used, from_addr, to_addr) VALUES `)
		args := make([]interface{}, 0, len(batch)*7)
		for i, t := range batch {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString("(?, ?, ?, ?, ?, ?, ?)")
			var to interface{}
			if t.To != nil {
				to = t.To
			}
			args = append(args, t.Hash, t.BlockNumber, t.BlockHash, t.BlockTime, t.GasUsed, t.From, to)
		}
		b.WriteString(` ON DUPLICATE KEY UPDATE block_number=block_number`)
		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

// ChunkDone reports whether the chunk starting at chunkStart is already recorded
// complete.
func (db *DB) ChunkDone(ctx context.Context, chunkStart int64) (bool, error) {
	var one int
	err := db.sql.QueryRowContext(ctx,
		`SELECT 1 FROM import_progress WHERE chunk_start = ?`, chunkStart).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// MarkChunkDone records the chunk complete and its failed blocks in ONE
// transaction, so a crash cannot leave a "done" chunk whose failures were never
// recorded (they would otherwise be lost to any later rescan).
func (db *DB) MarkChunkDone(ctx context.Context, start, end int64, failures []common.FailedBlock) error {
	return db.withRetry(ctx, func() error {
		tx, err := db.sql.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, f := range failures {
			reason := f.Reason
			if len(reason) > 500 {
				reason = reason[:500]
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO import_failed (block_number, reason) VALUES (?, ?)
				 ON DUPLICATE KEY UPDATE reason=VALUES(reason)`, f.Block, reason); err != nil {
				tx.Rollback()
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO import_progress (chunk_start, chunk_end) VALUES (?, ?)
			 ON DUPLICATE KEY UPDATE chunk_end=VALUES(chunk_end)`, start, end); err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

// ReadFailed returns the block numbers currently recorded as failed.
func (db *DB) ReadFailed(ctx context.Context) ([]int64, error) {
	rows, err := db.sql.QueryContext(ctx, `SELECT block_number FROM import_failed ORDER BY block_number`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// DeleteFailed removes a recovered block from the failed table.
func (db *DB) DeleteFailed(ctx context.Context, block int64) error {
	return db.withRetry(ctx, func() error {
		_, err := db.sql.ExecContext(ctx, `DELETE FROM import_failed WHERE block_number = ?`, block)
		return err
	})
}

// RecordFailed upserts a single failed block (used by the rescan pass when a
// block fails again).
func (db *DB) RecordFailed(ctx context.Context, block int64, reason string) error {
	if len(reason) > 500 {
		reason = reason[:500]
	}
	return db.withRetry(ctx, func() error {
		_, err := db.sql.ExecContext(ctx,
			`INSERT INTO import_failed (block_number, reason) VALUES (?, ?)
			 ON DUPLICATE KEY UPDATE reason=VALUES(reason)`, block, reason)
		return err
	})
}

// withRetry runs fn, retrying transient MySQL/network errors with exponential
// backoff + jitter. Non-transient errors (constraint, truncation, syntax) fail fast.
func (db *DB) withRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= db.maxRetry; attempt++ {
		if attempt > 0 {
			wait := db.baseWait*time.Duration(1<<uint(attempt-1)) + time.Duration(rand.Int63n(int64(db.baseWait)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isTransient(err) {
			return err
		}
	}
	return fmt.Errorf("after %d retries: %w", db.maxRetry, lastErr)
}

// isTransient reports whether a MySQL error is worth retrying.
func isTransient(err error) bool {
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1205, // lock wait timeout
			1213, // deadlock
			1040, // too many connections
			1203: // user already has max connections
			return true
		}
		return false // any other server error (constraint, truncation, ...) fails fast
	}
	// Non-MySQLError (network, timeout, ErrBadConn wrappers) -> transient.
	return true
}
