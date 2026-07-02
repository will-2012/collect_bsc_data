package importmysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"bsc_stats/common"

	"github.com/DATA-DOG/go-sqlmock"
)

// oneBlockMock returns a valid single-tx block for any requested number.
func oneBlockRPC(t *testing.T) *common.Client {
	m := newRPCMock(t)
	m.header = map[string]interface{}{
		"number": "0x5", "hash": bHash, "timestamp": "0x1",
		"gasUsed": "0x1", "gasLimit": "0x2", "transactions": []string{tHash},
	}
	m.receipts = []map[string]interface{}{
		{"transactionHash": tHash, "transactionIndex": "0x0", "from": from1, "to": to1, "gasUsed": "0x1", "blockHash": bHash, "blockNumber": "0x5"},
	}
	return m.client()
}

func testCfg() *Config {
	return &Config{Concurrency: 1, DBConns: 1, ChunkSize: 10}
}

// compensate returns immediately (no rescan, no residual) when import_failed is
// empty — a normal import with zero failures must not enter the rescan loop.
func TestCompensateNoFailures(t *testing.T) {
	db, mock := newTestDB(t)
	mock.ExpectQuery("SELECT block_number FROM import_failed").
		WillReturnRows(sqlmock.NewRows([]string{"block_number"}))
	im := newImporter(testCfg(), nil, db, common.NewProgress(0))
	residual, err := compensate(context.Background(), im)
	if err != nil || residual != 0 {
		t.Fatalf("residual=%d err=%v want 0/nil", residual, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A chunk already in import_progress must be skipped: no block fetch, no write.
func TestImporterSkipsDoneChunk(t *testing.T) {
	db, mock := newTestDB(t)
	mock.ExpectQuery("SELECT 1 FROM import_progress").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	im := newImporter(testCfg(), oneBlockRPC(t), db, common.NewProgress(0))
	if err := im.Run(context.Background(), 5, 5); err != nil {
		t.Fatal(err)
	}
	// No Begin/Exec expected — if importChunk ran, ExpectationsWereMet would flag
	// the unmatched calls.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A fresh chunk: fetch block, write it, then mark the chunk done with no failures.
func TestImporterImportsAndMarksDone(t *testing.T) {
	db, mock := newTestDB(t)
	db.baseWait = time.Millisecond
	mock.ExpectQuery("SELECT 1 FROM import_progress").WithArgs(5).WillReturnError(sql.ErrNoRows)
	// WriteBlock
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO block").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO tx").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	// MarkChunkDone: no failures => just the progress row, in its own tx.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO import_progress").WithArgs(5, 5).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	im := newImporter(testCfg(), oneBlockRPC(t), db, common.NewProgress(1))
	if err := im.Run(context.Background(), 5, 5); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
