package importmysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"bsc_stats/common"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func newTestDB(t *testing.T) (*DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	return &DB{sql: sqlDB, maxRetry: 3, baseWait: time.Millisecond}, mock
}

func sampleBlock() *BlockData {
	h := make([]byte, 32)
	a := make([]byte, 20)
	return &BlockData{
		Number: 100, Hash: h, Time: 5, GasUsed: 1000, GasLimit: 2000, TxCount: 2,
		Txs: []TxData{
			{Hash: h, BlockNumber: 100, BlockHash: h, BlockTime: 5, GasUsed: 10, From: a, To: a},
			{Hash: h, BlockNumber: 100, BlockHash: h, BlockTime: 5, GasUsed: 20, From: a, To: nil},
		},
	}
}

// WriteBlock must run block upsert + one batched tx upsert inside a single
// committed transaction.
func TestWriteBlockCommitsOneTx(t *testing.T) {
	db, mock := newTestDB(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO block").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO tx").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	if err := db.WriteBlock(context.Background(), sampleBlock()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A deadlock on commit rolls back; the retry must restart a fresh transaction.
func TestWriteBlockRetriesOnDeadlock(t *testing.T) {
	db, mock := newTestDB(t)
	deadlock := &mysql.MySQLError{Number: 1213, Message: "deadlock"}
	// Attempt 1: begins, block ok, tx ok, commit fails with deadlock.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO block").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO tx").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit().WillReturnError(deadlock)
	// Attempt 2: fresh transaction, succeeds.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO block").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO tx").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	if err := db.WriteBlock(context.Background(), sampleBlock()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A constraint/data error is NOT retried.
func TestWriteBlockFailsFastOnNonTransient(t *testing.T) {
	db, mock := newTestDB(t)
	dataErr := &mysql.MySQLError{Number: 1406, Message: "data too long"}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO block").WillReturnError(dataErr)
	// No second attempt expected.

	err := db.WriteBlock(context.Background(), sampleBlock())
	if err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// MarkChunkDone must write failed rows AND the progress row in one transaction,
// so a crash cannot leave a done chunk whose failures went unrecorded.
func TestMarkChunkDoneAtomic(t *testing.T) {
	db, mock := newTestDB(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO import_failed").WithArgs(7, "boom").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO import_progress").WithArgs(0, 9).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := db.MarkChunkDone(context.Background(), 0, 9, []common.FailedBlock{{Block: 7, Reason: "boom"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestIsTransient(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{driver.ErrBadConn, true},
		{&mysql.MySQLError{Number: 1213}, true},  // deadlock
		{&mysql.MySQLError{Number: 1205}, true},  // lock wait timeout
		{&mysql.MySQLError{Number: 1406}, false}, // data too long
		{&mysql.MySQLError{Number: 1062}, false}, // duplicate key
		{errors.New("some network blip"), true},
	}
	for _, tc := range cases {
		if got := isTransient(tc.err); got != tc.want {
			t.Errorf("isTransient(%v)=%v want %v", tc.err, got, tc.want)
		}
	}
}
