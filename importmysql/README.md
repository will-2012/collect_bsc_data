# import-mysql

Imports BNB Smart Chain (BSC) block and transaction data from a JSON-RPC
endpoint into MySQL, so transactions can later be analyzed by address and time
range — specifically, the **share of a given address's transactions' gas within
total block gas used / gas limit** over a time window.

## What it stores

Two data tables (plus two bookkeeping tables for resume/retry):

**`block`** — one row per block: `number` (PK), `hash`, `block_time` (unix
seconds), `gas_used`, `gas_limit`, `tx_count`.

**`tx`** — one row per transaction: `hash` (PK), `block_number`, `block_hash`,
`block_time`, `gas_used`, `from_addr`, `to_addr` (NULL for contract creation).

`tx.gas_used` is the **real gas consumed**, taken from the transaction receipt
(`eth_getBlockReceipts`) — not the block body's per-tx `gas` field, which is the
gas *limit*. Getting this right is the whole point of the metric.

## Schema (DDL)

The importer runs these `CREATE TABLE IF NOT EXISTS` statements on startup
(source: `db.go`), so a fresh database is usable without a separate migration.
MySQL 8.0+, InnoDB.

```sql
CREATE TABLE IF NOT EXISTS block (
    number      BIGINT UNSIGNED NOT NULL,           -- block height (PK)
    hash        BINARY(32)      NOT NULL,           -- 0x-stripped block hash
    block_time  BIGINT UNSIGNED NOT NULL,           -- unix seconds
    gas_used    BIGINT UNSIGNED NOT NULL,
    gas_limit   BIGINT UNSIGNED NOT NULL,
    tx_count    INT    UNSIGNED NOT NULL,
    PRIMARY KEY (number),
    -- denominators of the gas-share query (COUNT/SUM over a time range),
    -- answered index-only from the leaf:
    KEY idx_block_time_gas (block_time, gas_used, gas_limit),
    -- gas_limit leading, so "which gas-limit eras exist + their time spans"
    -- (GROUP BY gas_limit, MIN/MAX(block_time)) is a loose index scan over the
    -- handful of distinct values, not a full-year table scan:
    KEY idx_gas_limit (gas_limit, block_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tx (
    hash          BINARY(32)      NOT NULL,         -- tx hash (PK)
    block_number  BIGINT UNSIGNED NOT NULL,
    block_hash    BINARY(32)      NOT NULL,
    block_time    BIGINT UNSIGNED NOT NULL,         -- denormalized from block (unix seconds)
    gas_used      BIGINT UNSIGNED NOT NULL,         -- receipt gasUsed (real gas consumed)
    from_addr     BINARY(20)      NOT NULL,
    to_addr       BINARY(20)      NULL,             -- NULL for contract creation
    PRIMARY KEY (hash),
    -- covering indexes: (addr, block_time) equality+range prefix, with
    -- block_number + gas_used in the leaf so the numerator's
    -- COUNT(DISTINCT block_number) and SUM(gas_used) are index-only:
    KEY idx_from_time (from_addr, block_time, block_number, gas_used),
    KEY idx_to_time   (to_addr,   block_time, block_number, gas_used),
    -- tx<->block reverse lookups (all txs of a block; join to block PK):
    KEY idx_block_number (block_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Resume bookkeeping: a chunk is recorded here only after all its blocks commit.
CREATE TABLE IF NOT EXISTS import_progress (
    chunk_start BIGINT UNSIGNED NOT NULL,
    chunk_end   BIGINT UNSIGNED NOT NULL,
    done_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chunk_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Blocks that exhausted retries; written in the same txn as the chunk's
-- done-marker, and cleared by a successful -rescan_failed pass.
CREATE TABLE IF NOT EXISTS import_failed (
    block_number BIGINT UNSIGNED NOT NULL,
    reason       VARCHAR(500)    NOT NULL,
    failed_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (block_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Why these fields/indexes (short version): addresses `BINARY(20)` / hashes
`BINARY(32)` are raw bytes (~2× smaller than hex, so the hot `tx` indexes keep
more of the buffer pool); `block_time` is unix seconds denormalized onto `tx` so
the address + time-range query needs no join; the two `tx` covering indexes make
the numerator index-only, and `idx_block_time_gas` does the same for the block
denominators. See below for the full rationale.

## The query this schema is built for

Given an address and a time range `[t0, t1]` (unix seconds), the analysis is two
index-only queries combined in the app layer. Run them in one `REPEATABLE READ`
transaction so numerator and denominator see a consistent snapshot.

Denominators (all blocks in the range) — served by `block` index `(block_time, gas_used, gas_limit)`:

```sql
SELECT COUNT(*), SUM(gas_used), SUM(gas_limit)
FROM block WHERE block_time BETWEEN ? AND ?;
```

Numerator (matching txs) — served by `tx` covering index `(from_addr, block_time, block_number, gas_used)`:

```sql
SELECT COUNT(DISTINCT block_number), SUM(gas_used)
FROM tx WHERE from_addr = UNHEX(?) AND block_time BETWEEN ? AND ?;
```

(`to_addr` uses the symmetric `idx_to_time`.) Then:

- **block coverage** = `COUNT(DISTINCT block_number) / COUNT(*)`
- **gas-used share** = `SUM(tx.gas_used) / SUM(block.gas_used)`
- **gas-limit share** = `SUM(tx.gas_used) / SUM(block.gas_limit)`

## Schema decisions

- **`block_time` denormalized onto `tx`.** The query filters tx by *time*, but a
  tx has no timestamp of its own. Copying the block's time onto each tx row lets
  a single composite index answer `addr = ? AND block_time BETWEEN ? AND ?` in
  one range scan — no join to `block`. It is immutable (copied once at import),
  so it never drifts.
- **Covering indexes.** `(addr, block_time, block_number, gas_used)` carries
  everything the aggregate needs (`COUNT(DISTINCT block_number)`,
  `SUM(gas_used)`) in the index leaf, avoiding a per-row primary-key lookup that
  would otherwise be millions of random reads for a busy address.
- **`BINARY(20)` addresses / `BINARY(32)` hashes.** Raw bytes are ~2× smaller
  than hex strings, so the hot `tx` indexes keep more in the buffer pool. Query
  with `UNHEX(...)` on input and `HEX(...)` on output.
- **`tx.block_number` indexed** (`idx_block_number`) so tx↔block reverse lookups
  (all txs of a block, or a tx's block via `block.number` PK) stay index-driven.
- Only the two single-address indexes are created. A combined
  `(from_addr, to_addr, ...)` index is intentionally omitted; add it only if
  filtering by both together becomes a hot path.

## Import behavior

- **Concurrency.** `-concurrency` bounds simultaneous requests to the endpoint
  (each block fetch issues its two RPC calls sequentially, so in-flight requests
  ≈ `-concurrency`). Default 100 — raise it only within your provider's
  connection/rate ceiling. MySQL writes are throttled separately by `-db_conns`
  (kept low — hundreds of concurrent random-PK inserts would cause deadlock
  storms). Each block (its row + all its tx rows) is written in one transaction.
- **Resumable.** Work is split into fixed-size chunks. A chunk is recorded
  `done` in `import_progress` only after every block in it is durably committed;
  on restart, done chunks are skipped. An interrupted chunk is fully redone —
  and re-import is idempotent (`INSERT … ON DUPLICATE KEY UPDATE` on the natural
  PKs `block.number` / `tx.hash`), so nothing is double-counted.
- **Fault tolerance.** Every RPC and MySQL call retries with exponential backoff
  + jitter (transient DB errors — deadlock, lock-wait, bad-conn — only; a
  constraint/data error fails fast). A block that exhausts retries is recorded
  in `import_failed` **in the same transaction that marks its chunk done**, so a
  crash can't lose it.
- **Automatic compensation.** At the end of a normal run the importer
  automatically re-imports any `import_failed` blocks (a few bounded rounds), so
  a plain re-run reaches full coverage. If some blocks still fail, it logs a
  warning **and exits non-zero** so a partial import is not mistaken for
  complete. `-rescan_failed` runs this compensation pass standalone.
- **Progress.** A progress line (blocks / %, tx, failed, elapsed, ETA) is logged
  every `-progress_interval` (default 1m), plus a final line on exit.

## Usage

```sh
./bsc_stats import-mysql \
  -endpoint  "https://your-bsc-rpc.example/v1/<API_KEY>" \
  -mysql_dsn "user:pass@tcp(127.0.0.1:3306)/bsc" \
  -start_date 2025-05-01 \
  -end_date   2026-06-30 \
  -concurrency 100 \
  -db_conns 16 \
  -chunk_size 10000
```

Explicit block range (overrides dates):

```sh
./bsc_stats import-mysql -endpoint "$BSC_ENDPOINT" -mysql_dsn "$BSC_MYSQL_DSN" \
  -start_block 107000590 -end_block 107001589
```

Retry blocks that failed all retries, then exit:

```sh
./bsc_stats import-mysql -endpoint "$BSC_ENDPOINT" -mysql_dsn "$BSC_MYSQL_DSN" -rescan_failed
```

## Flags

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-endpoint` | `BSC_ENDPOINT` | (required) | JSON-RPC endpoint |
| `-mysql_dsn` | `BSC_MYSQL_DSN` | (required) | MySQL DSN, e.g. `user:pass@tcp(host:3306)/bsc` |
| `-concurrency` | `BSC_CONCURRENCY` | `100` | Simultaneous requests to the endpoint |
| `-db_conns` | `BSC_DB_CONNS` | `16` | Max concurrent MySQL writers |
| `-chunk_size` | `BSC_CHUNK_SIZE` | `10000` | Blocks per resumable chunk |
| `-progress_interval` | — | `1m` | Interval between progress log lines |
| `-start_date` | `BSC_START_DATE` | `2025-05-01` | Inclusive UTC start date (YYYY-MM-DD) |
| `-end_date` | `BSC_END_DATE` | `2026-06-30` | Inclusive UTC end date (YYYY-MM-DD) |
| `-start_block` | — | `-1` | Explicit inclusive start block (with `-end_block`, overrides dates) |
| `-end_block` | — | `-1` | Explicit inclusive end block (with `-start_block`, overrides dates) |
| `-rescan_failed` | — | `false` | Run the failed-block compensation pass standalone, then exit |

## Ad-hoc analysis (example SQL)

Analysis is intentionally done as plain SQL (needs are fluid), not a subcommand.
The schema is built so the core questions are answered index-only. Classify txs
by `to_addr` (contracts) or `from_addr` (sender EOAs); addresses are `BINARY(20)`
so match with `UNHEX('<40-hex>')` (lowercase, no `0x`, never `LOWER()`). Bounds
`:t0`/`:t1` are unix seconds. Contract-creation txs have `to_addr IS NULL`.

**(a) Average matching txs per block** for an address set (dedup by `hash` so a
tx matching both sides isn't counted twice; drop the `UNION` if the set is
purely one side):

```sql
SELECT COUNT(*) AS matching_txs,
       COUNT(DISTINCT block_number) AS matched_blocks,
       COUNT(*) / COUNT(DISTINCT block_number) AS avg_tx_per_block
FROM (
  SELECT hash, block_number FROM tx
    WHERE to_addr IN (UNHEX('55d3…'), UNHEX('8ac7…')) AND block_time BETWEEN :t0 AND :t1
) m;
```

**(b) Gas-used share** vs block gas_used AND block gas_limit, over the blocks the
matching txs touch (numerator index-only; denominators are `block` PK lookups):

```sql
WITH matched AS (
  SELECT DISTINCT block_number, gas_used, hash FROM tx
   WHERE to_addr IN (UNHEX('55d3…')) AND block_time BETWEEN :t0 AND :t1
)
SELECT (SELECT SUM(gas_used) FROM matched)                                  AS tx_gas_used,
       SUM(b.gas_used)                                                      AS blk_gas_used,
       SUM(b.gas_limit)                                                     AS blk_gas_limit,
       (SELECT SUM(gas_used) FROM matched) / SUM(b.gas_used)                AS share_of_used,
       (SELECT SUM(gas_used) FROM matched) / SUM(b.gas_limit)               AS share_of_limit
FROM block b
WHERE b.number IN (SELECT DISTINCT block_number FROM matched);
```

**(c) Block coverage** = matched blocks / all blocks in the range:

```sql
SELECT
  (SELECT COUNT(DISTINCT block_number) FROM tx
     WHERE to_addr IN (UNHEX('55d3…')) AND block_time BETWEEN :t0 AND :t1)
  / (SELECT COUNT(*) FROM block WHERE block_time BETWEEN :t0 AND :t1) AS coverage;
```

**(d) Which gas-limit eras exist + their time spans** (a handful of distinct
values over the year; served by `idx_gas_limit`):

```sql
SELECT gas_limit,
       COUNT(*)        AS blocks,
       FROM_UNIXTIME(MIN(block_time)) AS first_ts,
       FROM_UNIXTIME(MAX(block_time)) AS last_ts
FROM block
GROUP BY gas_limit
ORDER BY MIN(block_time);
```

Run window-based views (12/6/3/1-month) by setting `:t0 = :t1 - Δ`; segment by
gas-limit era by using each era's `[first, last]` as `:t0/:t1`. For a consistent
snapshot during a live import, wrap the queries in one `REPEATABLE READ`
transaction. Do **not** sum shares across address groups — a tx can belong to a
`from` group and a `to` group at once.
