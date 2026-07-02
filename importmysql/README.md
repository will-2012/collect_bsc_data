# import-mysql

Imports BNB Smart Chain (BSC) block and transaction data from a JSON-RPC
endpoint into MySQL, so transactions can later be analyzed by address and time
range — specifically, the **share of a given address's transactions' gas within
total block gas used / gas limit** over a time window.

## What it stores

Two data tables (plus two bookkeeping tables for resume/retry):

**`block`** — one row per block: `number` (PK), `hash`, `block_time` (unix
seconds), `gas_used`, `gas_limit`, `tx_count`.

**`tx`** — one row per transaction, keyed by `(block_number, tx_index)` (the
tx's on-chain position in its block): `hash`, `block_hash`, `block_time`,
`gas_used`, `from_addr`, `to_addr` (NULL for contract creation).

`tx.gas_used` is the **real gas consumed**, taken from the transaction receipt
(`eth_getBlockReceipts`) — not the block body's per-tx `gas` field, which is the
gas *limit*. Getting this right is the whole point of the metric.

## Schema (DDL)

The importer runs these `CREATE TABLE IF NOT EXISTS` statements on startup
(source: `db.go`), so a fresh database is usable without a separate migration.
MySQL 8.0+, InnoDB. **Only primary keys are created during import** — the
analysis (secondary) indexes are built afterward in one pass; see
[Post-import indexes](#post-import-indexes) for why and how.

```sql
CREATE TABLE IF NOT EXISTS block (
    number      BIGINT UNSIGNED NOT NULL,           -- block height (PK)
    hash        BINARY(32)      NOT NULL,           -- 0x-stripped block hash
    block_time  BIGINT UNSIGNED NOT NULL,           -- unix seconds
    gas_used    BIGINT UNSIGNED NOT NULL,
    gas_limit   BIGINT UNSIGNED NOT NULL,
    tx_count    INT    UNSIGNED NOT NULL,
    PRIMARY KEY (number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tx (
    block_number  BIGINT UNSIGNED NOT NULL,         -- FK-ish to block.number
    tx_index      INT    UNSIGNED NOT NULL,         -- on-chain position in block
    hash          BINARY(32)      NOT NULL,         -- tx hash (indexed post-import if needed)
    block_hash    BINARY(32)      NOT NULL,
    block_time    BIGINT UNSIGNED NOT NULL,         -- denormalized from block (unix seconds)
    gas_used      BIGINT UNSIGNED NOT NULL,         -- receipt gasUsed (real gas consumed)
    from_addr     BINARY(20)      NOT NULL,
    to_addr       BINARY(20)      NULL,             -- NULL for contract creation
    -- (block_number, tx_index): near-sequential clustered-index inserts (a moving
    -- per-chunk window that stays hot in the buffer pool) instead of the random
    -- scatter a hash PK causes; also serves per-block lookups (WHERE block_number = ?)
    -- from the PK prefix, so no separate block_number index is needed.
    PRIMARY KEY (block_number, tx_index)
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

Why these fields (short version): addresses `BINARY(20)` / hashes `BINARY(32)`
are raw bytes (~2× smaller than hex, so the analysis indexes keep more of the
buffer pool); `block_time` is unix seconds denormalized onto `tx` so the address
+ time-range query needs no join; the `tx` PK `(block_number, tx_index)` keeps
the bulk load off the disk's random-IO wall (near-sequential inserts). See below
for the full rationale.

## Post-import indexes

The analysis queries below rely on secondary indexes that are **not** created
during import. Maintaining them while inserting billions of rows turns each
insert into several random reads+writes and saturates the disk's IOPS long
before CPU/network — the disk becomes the bottleneck and the import crawls.
Loading with primary keys only, then building the indexes once (a single
sort-build, not per-row random IO), is dramatically faster overall.

Run this **once, after the import (and its `-rescan_failed` compensation) has
fully completed**:

```sql
ALTER TABLE tx
    ADD INDEX idx_from_time (from_addr, block_time, gas_used),
    ADD INDEX idx_to_time   (to_addr,   block_time, gas_used);

ALTER TABLE block
    ADD INDEX idx_block_time_gas (block_time, gas_used, gas_limit),
    ADD INDEX idx_gas_limit      (gas_limit, block_time);

-- Optional: only if you look transactions up by hash. Costs a large index on a
-- huge table, so skip it unless you need it.
-- ALTER TABLE tx ADD INDEX idx_hash (hash);
```

The `tx` indexes lead with the address and carry `block_time` + `gas_used`.
InnoDB appends the primary key `(block_number, tx_index)` to every secondary
index leaf, so `block_number` rides along for free — the numerator's
`SUM(gas_used)` **and** `COUNT(DISTINCT block_number)` are both answered
index-only, with no explicit `block_number` column needed in the index
definition. `idx_block_time_gas` / `idx_gas_limit` answer the block-side
denominators and gas-limit-era grouping index-only.

## The query this schema is built for

Given an address and a time range `[t0, t1]` (unix seconds), the analysis is two
index-only queries combined in the app layer. Run them in one `REPEATABLE READ`
transaction so numerator and denominator see a consistent snapshot.

Denominators (all blocks in the range) — served by `block` index `(block_time, gas_used, gas_limit)`:

```sql
SELECT COUNT(*), SUM(gas_used), SUM(gas_limit)
FROM block WHERE block_time BETWEEN ? AND ?;
```

Numerator (matching txs) — served by `tx` covering index `(from_addr, block_time, gas_used)` (with `block_number` from InnoDB's PK suffix):

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
- **Sequential `tx` primary key `(block_number, tx_index)`.** A hash PK scatters
  inserts randomly across a clustered index that quickly outgrows the buffer
  pool, so every insert becomes a random read+write and the disk's IOPS caps
  throughput. Keying by `(block_number, tx_index)` makes inserts near-sequential
  — each chunk touches a narrow, hot window of the index — which is what keeps a
  multi-billion-row load off the random-IO wall. It also serves per-block lookups
  and `COUNT(DISTINCT block_number)` from the PK / secondary-index leaf.
- **Covering analysis indexes (built post-import).** `(addr, block_time,
  gas_used)` carries everything the aggregate needs (`SUM(gas_used)`, plus
  `block_number` implicitly via InnoDB's PK suffix for `COUNT(DISTINCT
  block_number)`) in the index leaf, avoiding per-row PK lookups that would be
  millions of random reads for a busy address.
- **`BINARY(20)` addresses / `BINARY(32)` hashes.** Raw bytes are ~2× smaller
  than hex strings, so the analysis indexes keep more in the buffer pool. Query
  with `UNHEX(...)` on input and `HEX(...)` on output.
- Only the two single-address indexes are built. A combined
  `(from_addr, to_addr, ...)` index is intentionally omitted; add it only if
  filtering by both together becomes a hot path.

## Import behavior

- **Concurrency.** `-concurrency` bounds simultaneous requests to the endpoint
  (each block fetch issues its two RPC calls sequentially, so in-flight requests
  ≈ `-concurrency`). Default 100 — raise it only within your provider's
  connection/rate ceiling. MySQL writes are throttled separately by `-db_conns`
  (kept well below `-concurrency`). Each block (its row + all its tx rows) is
  written in one transaction.
- **Resumable.** Work is split into fixed-size chunks. A chunk is recorded
  `done` in `import_progress` only after every block in it is durably committed;
  on restart, done chunks are skipped. An interrupted chunk is fully redone —
  and re-import is idempotent (`INSERT … ON DUPLICATE KEY UPDATE` on the PKs
  `block.number` / `tx.(block_number, tx_index)`), so nothing is double-counted.
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
| `-confirmations` | — | `15` | Skip blocks within this many of chain head (reorg safety) |
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

**(a) Average matching txs per block** for a single-side address set. A tx
appears once per side, so no dedup is needed here — this stays index-only on
`idx_to_time` (never select `hash`: it isn't in the index leaf and would force a
per-row clustered-index lookup):

```sql
SELECT COUNT(*) AS matching_txs,
       COUNT(DISTINCT block_number) AS matched_blocks,
       COUNT(*) / COUNT(DISTINCT block_number) AS avg_tx_per_block
FROM tx
WHERE to_addr IN (UNHEX('55d3…'), UNHEX('8ac7…')) AND block_time BETWEEN :t0 AND :t1;
```

If you instead need both sides at once (a tx that is both a matching `from` and
a matching `to` would be counted twice by a `UNION ALL`), dedup on the primary
key `(block_number, tx_index)` — never `hash`, which is uncovered:

```sql
SELECT COUNT(*) AS matching_txs, COUNT(DISTINCT block_number) AS matched_blocks
FROM (
  SELECT block_number, tx_index FROM tx WHERE from_addr IN (UNHEX('…')) AND block_time BETWEEN :t0 AND :t1
  UNION
  SELECT block_number, tx_index FROM tx WHERE to_addr   IN (UNHEX('…')) AND block_time BETWEEN :t0 AND :t1
) m;
```

**(b) Gas-used share** = matching tx gas ÷ **all** block gas in the window (both
vs `gas_used` and vs `gas_limit`). The denominator is every block in the range,
NOT only matched blocks — this is "what fraction of total chain gas this set
consumed". (Numerator is index-only on `idx_to_time`; block sums are index-only
on `idx_block_time_gas`.)

```sql
SELECT
  (SELECT SUM(gas_used)  FROM tx    WHERE to_addr IN (UNHEX('55d3…')) AND block_time BETWEEN :t0 AND :t1) AS tx_gas_used,
  (SELECT SUM(gas_used)  FROM block WHERE block_time BETWEEN :t0 AND :t1)                                  AS blk_gas_used,
  (SELECT SUM(gas_limit) FROM block WHERE block_time BETWEEN :t0 AND :t1)                                  AS blk_gas_limit,
  (SELECT SUM(gas_used) FROM tx WHERE to_addr IN (UNHEX('55d3…')) AND block_time BETWEEN :t0 AND :t1)
    / (SELECT SUM(gas_used)  FROM block WHERE block_time BETWEEN :t0 AND :t1) AS share_of_used,
  (SELECT SUM(gas_used) FROM tx WHERE to_addr IN (UNHEX('55d3…')) AND block_time BETWEEN :t0 AND :t1)
    / (SELECT SUM(gas_limit) FROM block WHERE block_time BETWEEN :t0 AND :t1) AS share_of_limit;
```

> **Attribution caveat**: `tx.gas_used` is the receipt's top-level gas, attributed
> to the tx's *top-level* `from`/`to`. So a set matched on `to_addr` counts gas of
> txs *directly addressed* to those contracts and **excludes** gas where they are
> reached via internal calls from a router/aggregator (and includes the tx's own
> downstream internal-call gas). Execution-level attribution would need tracing
> (`debug_traceTransaction`), which this importer does not collect.

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
gas-limit era by using each era's `[first, last]` as `:t0/:t1` (use half-open
bounds — `>= first AND < next_first` — so a block at a shared boundary second
isn't counted in two eras).

Notes:
- **`both`-side sets** (a wallet matched as either sender or receiver): match with
  `WHERE from_addr IN (…) OR to_addr IN (…)` and aggregate once (one row per tx →
  no double count). If you instead `UNION` two per-side queries, dedup on the PK
  `(block_number, tx_index)` — not `tx.hash`, which is not in the analysis indexes
  and would break index-only execution.
- **Complements / totals**: `to_addr NOT IN (…)` silently drops contract-creation
  txs (`to_addr IS NULL`); add `OR to_addr IS NULL` if they should be included, or
  take the total from the `block` table.
- Do **not** sum shares across address groups — a tx can belong to a `from` group
  and a `to` group at once.
- For a consistent snapshot during a live import, wrap the queries in one
  `REPEATABLE READ` transaction with `@t1` pinned once.

## Completeness & operability

- A run is complete only when it exits **0**. It exits non-zero (and logs a
  `WARNING`) if any block still fails after the automatic compensation rounds, or
  if the whole-range block-coverage check finds a gap. For an unattended
  (cron) backfill, treat a non-zero exit as an alert.
- Authoritative completeness check: `SELECT COUNT(*) FROM import_failed` must be
  `0`, and `SELECT COUNT(*) FROM block WHERE number BETWEEN :start AND :end` must
  equal `:end-:start+1`.
- For a resumable production backfill, prefer explicit `-start_block/-end_block`
  so the chunk grid (resume key) is stable across runs; changing `-start_date` or
  `-chunk_size` between resumes shifts the grid.
- **Reorg safety**: `-confirmations` (default 15) keeps the import a margin below
  chain head. Blocks are written with idempotent no-op upserts, so a block
  imported before finality that later reorgs would NOT be healed on re-import —
  hence never import within finality of head. Historical backfills (end well in
  the past) are unaffected.
