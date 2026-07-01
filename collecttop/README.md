# collect-top

Scans a range of BNB Smart Chain (BSC) blocks over JSON-RPC and produces
transaction statistics for the range.

## What it does

Given a block range (resolved from UTC dates, or given explicitly), it reports:

1. **Total transaction count.**
2. **Transaction-type breakdown** — count and percentage for each type:
   - `0x00` LegacyTx
   - `0x01` AccessListTx (EIP-2930)
   - `0x02` DynamicFeeTx (EIP-1559)
   - `0x03` BlobTx (EIP-4844)
   - `0x04` SetCodeTx (EIP-7702)
3. **Top-100 `To` addresses**, split into two independent rankings:
   - top-100 **contract** addresses
   - top-100 **EOA** (non-contract) addresses

   each with occurrence count and percentage. Contract-creation transactions
   (`to == null`) are counted separately and excluded from the rankings.

Outputs (in `out_dir`): `summary.json`, `top100_contracts.csv`,
`top100_eoa.csv`, and a merged human-readable `report.md`.

## Design & principles

The data source is a plain JSON-RPC endpoint with no server-side aggregation,
so all statistics are computed client-side by scanning every block in the
range. The range can span tens of millions of blocks, and a full run can take
a long time, so the design is built around three properties: **bounded memory,
resumability, and fault tolerance**.

```
date range ──► binary search by block timestamp ──► [startBlock, endBlock]
                                                          │
        split into fixed-size chunks (default 100k blocks)
                                                          │
        worker pool (configurable concurrency)
            per block: eth_getBlockByNumber(n, true)
            extract each tx's type and `to`
                                                          │
        chunk complete ──► atomic write (temp file + rename) of
                           { total tx, per-type counts, addr→count }
                                                          │
   merge all chunk files ──► global totals + addr→count
                                                          │
   classify top addresses via eth_getCode (contract vs EOA)
                                                          │
   write summary.json / CSVs / report.md
```

Key mechanisms:

- **Chunked, resumable scanning.** Work is split into chunks. A chunk's result
  file is written atomically only after every block in it succeeds; its
  presence marks the chunk complete. On restart, completed chunks are skipped,
  so a killed or interrupted run resumes from where it stopped. A chunk
  interrupted mid-flight is never persisted, avoiding partial/double counting.
- **Block range resolution.** Start/end blocks are found by binary search over
  block timestamps (lower bound for start, upper bound for end). Header lookups
  use `full=false` and decode only `number`/`timestamp`.
- **Contract vs EOA classification.** After merging, addresses are sorted by
  count descending; `eth_getCode` is called (concurrently, in count-ordered
  batches) walking down the list until both top-100 lists are filled. Only a
  few hundred to a few thousand `getCode` calls are needed regardless of total
  address count.
- **Fault tolerance.** Every RPC call retries with exponential backoff and
  jitter. Blocks that exhaust retries are appended to `failed_blocks.log`
  (with reason) and the scan continues; they can be re-fetched later with
  `-rescan_failed`. If any blocks failed, the report records the count and
  emits a warning so the output is not silently incomplete.
- **Progress reporting.** A background reporter prints overall progress every
  10 minutes (blocks done / total, percent, cumulative tx, elapsed, ETA), plus
  a final summary on exit.

### Performance note

`eth_getBlockByNumber(n, true)` returns the full transaction objects of a
block and is comparatively heavy. Effective throughput is usually bounded by
the endpoint's compute-unit / rate limits on this method rather than by client
concurrency — raising concurrency past a moderate level may not increase
throughput. Size the date range and pick an endpoint tier accordingly; for
very large ranges, ensure the endpoint provides enough capacity.

## Usage

The endpoint is required (it may contain an API key, so it is never hardcoded):

```sh
./bsc_stats collect-top \
  -endpoint "https://your-bsc-rpc.example/v1/<API_KEY>" \
  -start_date 2025-05-01 \
  -end_date   2026-06-30 \
  -concurrency 500 \
  -chunk_size 100000 \
  -out_dir ./out
```

Scan an explicit block range instead of dates (overrides `-start_date`/`-end_date`):

```sh
./bsc_stats collect-top -endpoint "$BSC_ENDPOINT" -start_block 107000590 -end_block 107001589
```

Re-scan blocks that previously failed all retries, then exit:

```sh
./bsc_stats collect-top -endpoint "$BSC_ENDPOINT" -rescan_failed -out_dir ./out
```

## Flags

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-endpoint` | `BSC_ENDPOINT` | (required) | JSON-RPC endpoint |
| `-concurrency` | `BSC_CONCURRENCY` | `500` | Worker concurrency |
| `-start_date` | `BSC_START_DATE` | `2025-05-01` | Inclusive UTC start date (YYYY-MM-DD) |
| `-end_date` | `BSC_END_DATE` | `2026-06-30` | Inclusive UTC end date (YYYY-MM-DD) |
| `-start_block` | — | `-1` | Explicit inclusive start block (with `-end_block`, overrides dates) |
| `-end_block` | — | `-1` | Explicit inclusive end block (with `-start_block`, overrides dates) |
| `-chunk_size` | `BSC_CHUNK_SIZE` | `100000` | Blocks per chunk |
| `-out_dir` | `BSC_OUT_DIR` | `./out` | Output directory |
| `-rescan_failed` | — | `false` | Re-scan blocks in `failed_blocks.log` and exit |
