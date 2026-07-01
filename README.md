# bsc_stats

A Go tool for computing statistics over BNB Smart Chain (BSC) data via
JSON-RPC. Functionality is organized into subcommands. The `collect-top`
subcommand is dependency-free; `import-mysql` adds the MySQL driver.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| [`collect-top`](collecttop/README.md) | Scan a block/date range and report transaction stats + top-100 senders |
| [`import-mysql`](importmysql/README.md) | Import block/tx data into MySQL for address + time-range gas-share queries |

Shared low-level primitives (retrying JSON-RPC client, hex helpers, date→block
resolution, chunk math, progress, failed-block log, config helpers) live in the
[`common`](common/) package; each subcommand is its own package.

## Build

Requires Go 1.21+.

```sh
go build -o bsc_stats .
```

> Note: on some macOS + Go 1.21 setups the default linker produces a binary
> that aborts with `dyld: missing LC_UUID`. If you hit this, build/test with
> the pure-Go linker:
>
> ```sh
> CGO_ENABLED=0 go build -o bsc_stats .
> ```

## Run

```sh
./bsc_stats <subcommand> [flags]
```

See each subcommand's README (linked above) for its flags and behavior.

## Test

```sh
CGO_ENABLED=0 go test ./...
```

Tests are offline: they run against an in-process `net/http/httptest` mock
JSON-RPC server and never contact a live endpoint.
