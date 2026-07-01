# bsc_stats

A standalone, dependency-free Go tool for computing statistics over BNB Smart
Chain (BSC) data via JSON-RPC. Functionality is organized into subcommands.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| [`collect-top`](collecttop/README.md) | Scan a block/date range and report transaction stats + top-100 senders |

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
