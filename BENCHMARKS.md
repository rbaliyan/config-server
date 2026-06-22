# Benchmarks

This repository ships Go benchmarks for the performance-sensitive hot paths that
run on every request or cluster event. They live alongside the code they measure
in `bench_test.go` files and use the `b.Loop()` idiom with `b.ReportAllocs()`.

## What is covered

| Package    | Benchmark | Hot path |
|------------|-----------|----------|
| `peersync` | `BenchmarkRingOwnerOf` | Consistent-hash lookup (RLock + binary search) on every routed op |
| `peersync` | `BenchmarkRingOwnerOfParallel` | `OwnerOf` read path under concurrent readers (RWMutex contention) |
| `peersync` | `BenchmarkRingAdd` | Member registration — inserts 150 vnodes (`fmt.Sprintf` per vnode) |
| `peersync` | `BenchmarkRingApply` | Full ring rebuild from a gossiped `RingState` |
| `service`  | `BenchmarkClassifyError` | `config` error → gRPC status mapping on every failed RPC |
| `service`  | `BenchmarkValueToProto` | `config.Value` → proto `Entry` on every Get/List/Snapshot entry |
| `gateway`  | `BenchmarkEventBufferPush` | SSE ring-buffer push (Last-Event-ID replay support) |
| `gateway`  | `BenchmarkEventBufferSince` | SSE replay scan over a full buffer on reconnect |
| `client`   | `BenchmarkIsNonRetryable` | Retry classification on every failed RPC attempt |
| `client`   | `BenchmarkIsCircuitOpen` | Circuit-breaker gate (mutex) consulted before every RPC |

The `peersync` `RingAdd` and `RingApply` benchmarks deliberately report
allocations: both call `fmt.Sprintf("%s#%d", ...)` once per virtual node (150
vnodes per member), so the alloc counts scale with member count. `ReportAllocs`
makes that cost visible and lets you track it across changes.

## Running

```bash
# Full run: 10 iterations per benchmark with allocation stats, saved to bench.txt
just bench

# Or directly:
go test -run '^$' -bench=. -benchmem -count=10 ./...

# Quick smoke run (verify they compile and execute, no stable timings):
go test -run '^$' -bench=. -benchmem -benchtime=10x ./...

# A single package or benchmark:
go test -run '^$' -bench=BenchmarkRingOwnerOf -benchmem ./peersync/
```

## Baseline and comparison

`baseline.txt` holds a captured `-count=10` run. To check for regressions,
produce a new run and compare with
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
go test -run '^$' -bench=. -benchmem -count=10 ./... > new.txt
go run golang.org/x/perf/cmd/benchstat@latest baseline.txt new.txt
```

## CI

The `benchmarks` job in `.github/workflows/ci.yml` runs the suite with
`-benchtime=1x` on every push and pull request. It is a correctness gate that
ensures the benchmarks keep compiling and running; it is not run under `-race`
and is not used for performance measurement, since hosted runners are too noisy
for stable timings.
