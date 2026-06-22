# Fuzzing

This repository fuzzes its security-relevant parsers and conversion code with
Go's native fuzzing engine. The same targets are run continuously in CI via
[ClusterFuzzLite](https://google.github.io/clusterfuzzlite/).

## Targets

All targets are standard Go fuzz functions (`func FuzzXxx(f *testing.F)`).

| Target | Package | What it exercises | Key invariants |
|--------|---------|-------------------|----------------|
| `FuzzResourceFromPath` | `gateway` | `resourceFromPath` — the HTTP authorization path parser that maps a request path onto the `service.Resource{Namespace, Key}` that resource-scoped policies fire on | Never panics; derived namespace/key are substrings of the raw path; parse is deterministic and idempotent on the canonical `/v1/namespaces/{ns}/keys/{key}` reconstruction (impersonation guard); never silently percent-decodes `%2e%2e` into `..` |
| `FuzzEventBufferSince` | `gateway` | The SSE `Last-Event-ID` replay path: ring-buffer pushes followed by `eventBuffer.since` driven by an attacker-supplied header value | Never panics; never replays an evicted event; only returns events strictly newer than the cutoff; results are in ascending id (push) order; result count never exceeds the buffer contents |
| `FuzzSanitizeSSEField` | `gateway` | `sanitizeSSEField` — CRLF stripping for SSE field values | Output never contains `\r` or `\n` (header/response-splitting guard) |
| `FuzzParseWatchQuery` | `gateway` | `parseWatchQuery` — watch query-string + `Last-Event-ID` parsing | Never panics |
| `FuzzIsForwardableHeader` | `gateway` | `isForwardableHeader` — header allow-list | Never panics |
| `FuzzServiceGet` | `service` | `ConfigService.Get` over fuzzed namespace/key | Errors are well-formed gRPC statuses (never `Unknown`/`OK`); a successful response echoes the requested namespace/key |
| `FuzzServiceSet` | `service` | `ConfigService.Set` over fuzzed namespace/key/value/codec | Proper gRPC status on error; **no partial write** — a failed `Set` leaves no readable value on a fresh store; success echoes coordinates |
| `FuzzServiceList` | `service` | `ConfigService.List` over fuzzed filters | Proper gRPC status; every returned entry belongs to the requested namespace and matches the requested prefix (no cross-namespace / out-of-prefix leakage) |
| `FuzzValueToProtoRoundTrip` | `service` | `valueToProto` — `config.Value` → proto `Entry` conversion used to serialize stored values onto the wire | Never panics; namespace, key, codec, payload bytes, and type enum are preserved with full fidelity; conversion is idempotent |

## Running locally

Run a single target (replace the name and package as needed):

```bash
# 15 seconds is enough to confirm a target runs cleanly; raise for real campaigns.
go test -run '^$' -fuzz='^FuzzResourceFromPath$' -fuzztime=15s ./gateway
go test -run '^$' -fuzz='^FuzzEventBufferSince$' -fuzztime=15s ./gateway
go test -run '^$' -fuzz='^FuzzValueToProtoRoundTrip$' -fuzztime=15s ./service
go test -run '^$' -fuzz='^FuzzServiceSet$' -fuzztime=15s ./service
```

Replay only the committed seed corpus (fast, deterministic — what CI unit-test
runs do, and what catches regressions on known-interesting inputs):

```bash
go test -run '^Fuzz' ./gateway ./service
```

`-run '^$'` selects no unit tests so only the fuzz loop runs; dropping `-fuzz`
and using `-run '^Fuzz'` replays the corpus once without mutation.

## Seed corpora

Committed seeds live under `testdata/fuzz/<FuzzName>/`. Each file uses Go's
corpus format:

```
go test fuzz v1
string("/v1/namespaces/prod/keys/app/db/host")
```

Inline seeds added with `f.Add(...)` inside each target are the primary source
of coverage; the `testdata/fuzz` files add a few realistic and adversarial
inputs (path traversal, double slashes, encoded `%2e%2e`, integer-overflow
`Last-Event-ID`, binary payloads) that we want pinned regardless of the engine's
generated cache. Inputs the engine discovers during a run are written to the Go
build cache (`$(go env GOCACHE)/fuzz`), not to `testdata`.

## Adding a new target

1. Write `func FuzzXxx(f *testing.F)` in a `_test.go` file in the package under
   test. If the function under test is unexported, the fuzz file must be in the
   **internal** test package (e.g. `package gateway`, not `gateway_test`).
2. Seed it with `f.Add(...)` covering realistic and adversarial inputs.
3. Assert a real invariant, not just "doesn't panic" — round-trip fidelity, a
   well-formed error, an ordering/containment property, etc.
4. Add a few `testdata/fuzz/FuzzXxx/` corpus files for the inputs worth pinning.
5. Register it in `.clusterfuzzlite/build.sh` with `compile_native_go_fuzzer`.
6. Confirm it runs: `go test -run '^$' -fuzz='^FuzzXxx$' -fuzztime=15s ./pkg`.

## Triaging a crash

When a target fails, Go writes the offending input to
`testdata/fuzz/<FuzzName>/<hash>` and prints a re-run command:

```bash
go test -run='FuzzXxx/<hash>' ./pkg
```

That input is now a permanent regression case. Workflow:

1. Reproduce with the printed `-run` command.
2. Decide whether the failure is a **production bug** or an **over-strict
   oracle**. If it is a production bug, fix the production code and keep the
   crashing input as a regression seed. If the oracle was wrong, fix the
   assertion — do not silently delete the crasher.
3. Re-run the target to confirm the corpus is green, then commit the new
   `testdata/fuzz` entry alongside the fix.

## CI

`.clusterfuzzlite/build.sh` compiles every target listed above as a native Go
fuzzer. ClusterFuzzLite runs them on pull requests and on a schedule; see
`.clusterfuzzlite/` and `SECURITY.md` for the security context.
