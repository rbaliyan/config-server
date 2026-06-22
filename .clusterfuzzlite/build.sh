#!/bin/bash -eu
compile_native_go_fuzzer github.com/rbaliyan/config-server/gateway FuzzSanitizeSSEField fuzz_sanitize_sse_field
compile_native_go_fuzzer github.com/rbaliyan/config-server/gateway FuzzParseWatchQuery fuzz_parse_watch_query
compile_native_go_fuzzer github.com/rbaliyan/config-server/gateway FuzzIsForwardableHeader fuzz_is_forwardable_header
compile_native_go_fuzzer github.com/rbaliyan/config-server/gateway FuzzResourceFromPath fuzz_resource_from_path
compile_native_go_fuzzer github.com/rbaliyan/config-server/gateway FuzzEventBufferSince fuzz_event_buffer_since
compile_native_go_fuzzer github.com/rbaliyan/config-server/gateway FuzzParseDiffParams fuzz_parse_diff_params
compile_native_go_fuzzer github.com/rbaliyan/config-server/service FuzzServiceGet fuzz_service_get
compile_native_go_fuzzer github.com/rbaliyan/config-server/service FuzzServiceSet fuzz_service_set
compile_native_go_fuzzer github.com/rbaliyan/config-server/service FuzzServiceList fuzz_service_list
compile_native_go_fuzzer github.com/rbaliyan/config-server/service FuzzValueToProtoRoundTrip fuzz_value_to_proto_round_trip
compile_native_go_fuzzer github.com/rbaliyan/config-server/service FuzzDecodeNamespaceCursor fuzz_decode_namespace_cursor

# NOTE: authorizer/opa is a SEPARATE Go module (its own go.mod). OSS-Fuzz's
# compile_native_go_fuzzer builds the ROOT module ($SRC checkout dir) and cannot
# target a fuzz function that lives in a nested module — the package path
# github.com/rbaliyan/config-server/authorizer/opa is not importable from the
# root module without a replace/workspace directive that OSS-Fuzz does not set
# up. The nested-module target FuzzVerifyToken is therefore NOT registered here;
# it is exercised by the `fuzz-smoke` CI job (opa step) instead.
