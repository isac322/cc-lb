# Prompt-cache production regression gate

This gate protects the exact incremental prompt-cache analyzer from returning to cumulative full-prefix serialization or tokenization.

## Fixture

The test `production_size_prompt_cache_release_regression_gate` builds its request in memory. No binary fixture is stored in the repository.

The fixture has these fixed properties:

- 4,065,868 serialized request bytes, with an allowed production-size range of 4,000,000–4,800,000 bytes
- 182 alternating user and assistant messages
- one text content block per message
- exactly four cache-control breakpoints at message indexes 44, 90, 136, and 180
- `1h`, `1h`, `5m`, and `5m` TTLs in provider-valid order
- quotes, backslashes, JSON closing delimiters, commas, newlines, tabs, carriage returns, Korean text, emoji with a zero-width joiner, non-breaking space, and repeated whitespace

The breakpoint positions are deep enough to require the full 20-block lookback window. Assertions fix the message count, serialized size, breakpoint count and positions, TTLs, and serializer/tokenizer boundary characters.

## Invariants

The cold analysis must satisfy every invariant below:

- The frozen reference analyzer and optimized analyzer return the same complete `V3PromptCacheAnalysis`.
- All four prefix token counts, prefix hashes, TTLs, and lookback records match.
- Each optimized serialized prefix is byte-identical to the reference serializer output.
- The first analysis records four cache misses and no hits, coalesced claims, or fallback prefixes.
- Tokenizer input bytes and produced tokens stay at or below 1.25 times the deepest prefix. Four cumulative passes cannot fit within this budget.
- The exact-prefix batch owns serialized bytes equal to one deepest prefix. Its serialized capacity stays at or below 2.25 times the deepest prefix, allowing normal `Vec` growth without allowing four retained full-prefix buffers. The test-only accounting helper exhaustively destructures every `ExactPrefixBatch` field without `..`: it counts the current open-prefix and suffix buffers, acknowledges offsets and token keys as non-serialized storage, and forces an explicit accounting update if a new owned field is added.

The second analysis uses the same credential scope and cache. It must return the same reference result, record four cache hits, and perform zero additional tokenizer input-byte, produced-token, or fallback-prefix work.

The test prints the fixture byte count, deepest prefix bytes and tokens, actual cold tokenizer bytes and tokens, warm hit count, and owned serialized storage. These values are structural evidence; the gate does not use wall-clock thresholds, sleeps, retries, or timing-based pass criteria.

## CI command

The dedicated `prompt-cache 4MB release regression` CI job uses the repository's `cc-lb-4` self-hosted runner, pinned stable Rust toolchain, release profile, Rust cache convention, build-skip environment variables, and a 45-minute timeout for the cold fat-LTO build. The step enables pipeline failure propagation, tees the output to a log, and requires exactly one `1 passed; 0 failed` test summary. Renaming or deleting the exact test therefore fails the job instead of passing with zero tests. The test command is:

```sh
CC_LB_ADMIN_SKIP_SPA=1 CC_LB_SKIP_WASM_FIXTURE_BUILD=1 \
  cargo test --locked --release -p cc-lb-engine --lib \
  prompt_cache_simulator::optimized::tests::production_size_prompt_cache_release_regression_gate \
  -- --exact --nocapture
```

## Failure interpretation

- **Fixture byte or shape assertion:** The production fixture was reduced or changed. Confirm the change remains representative before updating any frozen value.
- **Reference equality or prefix-byte assertion:** The optimized path changed prompt-cache identity, token counts, TTL handling, lookback metadata, or exact serialization.
- **Cold cache-count assertion:** Cache claims no longer correspond one-for-one with the four exact prefixes.
- **Tokenizer work budget:** The analyzer likely re-tokenized cumulative prefixes, entered the fallback path, or retained too much unstable input between breakpoints.
- **Owned byte or capacity budget:** The exact-prefix batch likely materialized or retained multiple complete serialized prefixes.
- **Warm assertion:** Exact count keys, credential scoping, or cache reuse changed, causing work on a request that should produce four hits.

Do not loosen a deterministic budget to hide a failure. Compare the printed deepest and actual work values, then inspect exact-prefix preparation, nested tokenization, and cache-key construction.

## Contract checklist

### Fixture

- [x] Deterministic in-memory 4 MB-class fixture
- [x] 182-message shape and exact four-breakpoint assertions
- [x] Unicode, escape, delimiter, and whitespace boundaries
- [x] Valid TTL ordering and 20-block lookback coverage

### Correctness

- [x] Cold full-analysis reference parity
- [x] Exact serialized prefix byte parity
- [x] Explicit token-count, hash, TTL, and lookback parity
- [x] Warm full-analysis parity with four hits and zero tokenizer work

### Work budget

- [x] Near-deepest-prefix input-byte and token budgets
- [x] Zero fallback-prefix requirement
- [x] Test-only owned serialized byte and capacity bounds
- [x] No public production API or timing threshold

### Release CI

- [x] Dedicated stable release-profile job
- [x] Exact named-test command with existing runner and environment conventions
- [x] `--nocapture` evidence output and exactly-one-pass summary guard configured

### Parent verification — 2026-09-08 local evidence

- [x] The exact release test passed twice on the current head with identical counters: fixture 4,065,868 bytes; deepest prefix 4,043,246 bytes and 1,234,134 tokens; actual tokenizer work 4,043,384 bytes and 1,234,189 tokens; four warm hits; owned storage 4,043,246 bytes with 4,980,776-byte capacity.
- [x] The ordinary optimized prompt-cache module run passed 20 tests with the dedicated release gate ignored; the gate passed separately in release mode.
- [x] `cargo fmt --all -- --check` passed.
- [x] `actionlint .github/workflows/ci.yml` passed.
- [x] Independent spec re-review passed G1–G16, and the security review reported zero findings.
