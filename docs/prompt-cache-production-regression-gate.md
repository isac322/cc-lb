# Prompt-cache production regression gate

This gate protects the exact incremental prompt-cache analyzer from returning to cumulative full-prefix serialization or tokenization.

## Fixture

The test `production_size_prompt_cache_regression_gate` builds its request in memory. No binary fixture is stored in the repository.

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

## CI integration

The gate runs as a normal, non-ignored unit test in the existing `nextest-cov` job. That job already checks out the repository, installs the pinned toolchain, restores the Rust cache, and runs the workspace test suite:

```sh
cargo llvm-cov nextest \
  --workspace \
  --exclude cc-lb-loadgen \
  --exclude cc-lb-stress-suite \
  --all-features \
  --no-report
```

The gate uses deterministic serialized-byte, token-count, cache-hit, and owned-capacity assertions. None of its pass criteria depend on optimization level or wall-clock timing, so a separate release-profile build does not strengthen the contract. Keeping it in the ordinary suite also makes local `cc-lb-engine` test runs exercise the production-size case.

Passing test output is captured by nextest. To print the structural counters while investigating a failure or intentionally changing the frozen fixture, run the same gate locally without a separate CI job:

```sh
CC_LB_ADMIN_SKIP_SPA=1 CC_LB_SKIP_WASM_FIXTURE_BUILD=1 \
  cargo test --locked -p cc-lb-engine --lib \
  prompt_cache_simulator::optimized::tests::production_size_prompt_cache_regression_gate \
  -- --exact --nocapture
```

CI measurements before this integration showed that `nextest-cov` took 5 minutes 36 seconds with the gate enabled and 5 minutes 33 seconds with it ignored. The separate release job took 1 minute 55 seconds. Running the gate in the existing suite therefore removes a checkout, toolchain setup, cache setup, runner allocation, and release build for an observed coverage-job increase of about 3 seconds.

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

### Existing coverage suite

- [x] Normal non-ignored test in the existing `nextest-cov` workspace run
- [x] No dedicated job, runner allocation, or release-profile rebuild
- [x] Structural assertions independent of optimization level

### Parent verification — 2026-09-09 integration evidence

- [x] The unchanged gate previously passed twice in release mode with identical counters: fixture 4,065,868 bytes; deepest prefix 4,043,246 bytes and 1,234,134 tokens; actual tokenizer work 4,043,384 bytes and 1,234,189 tokens; four warm hits; owned storage 4,043,246 bytes with 4,980,776-byte capacity.
- [x] The ordinary optimized prompt-cache module run includes the production-size gate and passes all 21 tests.
- [x] CI timing comparison showed about 3 seconds of `nextest-cov` impact versus 1 minute 55 seconds for the removed standalone job.
- [x] `cargo fmt --all -- --check` passed.
- [x] `actionlint .github/workflows/ci.yml` passed.
- [x] Independent spec re-review passed G1–G16, and the security review reported zero findings.
