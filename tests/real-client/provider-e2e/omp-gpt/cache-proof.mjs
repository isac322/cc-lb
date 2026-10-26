#!/usr/bin/env node
// OpenAI prompt-cache proof for the omp-gpt slice, derived only from saved observations:
// tap records (redacted request/response shape), cc-lb request events and the installed price catalog.
// No network, no provider calls. Used by run.mjs (finalize) and as a standalone post-processor:
//   node cache-proof.mjs            -> writes evidence/cache-proof.json from evidence/tap.jsonl etc.
import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const OPENAI_PROMPT_CACHE_THRESHOLD_TOKENS = 1024;
const SCENARIO_ID = "x.cache_usage_cost.openai";
const shortHash = (value) => createHash("sha256").update(String(value)).digest("hex").slice(0, 12);
const round = (value) => (typeof value === "number" ? Math.round(value * 1e9) / 1e9 : null);

// Raw tap record -> comparable row. Missing captures stay null; nothing is synthesized.
export function normalizeTapRecord(record) {
  const request = record.request ?? {};
  const response = record.response ?? {};
  return {
    seq: record.seq,
    run: record.run,
    generation: Boolean(record.generation),
    forwarded: Boolean(record.forwarded),
    status: response.status ?? null,
    store: request.store,
    instructions_sha12: request.instructions_sha12 ?? null,
    tools_sha12: request.tools_sha12 ?? null,
    input_item_count: request.input_item_count ?? null,
    input_leading_sha12: Array.isArray(request.input_leading_sha12) ? request.input_leading_sha12 : null,
    prompt_cache_key_present: request.prompt_cache_key_present ?? null,
    previous_response_id_present: Boolean(request.previous_response_id_present),
    previous_response_id_hash: request.previous_response_id_hash ?? null,
    provider_response_id_hash: response.provider_response_id ? shortHash(response.provider_response_id) : null,
    usage: response.usage ?? null,
    cclb_request_id: response.cclb_request_id ?? null,
  };
}

export function loadCatalogPrice(catalogPath, model) {
  if (!catalogPath || !existsSync(catalogPath)) return null;
  const catalog = JSON.parse(readFileSync(catalogPath, "utf8"));
  const entry = catalog[model];
  if (typeof entry?.input_cost_per_token !== "number" || typeof entry?.output_cost_per_token !== "number") return null;
  return {
    key: model,
    provider: entry.litellm_provider ?? null,
    input: entry.input_cost_per_token,
    output: entry.output_cost_per_token,
    cacheRead: typeof entry.cache_read_input_token_cost === "number" ? entry.cache_read_input_token_cost : null,
  };
}

// OpenAI usage: input_tokens includes cached_tokens; output_tokens includes reasoning tokens.
export function costArithmetic(usage, price) {
  if (!price || !usage || typeof usage.input_tokens !== "number" || typeof usage.output_tokens !== "number") return null;
  const cached = typeof usage.cached_tokens === "number" ? usage.cached_tokens : 0;
  const uncached = usage.input_tokens - cached;
  const cacheRate = price.cacheRead ?? price.input;
  const actual = uncached * price.input + cached * cacheRate + usage.output_tokens * price.output;
  const counterfactualNoCache = usage.input_tokens * price.input + usage.output_tokens * price.output;
  return {
    inputTokens: usage.input_tokens,
    cachedTokens: cached,
    uncachedInputTokens: uncached,
    outputTokens: usage.output_tokens,
    reasoningTokens: usage.reasoning_tokens ?? null,
    formula: `${uncached}*${price.input} + ${cached}*${cacheRate} + ${usage.output_tokens}*${price.output}`,
    actualUsd: round(actual),
    counterfactualNoCacheUsd: round(counterfactualNoCache),
    savedUsd: round(counterfactualNoCache - actual),
  };
}

function ccLbCost(event) {
  if (!event) return null;
  return {
    request_id: event.request_id,
    input_tokens_uncached: event.input_tokens ?? null,
    cache_read_input_tokens: event.cache_read_input_tokens ?? null,
    output_tokens: event.output_tokens ?? null,
    cost_usd: typeof event.cost_usd_micros === "number" ? event.cost_usd_micros / 1e6 : null,
    cost_input_usd: typeof event.cost_input_micros === "number" ? event.cost_input_micros / 1e6 : null,
    cost_cache_read_usd: typeof event.cost_cache_read_micros === "number" ? event.cost_cache_read_micros / 1e6 : null,
    cost_output_usd: typeof event.cost_output_micros === "number" ? event.cost_output_micros / 1e6 : null,
  };
}

function ccLbAgrees(row, event) {
  if (!row?.usage || !event) return null;
  const cached = row.usage.cached_tokens ?? 0;
  return event.cache_read_input_tokens === cached && event.input_tokens === row.usage.input_tokens - cached && event.output_tokens === row.usage.output_tokens;
}

const ok = (row) => row.generation && row.forwarded && row.status >= 200 && row.status < 300;
const scenarioOf = (row) => String(row.run ?? "").split("-")[0];
const sameInstructionsTools = (a, b) => Boolean(a.instructions_sha12 && a.instructions_sha12 === b.instructions_sha12 && a.tools_sha12 && a.tools_sha12 === b.tools_sha12);

function pairEntry({ anchor, later, basis, unchangedPrefix, rows, events, price, extra }) {
  const earlierSamePrefix = rows
    .filter((row) => ok(row) && row.seq < later.seq && row.seq !== anchor.seq && sameInstructionsTools(row, later))
    .map((row) => ({ seq: row.seq, run: row.run, cachedTokens: row.usage?.cached_tokens ?? null }));
  return {
    scenarioId: SCENARIO_ID,
    provider: "openai",
    thresholdTokens: OPENAI_PROMPT_CACHE_THRESHOLD_TOKENS,
    prefixTokensEstimate: anchor.usage?.input_tokens ?? null,
    unchangedPrefix,
    unchangedPrefixBasis: basis,
    runLabels: [anchor.run, later.run],
    wireSeqs: [anchor.seq, later.seq],
    anchorCachedTokens: anchor.usage?.cached_tokens ?? null,
    laterCachedTokens: later.usage?.cached_tokens ?? null,
    laterInputTokens: later.usage?.input_tokens ?? null,
    hit: typeof later.usage?.cached_tokens === "number" ? later.usage.cached_tokens > 0 : null,
    laterCost: costArithmetic(later.usage, price),
    laterCcLb: ccLbCost(events.get(later.cclb_request_id)),
    laterCcLbMatchesProviderUsage: ccLbAgrees(later, events.get(later.cclb_request_id)),
    // A same-prefix hit is reported; which earlier request warmed the provider cache is not observable.
    causality: "same_prefix_hit_only; provider does not report which earlier request populated the cache",
    earlierSamePrefixRequests: earlierSamePrefix,
    ref: `evidence/${later.run}.tap.json#seq=${later.seq}`,
    ...extra,
  };
}

export function buildCacheProof(rawRecords, { events = new Map(), price = null } = {}) {
  const rows = rawRecords.map(normalizeTapRecord).sort((a, b) => a.seq - b.seq);
  const entries = [];
  const notes = [];

  // First observed miss per scenario (cold prefix) with priced cost.
  const misses = [];
  for (const scenario of ["stateless", "stateful"]) {
    const first = rows.find((row) => ok(row) && scenarioOf(row) === scenario);
    if (!first) continue;
    misses.push({
      scenario,
      seq: first.seq,
      run: first.run,
      cachedTokens: first.usage?.cached_tokens ?? null,
      cost: costArithmetic(first.usage, price),
      ccLb: ccLbCost(events.get(first.cclb_request_id)),
      ccLbMatchesProviderUsage: ccLbAgrees(first, events.get(first.cclb_request_id)),
    });
  }

  // Stateless: turn1 last vs turn2 first; leading native input hash proof only if captured.
  const statelessT1 = rows.filter((row) => ok(row) && row.run === "stateless-t1");
  const statelessT2 = rows.filter((row) => ok(row) && row.run === "stateless-t2");
  if (statelessT1.length && statelessT2.length) {
    const anchor = statelessT1.at(-1);
    const later = statelessT2[0];
    const anchorHashes = anchor.input_leading_sha12;
    const laterHashes = later.input_leading_sha12;
    if (anchorHashes?.length && laterHashes?.length && anchorHashes.length === anchor.input_item_count) {
      const k = anchorHashes.length;
      const leadingMatch = laterHashes[k - 1] === anchorHashes[k - 1];
      entries.push(pairEntry({
        anchor, later, rows, events, price,
        basis: "native_input_leading_hash",
        unchangedPrefix: sameInstructionsTools(anchor, later) && leadingMatch,
        extra: { leadingInputItemsCompared: k, laterInputItems: later.input_item_count },
      }));
    } else {
      notes.push(`stateless seq${anchor.seq}->seq${later.seq}: leading input item hashes were not captured in this run's tap records; native_input_leading_hash cannot be derived (no raw input bodies saved). Only instructions/tools identity is available.`);
      entries.push(pairEntry({
        anchor, later, rows, events, price,
        basis: "instructions_tools_hash",
        unchangedPrefix: sameInstructionsTools(anchor, later),
        extra: { leadingInputItemsCompared: null, inputItemCounts: [anchor.input_item_count, later.input_item_count] },
      }));
    }
  }

  // Stateful: each chained request whose previous_response_id equals an observed earlier response id.
  for (const later of rows.filter((row) => ok(row) && scenarioOf(row) === "stateful" && row.previous_response_id_present)) {
    const anchor = rows.find((row) => row.seq < later.seq && row.provider_response_id_hash && row.provider_response_id_hash === later.previous_response_id_hash);
    if (!anchor) {
      notes.push(`stateful seq${later.seq}: previous_response_id does not match any observed earlier response`);
      continue;
    }
    entries.push(pairEntry({
      anchor, later, rows, events, price,
      basis: "previous_response_chain+instructions_tools_hash",
      unchangedPrefix: sameInstructionsTools(anchor, later),
      extra: { chainLink: { previousResponseIdHash: later.previous_response_id_hash, anchorResponseIdHash: anchor.provider_response_id_hash }, laterStore: later.store },
    }));
  }

  // Stateful requests without previous_response_id (e.g. first request of a resumed process) are full replays.
  const statefulReplays = rows
    .filter((row) => ok(row) && scenarioOf(row) === "stateful" && !row.previous_response_id_present)
    .map((row) => ({ seq: row.seq, run: row.run, store: row.store, inputItems: row.input_item_count, cachedTokens: row.usage?.cached_tokens ?? null }));

  return { thresholdTokens: OPENAI_PROMPT_CACHE_THRESHOLD_TOKENS, price, firstMisses: misses, entries, statefulFullReplays: statefulReplays, totals: costTotals(rows, events, price), notes };
}

// cc-lb prices each component in integer micros, truncated (observed: 3911*1.75=6844.25 -> 6844,
// 3456*0.175=604.8 -> 604). Predicted micros are derived with the same per-component floor so they can
// be compared exactly against committed rows; the real-rate USD is the untruncated product.
function predictedMicros(usage, price) {
  if (!price || !usage) return null;
  const cached = usage.cached_tokens ?? 0;
  const input = Math.floor(((usage.input_tokens - cached) * price.input * 1e6) + 1e-9);
  const cacheRead = Math.floor((cached * (price.cacheRead ?? price.input) * 1e6) + 1e-9);
  const output = Math.floor((usage.output_tokens * price.output * 1e6) + 1e-9);
  return { input, cacheRead, output, total: input + cacheRead + output };
}

function costTotals(rows, events, price) {
  const generations = rows.filter(ok);
  const perRequest = generations.map((row) => {
    const event = events.get(row.cclb_request_id) ?? null;
    const predicted = predictedMicros(row.usage, price);
    return {
      seq: row.seq,
      run: row.run,
      requestId: row.cclb_request_id,
      providerUsage: row.usage,
      realRateUsd: costArithmetic(row.usage, price)?.actualUsd ?? null,
      predictedFloorMicros: predicted,
      committedEventPresent: Boolean(event),
      committedCostMicros: event?.cost_usd_micros ?? null,
      committedMatchesPredicted: event ? event.cost_usd_micros === predicted?.total : null,
      committedTokensMatchProvider: ccLbAgrees(row, event),
    };
  });
  const sum = (values) => values.reduce((total, value) => total + value, 0);
  const present = perRequest.filter((item) => item.committedEventPresent);
  const missing = perRequest.filter((item) => !item.committedEventPresent).map((item) => item.requestId);
  return {
    generationRequests: perRequest.length,
    realRateUsdAll: round(sum(perRequest.map((item) => item.realRateUsd ?? 0))),
    predictedFloorMicrosAll: sum(perRequest.map((item) => item.predictedFloorMicros?.total ?? 0)),
    committedEventsPresent: present.length,
    committedCostMicrosPresent: sum(present.map((item) => item.committedCostMicros ?? 0)),
    missingCommittedEvents: missing,
    // Only claimed when every generation has a committed event that matches.
    allCommittedMatch: missing.length === 0 && present.every((item) => item.committedMatchesPredicted && item.committedTokensMatchProvider),
    perRequest,
  };
}

// ---------------------------------------------------------------------------
// Standalone post-processing over saved evidence (no network).
// ---------------------------------------------------------------------------
function readJsonl(path) {
  return readFileSync(path, "utf8").split("\n").filter((line) => line.trim()).map((line) => JSON.parse(line));
}

async function cli() {
  const sliceDir = dirname(fileURLToPath(import.meta.url));
  const evidenceDir = join(sliceDir, "evidence");
  const records = readJsonl(join(evidenceDir, "tap.jsonl"));
  const events = new Map();
  for (const name of readdirSync(evidenceDir).filter((file) => /^events-.*\.jsonl$/.test(file))) {
    for (const event of readJsonl(join(evidenceDir, name))) if (event.request_id) events.set(event.request_id, event);
  }
  const prepare = JSON.parse(readFileSync(join(sliceDir, "prepare.json"), "utf8"));
  const model = prepare.model ?? "gpt-5.2";
  const price = loadCatalogPrice(prepare.costCatalog?.path, model);
  const proof = buildCacheProof(records, { events, price });
  const tapRequestIds = records.filter((record) => record.generation && record.forwarded).map((record) => record.response?.cclb_request_id).filter(Boolean);
  proof.inputs = {
    tap: "evidence/tap.jsonl",
    events: [...readdirSync(evidenceDir).filter((file) => /^events-.*\.jsonl$/.test(file))].map((file) => `evidence/${file}`),
    catalog: prepare.costCatalog?.path ?? null,
    catalogSha256: prepare.costCatalog?.provenance?.sha256 ?? null,
    tapGenerationRequestIds: tapRequestIds.length,
    tapRequestIdsWithoutSavedEvent: tapRequestIds.filter((id) => !events.has(id)),
  };
  writeFileSync(join(evidenceDir, "cache-proof.json"), `${JSON.stringify(proof, null, 2)}\n`);
  console.log(JSON.stringify({
    written: "evidence/cache-proof.json",
    entries: proof.entries.map((entry) => ({ seqs: entry.wireSeqs, basis: entry.unchangedPrefixBasis, unchangedPrefix: entry.unchangedPrefix, cached: entry.laterCachedTokens, hit: entry.hit })),
    firstMisses: proof.firstMisses.map((miss) => ({ seq: miss.seq, cached: miss.cachedTokens, usd: miss.cost?.actualUsd })),
    missingEvents: proof.inputs.tapRequestIdsWithoutSavedEvent,
    totals: { ...proof.totals, perRequest: proof.totals.perRequest.map((item) => ({ seq: item.seq, id: item.requestId, realUsd: item.realRateUsd, predictedMicros: item.predictedFloorMicros?.total, committedMicros: item.committedCostMicros, match: item.committedMatchesPredicted })) },
    notes: proof.notes,
  }, null, 2));
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  cli().catch((error) => {
    console.error(error?.stack ?? error);
    process.exitCode = 1;
  });
}
