// Metadata-only, synchronous admission accounting. No provider or CLI operations.
// Catalog list prices are modeling metadata, not a provider billing guarantee.
import { createHash } from 'node:crypto';
import { constants, openSync, closeSync, fstatSync, lstatSync, readFileSync, writeSync, ftruncateSync, fsyncSync, realpathSync } from 'node:fs';
import path from 'node:path';

const CAMPAIGN = 30_000_000n;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const CASES = Object.freeze({
  'codex-http': ['codex-http-subscription', 12, 'openai-responses'],
  'codex-ws': ['codex-ws-subscription', 12, 'openai-responses'],
  'codex-ws-reconnect': ['codex-ws-reconnect-subscription', 6, 'openai-responses'],
  'omp-gemini': ['omp-gemini-subscription', 16, 'google-generate-content'],
  agy: ['agy-subscription', 16, 'google-generate-content'],
  'api-key-codex': ['codex', 32, 'openai-responses'],
  'api-key-omp-gpt': ['omp-gpt', 16, 'openai-responses'],
  'api-key-omp-gemini': ['omp-gemini', 16, 'google-generate-content'],
  'api-key-agy': ['agy', 16, 'google-generate-content'],
  'api-key-faults': ['faults', 16, 'mixed'],
  'api-key-native-http-cancel': ['native-http-cancel', 16, 'openai-responses'],
  'api-key-native-http-cancel-concurrent': ['native-http-cancel-concurrent', 16, 'openai-responses'],
  'api-key-gemini-accounting-control': ['gemini-accounting-control', 1, 'google-generate-content'],
  'api-key-anthropic-regression': ['anthropic-regression', 1, 'anthropic-messages'],
});
const TIERS = new Set(['default', 'priority', 'flex', 'batch', 'ultrafast']);
const SUBSCRIPTION_CODEX = new Set(['codex-http', 'codex-ws', 'codex-ws-reconnect']);
const PROVIDERS = Object.freeze({ 'openai-responses': 'openai', 'google-generate-content': 'gemini', 'anthropic-messages': 'anthropic' });
const GOOGLE_TOOL_INPUT_UNVERIFIED = 'google_tool_use_prompt_cost_dimension_unverified';
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const integer = (value) => Number.isSafeInteger(value) && value >= 0;
const positive = (value) => integer(value) && value > 0;
const hash = (value) => createHash('sha256').update(value).digest('hex');
class BudgetError extends Error {}
const fail = (reason) => { throw new BudgetError(reason); };
const check = (condition, reason) => { if (!condition) fail(reason); };
const number = (value) => { check(value >= 0n && value <= MAX_SAFE, 'cost_exceeds_safe_metadata_range'); return Number(value); };
// Breach evidence must remain conservative even outside safe-integer range.
// Such a snapshot intentionally fails the parent's safe-integer release gate.
function evidenceNumber(value) {
  check(value >= 0n, 'negative_cost_evidence');
  let converted = Number(value);
  check(Number.isFinite(converted), 'cost_evidence_unrepresentable');
  if (BigInt(converted) < value) converted = Math.ceil(converted * (1 + Number.EPSILON));
  return converted;
}
const safeName = (value) => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,191}$/.test(value);

// Parse the decimal representation directly; never multiply floating-point USD.
function decimal(value) {
  check((typeof value === 'number' && Number.isFinite(value) && value >= 0) || (typeof value === 'string' && value.length <= 160), 'invalid_catalog_rate');
  const match = /^(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(String(value));
  check(match, 'invalid_catalog_rate');
  const exponent = Number(match[3] ?? 0) - (match[2]?.length ?? 0);
  check(Number.isSafeInteger(exponent) && Math.abs(exponent) <= 400, 'unsupported_catalog_rate_precision');
  const n = BigInt(`${match[1]}${match[2] ?? ''}`);
  return exponent >= 0 ? { n: n * 10n ** BigInt(exponent), d: 1n } : { n, d: 10n ** BigInt(-exponent) };
}
function maximum(values) {
  check(values.length > 0, 'required_catalog_rate_missing');
  return values.reduce((a, b) => a.n * b.d >= b.n * a.d ? a : b);
}
function charge(components) {
  // Every decimal denominator is a power of ten, so the largest is a common
  // denominator. Round the entire charge once, not each cached partition.
  const denominator = components.reduce((d, [, rate]) => rate.d > d ? rate.d : d, 1n);
  const numerator = components.reduce((sum, [tokens, rate]) => sum + BigInt(tokens) * rate.n * (denominator / rate.d), 0n) * 1_000_000n;
  return (numerator + denominator - 1n) / denominator;
}
function readRegular(file) {
  const fd = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = fstatSync(fd);
    check(stat.isFile() && stat.size <= 1_048_576, 'invalid_budget_file');
    return JSON.parse(readFileSync(fd, 'utf8'));
  } finally { closeSync(fd); }
}
function absent(file) {
  try { lstatSync(file); return false; } catch (error) { if (error?.code === 'ENOENT') return true; throw error; }
}

function catalogRates(entry) {
  const result = new Map();
  const prefixes = [
    ['input_cost_per_token', 'input'], ['output_cost_per_token', 'output'],
    ['cache_read_input_token_cost', 'cache_read'],
    ['cache_creation_input_token_cost_above_1hr', 'cache_write_1h'],
    ['cache_creation_input_token_cost_above_12hr', 'cache_write_1h'],
    ['cache_creation_input_token_cost', 'cache_write'],
  ];
  for (const [key, value] of Object.entries(entry)) {
    // Even non-text rates must never contain an invalid per-token price.
    if (/cost_per_.*token|token_cost/.test(key)) decimal(value);
    const prefix = prefixes.find(([name]) => key === name || key.startsWith(`${name}_`));
    if (!prefix) continue;
    let suffix = key.slice(prefix[0].length);
    suffix = suffix.replace(/_above_\d+(?:\.\d+)?hr/g, '').replace(/_above_\d+(?:\.\d+)?[km]?_tokens/g, '');
    const tier = suffix === '' ? 'default' : suffix === '_priority' ? 'priority' : suffix === '_flex' ? 'flex' : /^_batch(?:es)?$/.test(suffix) ? 'batch' : suffix === '_ultrafast' ? 'ultrafast' : null;
    check(tier !== null, 'unrecognized_catalog_text_rate_variant');
    const byTier = result.get(tier) ?? { input: [], output: [], cache_read: [], cache_write: [], cache_write_1h: [] };
    byTier[prefix[1]].push(decimal(value));
    result.set(tier, byTier);
  }
  return result;
}
function selection(info, tiers) {
  const values = { input: [], output: [], cache_read: [], cache_write: [], cache_write_1h: [] };
  let everyRead = true, everyWrite = true, everyHourWrite = true;
  for (const tier of tiers) {
    const tierRates = info.rates.get(tier);
    check(tierRates?.input.length > 0 && tierRates?.output.length > 0, 'eligible_tier_exact_rates_missing');
    for (const component of Object.keys(values)) values[component].push(...tierRates[component]);
    everyRead &&= tierRates.cache_read.length > 0;
    everyWrite &&= tierRates.cache_write.length > 0;
    everyHourWrite &&= tierRates.cache_write_1h.length > 0;
  }
  return {
    input: maximum(values.input), output: maximum(values.output),
    cache_read: everyRead ? maximum(values.cache_read) : null,
    cache_write: everyWrite ? maximum(values.cache_write) : null,
    cache_write_1h: everyHourWrite ? maximum(values.cache_write_1h) : null,
    // Cache writes may cost more than ordinary input; reserve that known maximum.
    full_input: maximum([...values.input, ...values.cache_read, ...values.cache_write, ...values.cache_write_1h]),
  };
}
function fullCost(info, rates, output) {
  return charge([[info.input, rates.full_input], [output, rates.output]]);
}

function nativeChildSemantic(protocol, semantic, key, parent, child) {
  if (semantic === 'json-data') return 'json-data';
  if (semantic === 'google-wrapper') return key === 'request' ? 'native-body' : 'generic';
  if (semantic === 'native-body') {
    if (key === 'tools') return 'tools-array';
    if (protocol === 'openai-responses' && key === 'input' && Array.isArray(child)) return 'openai-input-array';
    if (protocol === 'google-generate-content' && key === 'contents') return 'google-contents-array';
    if (protocol === 'anthropic-messages' && key === 'messages') return 'anthropic-messages-array';
  }
  if (semantic === 'tools-array') return protocol === 'openai-responses' ? 'openai-tool' : protocol === 'google-generate-content' ? 'google-tool' : 'anthropic-tool';
  if (semantic === 'openai-tool') {
    if (key === 'parameters' || key === 'input_schema') return 'json-data';
    if (parent.type === 'namespace' && key === 'tools') return 'tools-array';
    if ((parent.type === 'function' || parent.type === 'custom') && key === 'tools') return 'json-data';
    if (parent.type === 'function' && key === 'function') return 'openai-function-definition';
  }
  if (semantic === 'openai-function-definition' && (key === 'parameters' || key === 'input_schema')) return 'json-data';
  if (semantic === 'google-tool' && (key === 'functionDeclarations' || key === 'function_declarations')) return 'google-function-declarations-array';
  if (semantic === 'google-function-declarations-array') return 'google-function-declaration';
  if (semantic === 'google-function-declaration' && (key === 'parameters' || key === 'parametersJsonSchema' || key === 'parameters_json_schema' || key === 'response' || key === 'responseJsonSchema' || key === 'response_json_schema')) return 'json-data';
  if (semantic === 'anthropic-tool' && key === 'input_schema') return 'json-data';
  if (semantic === 'openai-input-array') return 'openai-input-item';
  if (semantic === 'openai-input-item') {
    if (parent.type === 'additional_tools' && key === 'tools') return 'tools-array';
    if (parent.type === 'function_call' && key === 'arguments') return 'json-data';
    if (parent.type === 'custom_tool_call' && key === 'input') return 'json-data';
    if ((parent.type === 'function_call_output' || parent.type === 'custom_tool_call_output') && key === 'output' && object(child)) return 'json-data';
  }
  if (semantic === 'google-contents-array') return 'google-content';
  if (semantic === 'google-content' && key === 'parts') return 'google-parts-array';
  if (semantic === 'google-parts-array') return 'google-part';
  if (semantic === 'google-part') {
    if (key === 'functionCall' || key === 'function_call') return 'google-function-call';
    if (key === 'functionResponse' || key === 'function_response') return 'google-function-response';
  }
  if (semantic === 'google-function-call' && key === 'args') return 'json-data';
  if (semantic === 'google-function-response' && key === 'response') return 'json-data';
  if (semantic === 'anthropic-messages-array') return 'anthropic-message';
  if (semantic === 'anthropic-message' && key === 'content' && Array.isArray(child)) return 'anthropic-content-array';
  if (semantic === 'anthropic-content-array') return 'anthropic-content-block';
  if (semantic === 'anthropic-content-block') {
    if (parent.type === 'tool_use' && key === 'input') return 'json-data';
    if (parent.type === 'tool_result' && key === 'content' && object(child)) return 'json-data';
  }
  return 'generic';
}
function parseBody(raw, protocol) {
  let body = raw;
  if (typeof body === 'string' || Buffer.isBuffer(body) || body instanceof Uint8Array) {
    try { body = JSON.parse(typeof body === 'string' ? body : Buffer.from(body).toString('utf8')); }
    catch { fail('invalid_native_json_body'); }
  }
  check(object(body), 'native_text_object_required');
  check(own(PROVIDERS, protocol), 'unsupported_native_protocol');
  // Tool names never classify fees. Check actual native tool definitions before
  // inspecting content types so hosted file/image tools get a pricing block.
  const toolBody = protocol === 'google-generate-content' && object(body.request) ? body.request : body;
  assertTokenOnlyTools(toolBody, protocol);
  const active = new Set();
  const cacheTtls = new Set();
  let nodes = 0;
  function inspect(value, depth, semantic) {
    check(++nodes <= 100_000 && depth <= 80, 'native_body_structure_unsupported');
    if (value === null || typeof value !== 'object') return;
    check(!active.has(value), 'cyclic_native_body');
    active.add(value);
    for (const [key, child] of Object.entries(value)) {
      if (semantic !== 'json-data') {
        if (key === 'cache_control') {
          check(protocol === 'anthropic-messages' && object(child) && child.type === 'ephemeral' && Object.keys(child).every((name) => name === 'type' || name === 'ttl'), 'explicit_cache_creation_or_control_unsupported');
          const ttl = child.ttl ?? '5m';
          check(ttl === '5m' || ttl === '1h', 'unknown_cache_write_ttl');
          cacheTtls.add(ttl);
        } else {
          check(!/^(?:cacheControl|cache_creation|cacheCreation|cachedContent|cached_content|cache_creation_input_tokens)$/.test(key), 'explicit_cache_creation_or_control_unsupported');
        }
        check(!/^(?:inlineData|inline_data|fileData|file_data|image_url|input_image|input_file|input_audio|audio|audio_url|video|images|imageConfig|file_id|file_uri|fileUri|videoMetadata|video_metadata|mediaResolution)$/.test(key), 'multimodal_native_request_unsupported');
        if (key === 'type' && typeof child === 'string') check(!/(?:image|audio|video|file|computer)/i.test(child), 'multimodal_native_request_unsupported');
        if (key === 'responseModalities' || key === 'response_modalities' || key === 'modalities') check(Array.isArray(child) && child.every((item) => item === 'TEXT' || item === 'text'), 'multimodal_native_request_unsupported');
      }
      inspect(child, depth + 1, nativeChildSemantic(protocol, semantic, key, value, child));
    }
    active.delete(value);
  }
  inspect(body, 0, protocol === 'google-generate-content' && object(body.request) ? 'google-wrapper' : 'native-body');
  return { body, cache_ttls: [...cacheTtls] };
}
function assertTokenOnlyTools(body, protocol) {
  const reason = 'provider_hosted_tool_fee_contract_required';
  if (protocol === 'openai-responses') {
    // Pinned native ToolSpec namespace containers recurse into direct tools.
    // Guard this pre-inspection traversal independently: no stack/cycle bypass.
    let nodes = 0;
    const active = new Set();
    const visit = (tool, depth) => {
      check(++nodes <= 100_000 && depth <= 80 && object(tool) && own(tool, 'type'), reason);
      check(!active.has(tool), reason);
      active.add(tool);
      if (tool.type === 'function' || tool.type === 'custom') {
        check(own(tool, 'name') && typeof tool.name === 'string' && tool.name.length > 0, reason);
      } else if (tool.type === 'namespace') {
        check(own(tool, 'name') && typeof tool.name === 'string' && tool.name.length > 0 && own(tool, 'tools') && Array.isArray(tool.tools), reason);
        for (const child of tool.tools) visit(child, depth + 1);
      } else fail(reason);
      active.delete(tool);
    };
    const visitTools = (tools) => {
      check(Array.isArray(tools), reason);
      for (const tool of tools) visit(tool, 0);
    };
    if (own(body, 'tools')) visitTools(body.tools);
    // Exactly direct input items are native ResponseItem::AdditionalTools.
    // Only the observed developer projection is supported; other roles block.
    if (Array.isArray(body.input)) {
      for (const item of body.input) {
        check(++nodes <= 100_000, reason);
        if (object(item) && own(item, 'type') && item.type === 'additional_tools') {
          check(own(item, 'role') && item.role === 'developer' && own(item, 'tools'), reason);
          visitTools(item.tools);
        }
      }
    }
    if (object(body.tool_choice)) {
      if (own(body.tool_choice, 'type') && body.tool_choice.type === 'allowed_tools') visitTools(body.tool_choice.tools);
      else visit(body.tool_choice, 0);
    }
  } else if (protocol === 'google-generate-content' && own(body, 'tools')) {
    check(Array.isArray(body.tools), reason);
    for (const tool of body.tools) {
      check(object(tool) && Object.keys(tool).length > 0 && Object.entries(tool).every(([key, declarations]) =>
        (key === 'functionDeclarations' || key === 'function_declarations') && Array.isArray(declarations) && declarations.every(object)), reason);
    }
  } else if (protocol === 'anthropic-messages' && own(body, 'tools')) {
    // Native custom tools have no type (or explicit custom). Server-tool type
    // names need their own verified fee and maximum-call contract.
    check(Array.isArray(body.tools) && body.tools.every((tool) => object(tool) &&
      (tool.type === undefined || tool.type === 'custom') && typeof tool.name === 'string' && object(tool.input_schema)), reason);
  }
}
function prewarmRates(rates) {
  // An explicit documented no-separate-write-fee contract permits ordinary
  // input pricing for otherwise unknown cache writes. Preserve every higher
  // catalog-provided write rate; no input/cache token is assumed free.
  const cacheWrite = maximum([rates.input, ...[rates.cache_write, rates.cache_write_1h].filter((rate) => rate !== null)]);
  return { ...rates, cache_write: cacheWrite, full_input: maximum([rates.full_input, cacheWrite]) };
}
function requestSettings(raw, model, info, transport) {
  let parsed;
  const steer = transport === 'responses-ws-steer';
  if (transport === 'responses-ws' || steer) {
    check(info.protocol === 'openai-responses', 'responses_ws_frame_not_admissible');
    try { parsed = parseBody(raw, info.protocol); }
    catch (error) {
      if (error instanceof BudgetError && error.message === 'provider_hosted_tool_fee_contract_required') throw error;
      fail('responses_ws_frame_not_admissible');
    }
    check(parsed.body.type === (steer ? 'response.steer' : 'response.create'), 'responses_ws_frame_not_admissible');
  } else parsed = parseBody(raw, info.protocol);
  const { body, cache_ttls } = parsed;
  const google = info.protocol === 'google-generate-content';
  // Only protocol-effective fields may lower a hold. An ignored nested field
  // or a field belonging to another provider must never lower the maximum.
  let effective = body;
  if (google && own(body, 'request')) {
    check(object(body.request), 'invalid_native_request_wrapper');
    effective = body.request;
  }
  if (own(body, 'model')) check(body.model === model, 'wire_model_mismatch');
  if (own(effective, 'model')) check(effective.model === model, 'wire_model_mismatch');
  if (info.protocol === 'openai-responses') check(!own(effective, 'prompt_cache_options'), 'prompt_cache_options_pricing_not_supported');
  let tier;
  let output = info.output;
  let input = info.input;
  check(!own(effective, 'serviceTier'), 'unsupported_wire_service_tier_field');
  if (google) check(!own(effective, 'service_tier'), 'unsupported_wire_service_tier_field');
  if (!google && own(effective, 'service_tier')) {
    check(typeof effective.service_tier === 'string', 'ambiguous_wire_service_tier');
    tier = effective.service_tier;
  }
  if (info.protocol === 'openai-responses' && !steer && own(effective, 'max_output_tokens')) {
    check(positive(effective.max_output_tokens), 'invalid_explicit_output_bound');
    output = Math.min(output, effective.max_output_tokens);
  } else if (info.protocol === 'openai-responses' && !own(effective, 'max_output_tokens')) {
    input = Math.min(input, 65536);
    output = Math.min(output, 16384);
  }
  if (google && own(effective, 'generationConfig')) {
    check(object(effective.generationConfig), 'invalid_generation_config');
    if (own(effective.generationConfig, 'candidateCount')) check(effective.generationConfig.candidateCount === 1, 'multiple_candidates_unsupported');
    if (own(effective.generationConfig, 'maxOutputTokens')) {
      check(positive(effective.generationConfig.maxOutputTokens), 'invalid_explicit_output_bound');
      output = Math.min(output, effective.generationConfig.maxOutputTokens);
    }
  }
  if (info.protocol === 'anthropic-messages' && own(effective, 'max_tokens')) {
    check(positive(effective.max_tokens), 'invalid_explicit_output_bound');
    output = Math.min(output, effective.max_tokens);
  }
  if (tier === undefined && !steer) {
    check(info.omitted !== undefined, 'documented_omitted_tier_mapping_required');
    tier = info.omitted;
  }
  const requestedTiers = tier === undefined ? [] : tier === 'auto' ? info.tiers : [tier];
  check(requestedTiers.every((item) => info.tiers.includes(item)), 'wire_service_tier_not_eligible');
  // A steer inherits the prior response's tier. Neither an omission mapping
  // nor an explicit lower-tier field proves that inherited exposure is lower.
  // Reserve every cited eligible tier, never unrelated catalog SKUs.
  const tiers = steer ? [...info.tiers] : requestedTiers;
  const prewarm = transport === 'responses-ws' && body.type === 'response.create' && body.generate === false;
  let rates = selection(info, tiers);
  if (prewarm) {
    if (own(effective, 'prompt_cache_retention')) {
      check(info.prewarm_policy !== null && typeof effective.prompt_cache_retention === 'string' && info.prewarm_policy.supported_prompt_cache_retention.includes(effective.prompt_cache_retention), 'prewarm_cache_retention_not_documented');
    }
    if (info.prewarm_policy !== null) {
      rates = prewarmRates(rates);
      output = 0;
    }
  }
  const reservationInfo = input === info.input ? info : { ...info, input };
  return { output, tiers, rates, cache_ttls, prewarm, prewarm_policy_citation_sha256: prewarm && info.prewarm_policy !== null ? [...info.prewarm_policy.citation_hashes] : [], bound: fullCost(reservationInfo, rates, output) };
}
function usageCounts(usage, format, protocol, cacheTtls = []) {
  check(object(usage), 'real_usage_missing');
  let input, output, cached = 0, written = 0, reasoning = 0;
  let cacheSplit = null;
  if (protocol === 'openai-responses') {
    check(format === 'openai' || format === 'openai-responses', 'usage_format_mismatch');
    input = usage.input_tokens;
    output = usage.output_tokens;
    if (own(usage, 'input_tokens_details')) {
      check(object(usage.input_tokens_details), 'invalid_usage');
      cached = usage.input_tokens_details.cached_tokens ?? 0;
    }
    if (own(usage, 'cached_tokens')) {
      check(!own(usage.input_tokens_details ?? {}, 'cached_tokens') || usage.input_tokens_details.cached_tokens === usage.cached_tokens, 'conflicting_usage_aliases');
      cached = usage.cached_tokens;
    }
    written = usage.cache_creation_input_tokens ?? 0;
    if (own(usage, 'output_tokens_details')) {
      check(object(usage.output_tokens_details), 'invalid_usage');
      reasoning = usage.output_tokens_details.reasoning_tokens ?? 0;
    }
    if (own(usage, 'reasoning_tokens')) {
      check(!own(usage.output_tokens_details ?? {}, 'reasoning_tokens') || usage.output_tokens_details.reasoning_tokens === usage.reasoning_tokens, 'conflicting_usage_aliases');
      reasoning = usage.reasoning_tokens;
    }
    // OpenAI output_tokens already includes reasoning_tokens.
  } else if (protocol === 'anthropic-messages') {
    check(format === 'anthropic' || format === 'anthropic-messages', 'usage_format_mismatch');
    const uncached = usage.input_tokens;
    output = usage.output_tokens;
    cached = usage.cache_read_input_tokens ?? 0;
    let five = 0, hour = 0;
    let ttlSource = 'none';
    if (own(usage, 'cache_creation')) {
      check(object(usage.cache_creation) && Object.keys(usage.cache_creation).every((key) => key === 'ephemeral_5m_input_tokens' || key === 'ephemeral_1h_input_tokens'), 'unknown_cache_write_ttl');
      five = usage.cache_creation.ephemeral_5m_input_tokens ?? 0;
      hour = usage.cache_creation.ephemeral_1h_input_tokens ?? 0;
      check(integer(five) && integer(hour) && Number.isSafeInteger(five + hour), 'invalid_usage');
      written = five + hour;
      check(!own(usage, 'cache_creation_input_tokens') || usage.cache_creation_input_tokens === written, 'inconsistent_cache_creation_usage');
      ttlSource = written > 0 ? 'observed_usage' : 'none';
    } else {
      written = usage.cache_creation_input_tokens ?? 0;
      check(integer(written), 'invalid_usage');
      if (written > 0) {
        check(cacheTtls.length === 1, 'cache_write_usage_ttl_unknown');
        if (cacheTtls[0] === '5m') five = written;
        else if (cacheTtls[0] === '1h') hour = written;
        else fail('unknown_cache_write_ttl');
        ttlSource = 'single_explicit_request_ttl';
      }
    }
    check(integer(uncached) && integer(cached) && integer(written) && Number.isSafeInteger(uncached + cached + written), 'invalid_usage');
    // Messages input_tokens excludes BOTH cache partitions. Keep the complete
    // input for hard-bound enforcement; each price partition is charged once.
    input = uncached + cached + written;
    cacheSplit = { uncached_input_tokens: uncached, cache_creation_input_tokens_5m: five, cache_creation_input_tokens_1h: hour, cache_creation_ttl_source: ttlSource };
  } else {
    check(format === 'gemini' || format === 'google-generate-content', 'usage_format_mismatch');
    input = usage.promptTokenCount;
    const candidates = usage.candidatesTokenCount;
    reasoning = usage.thoughtsTokenCount ?? 0;
    check(integer(candidates) && integer(reasoning) && Number.isSafeInteger(candidates + reasoning), 'invalid_usage');
    output = candidates + reasoning;
    cached = usage.cachedContentTokenCount ?? 0;
    written = usage.cacheCreationTokenCount ?? 0;
    // Google promptTokenCount includes cachedContentTokenCount.
  }
  check([input, output, cached, written, reasoning].every(integer), 'invalid_usage');
  check(cached <= input && written <= input - cached && reasoning <= output, 'inconsistent_usage');
  return { input_tokens: input, output_tokens: output, cache_read_input_tokens: cached, cache_creation_input_tokens: written, reasoning_tokens: reasoning, ...cacheSplit };
}

/**
 * pricingEligibility[model] = { protocol, eligible_tiers, omitted_tier?, citations }.
 * Optional responses_ws_prewarm requires generate_false_output_zero:true,
 * cache_write:'included_in_input', exact eligible_models, citations, and optional
 * supported_prompt_cache_retention values ('in_memory' or '24h').
 * plan.caps.slice_usd must explicitly select 3 or 6 for this fresh slice.
 * The captured cap never changes or reinterprets an existing slice's metadata.
 * Only sourced, parent-validated tier eligibility is accepted. omitted_tier may
 * be 'auto' only when the protocol's documented omission behavior establishes it.
 * Factory/preflight perform reads only. The first admitted reserve or explicit
 * seal exclusively creates the already-ledger-owned outbound-budget.json.
 * Seal closes future admission without releasing any existing held cost.
 */
export function createOutboundBudgetGuard({ ctx, plan, catalog, models, campaignBudgetBefore, pricingEligibility } = {}) {
  const records = new Map();
  const infos = new Map();
  let blocked = null;
  let breach = false;
  let unsupportedCostDimension = false;
  let sealed = false;
  let sealPersisted = false;
  let sealAttempted = false;
  let sealFailure = null;
  let file;
  let identity;
  let lastDigest;
  let campaignBefore = 0n;
  let sliceCap = null;
  let caseInfo;
  let codex = false;
  let catalogDigest = null;

  try {
    check(object(ctx) && object(plan), 'guard_context_required');
    caseInfo = CASES[plan.case];
    check(caseInfo && plan.name === caseInfo[0], 'unknown_or_mismatched_case');
    check(plan.caps?.case_forwards === caseInfo[1], 'original_case_forward_cap_required');
    check(plan.caps?.slice_usd === 3 || plan.caps?.slice_usd === 6, 'explicit_supported_slice_usd_cap_required');
    sliceCap = BigInt(plan.caps.slice_usd) * 1_000_000n;
    check(typeof ctx.root === 'string' && typeof plan.epoch === 'string', 'owned_paths_required');
    const root = realpathSync(ctx.root);
    const epoch = realpathSync(plan.epoch);
    check(path.dirname(root) === epoch && path.basename(root) === plan.name, 'slice_outside_owned_epoch');
    file = path.join(root, 'outbound-budget.json');
    check(absent(file), 'fresh_outbound_budget_required');
    check(object(campaignBudgetBefore) && integer(campaignBudgetBefore.conservativePriorMicros) && integer(campaignBudgetBefore.outstandingMaxOutboundMicros), 'verified_campaign_budget_required');
    campaignBefore = BigInt(campaignBudgetBefore.conservativePriorMicros) + BigInt(campaignBudgetBefore.outstandingMaxOutboundMicros);
    check(campaignBefore <= CAMPAIGN, 'campaign_ceiling_exceeded');
    check(object(catalog) && Array.isArray(models) && models.length > 0 && new Set(models).size === models.length, 'exact_native_models_required');
    check(object(pricingEligibility), 'documented_pricing_eligibility_required');
    codex = SUBSCRIPTION_CODEX.has(plan.case);
    for (const model of models) {
      check(safeName(model), 'invalid_native_model_metadata');
      const key = ctx.costCatalog?.prices?.[model]?.key;
      check(safeName(key) && own(catalog, key) && object(catalog[key]), 'exact_canonical_native_catalog_entry_required');
      const entry = catalog[key];
      const eligibility = own(pricingEligibility, model) ? pricingEligibility[model] : null;
      check(object(eligibility), 'documented_protocol_eligibility_required');
      const protocol = caseInfo[2] === 'mixed' ? eligibility.protocol : caseInfo[2];
      check(own(PROVIDERS, protocol) && eligibility.protocol === protocol && (caseInfo[2] !== 'mixed' || protocol === 'openai-responses' || protocol === 'google-generate-content'), 'documented_protocol_eligibility_required');
      check(entry.litellm_provider === PROVIDERS[protocol], 'non_native_catalog_entry');
      check(positive(entry.max_input_tokens) && positive(entry.max_output_tokens), 'catalog_hard_token_bounds_required');
      check(Array.isArray(eligibility.eligible_tiers) && eligibility.eligible_tiers.length > 0 && eligibility.eligible_tiers.every((tier) => TIERS.has(tier)) && new Set(eligibility.eligible_tiers).size === eligibility.eligible_tiers.length, 'documented_eligible_tiers_required');
      check(Array.isArray(eligibility.citations) && eligibility.citations.length > 0 && eligibility.citations.every((citation) => typeof citation === 'string' && citation.length > 0 && citation.length <= 4096), 'pricing_eligibility_citations_required');
      check(eligibility.omitted_tier === undefined || eligibility.omitted_tier === 'auto' || eligibility.eligible_tiers.includes(eligibility.omitted_tier), 'invalid_omitted_tier_mapping');
      let prewarmPolicy = null;
      if (own(eligibility, 'responses_ws_prewarm')) {
        const policy = eligibility.responses_ws_prewarm;
        check(protocol === 'openai-responses', 'prewarm_policy_requires_openai_responses');
        check(object(policy) && Object.keys(policy).every((name) => ['generate_false_output_zero', 'cache_write', 'eligible_models', 'citations', 'supported_prompt_cache_retention'].includes(name)) && policy.generate_false_output_zero === true && policy.cache_write === 'included_in_input', 'documented_responses_ws_prewarm_policy_required');
        check(Array.isArray(policy.eligible_models) && policy.eligible_models.length > 0 && policy.eligible_models.every(safeName) && new Set(policy.eligible_models).size === policy.eligible_models.length && policy.eligible_models.includes(model), 'prewarm_policy_model_not_eligible');
        check(Array.isArray(policy.citations) && policy.citations.length > 0 && policy.citations.every((citation) => typeof citation === 'string' && citation.length > 0 && citation.length <= 4096), 'prewarm_policy_citations_required');
        const retention = policy.supported_prompt_cache_retention ?? [];
        check(Array.isArray(retention) && retention.every((value) => value === 'in_memory' || value === '24h') && new Set(retention).size === retention.length, 'unsupported_prewarm_cache_retention_policy');
        prewarmPolicy = { citation_hashes: policy.citations.map(hash), supported_prompt_cache_retention: [...retention] };
      }
      const info = { model, key, input: entry.max_input_tokens, output: entry.max_output_tokens, rates: catalogRates(entry), protocol, prewarm_policy: prewarmPolicy, tiers: [...eligibility.eligible_tiers], omitted: eligibility.omitted_tier, citation_hashes: eligibility.citations.map(hash) };
      const allRates = selection(info, info.tiers);
      info.full_bound = fullCost(info, allRates, info.output);
      number(info.full_bound);
      infos.set(model, info);
    }
    catalogDigest = hash(JSON.stringify([...infos.values()].map((info) => ({ model: info.model, key: info.key, entry: catalog[info.key] }))));
  } catch (error) {
    blocked = knownReason(error, 'guard_setup_invalid');
  }

  function knownReason(error, fallback) {
    // Only our fixed identifiers may reach metadata; OS/parser messages can
    // include file paths, caller values, payload excerpts, or credentials.
    return error instanceof BudgetError ? error.message : fallback;
  }
  function totals() {
    let spent = 0n, inFlight = 0n, unknown = 0n;
    for (const record of records.values()) {
      if (record.status === 'SETTLED') spent += record.charged;
      else if (record.status === 'IN_FLIGHT') inFlight += record.bound;
      else unknown += record.charged > record.bound ? record.charged : record.bound;
    }
    return { spent, inFlight, unknown, total: spent + inFlight + unknown };
  }
  function snapshot() {
    const total = totals();
    return {
      schema: 'outbound-budget-v1', case: caseInfo ? plan.case : null,
      slice_usd_cap_micros: sliceCap === null ? null : number(sliceCap), campaign_usd_cap_micros: number(CAMPAIGN),
      case_forward_cap: caseInfo?.[1] ?? null, codex_aggregate_forward_cap: 32,
      attempts: records.size, spent_micros: evidenceNumber(total.spent),
      in_flight_hold_micros: evidenceNumber(total.inFlight), unknown_hold_micros: evidenceNumber(total.unknown),
      total_held_micros: evidenceNumber(total.total), campaign_before_micros: evidenceNumber(campaignBefore),
      unsafe_integer_cost_evidence: total.total > MAX_SAFE,
      total_held_micros_exact: total.total.toString(),
      unresolved: [...records.values()].some((record) => record.status !== 'SETTLED'), bound_breach: breach,
      sealed, seal_persisted: sealPersisted,
      unsupported_cost_dimension: unsupportedCostDimension,
      blocked: blocked !== null, reason: blocked, catalog_sha256: catalogDigest,
      pricing_basis: 'catalog_list_price_model_not_provider_guarantee',
      real_paid_funding_or_invoice_state: 'UNVERIFIED',
      actual_subscription_quota_or_credit_state: 'UNVERIFIED',
      records: [...records.values()].map((record) => ({
        id_sha256: record.id_hash, source_sha256: record.source_hash,
        model: record.info.model, catalog_key: record.info.key,
        protocol: record.info.protocol, eligible_tiers: [...record.info.tiers],
        transport: record.transport,
        prewarm: record.prewarm, effective_output_bound_tokens: record.output,
        prewarm_policy_citation_sha256: [...record.prewarm_policy_citation_sha256],
        cache_write_request_ttls: [...record.cache_ttls],
        unsupported_cost_dimension: record.unsupported_cost_dimension,
        tool_use_prompt_token_count_observed: record.tool_use_prompt_token_count_observed,
        selected_tiers: [...record.tiers], eligibility_citation_sha256: [...record.info.citation_hashes],
        observed_response_tier: record.response_tier ?? null,
        response_tier_matches_selected: record.response_tier === undefined ? null : record.tiers.includes(record.response_tier),
        status: record.status, reserved_bound_micros: number(record.bound),
        charged_micros: evidenceNumber(record.charged), max_input_tokens: record.info.input,
        max_output_tokens: record.output, catalog_max_output_tokens: record.info.output,
        usage: record.usage ? { ...record.usage } : null, provider_status: record.provider_status,
        reason: record.reason, bound_breach: record.breach,
        non_generation_proof_sha256: record.proof_hash ?? null,
      })),
    };
  }
  function verifyOwn() {
    if (!identity) { check(absent(file), 'fresh_outbound_budget_required'); return; }
    const stat = lstatSync(file);
    check(stat.isFile() && !stat.isSymbolicLink() && stat.dev === identity.dev && stat.ino === identity.ino, 'owned_budget_file_changed');
    check(hash(readFileSync(file)) === lastDigest, 'owned_budget_metadata_changed');
  }
  function persist() {
    verifyOwn();
    const bytes = Buffer.from(`${JSON.stringify(snapshot(), null, 2)}\n`);
    const fd = openSync(file, identity ? constants.O_WRONLY | constants.O_NOFOLLOW : constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    try {
      const stat = fstatSync(fd);
      check(stat.isFile() && (!identity || stat.dev === identity.dev && stat.ino === identity.ino), 'owned_budget_file_changed');
      identity = { dev: stat.dev, ino: stat.ino };
      ftruncateSync(fd, 0);
      let offset = 0;
      while (offset < bytes.length) {
        const count = writeSync(fd, bytes, offset, bytes.length - offset, offset);
        check(count > 0, 'budget_metadata_write_failed');
        offset += count;
      }
      fsyncSync(fd);
      lastDigest = hash(bytes);
      const directoryFd = openSync(path.dirname(file), constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
      try { fsyncSync(directoryFd); } finally { closeSync(directoryFd); }
    } finally { closeSync(fd); }
  }
  function codexAttempts() {
    if (!codex) return 0;
    let count = records.size;
    for (const [otherCase, [slice, ceiling]] of Object.entries(CASES)) {
      if (!SUBSCRIPTION_CODEX.has(otherCase) || otherCase === plan.case) continue;
      const other = path.join(path.dirname(file), '..', slice, 'outbound-budget.json');
      let budget;
      try { budget = readRegular(other); }
      catch (error) { if (error?.code === 'ENOENT') continue; fail('other_codex_budget_unreadable'); }
      check(object(budget) && budget.schema === 'outbound-budget-v1' && budget.case === otherCase && integer(budget.attempts) && budget.attempts <= ceiling && Array.isArray(budget.records) && budget.records.length === budget.attempts, 'other_codex_budget_invalid');
      check(budget.slice_usd_cap_micros === 3_000_000 || budget.slice_usd_cap_micros === 6_000_000, 'other_codex_budget_slice_cap_invalid');
      count += budget.attempts;
    }
    check(count <= 32, 'codex_aggregate_forward_cap_exceeded');
    return count;
  }
  function preflight() {
    if (blocked) return { ok: false, state: 'SETUP_BLOCKED', reason: blocked, ...snapshot() };
    try {
      verifyOwn();
      const aggregate = codexAttempts();
      return {
        ...snapshot(), ok: true, state: 'READY_FOR_WIRE_RESERVATION', reason: null,
        codex_aggregate_attempts: codex ? aggregate : null,
        models: [...infos.values()].map((info) => ({ model: info.model, catalog_key: info.key, full_catalog_bound_micros: number(info.full_bound), full_catalog_bound_exceeds_slice: info.full_bound > sliceCap, explicit_native_output_cap_may_be_required: info.full_bound > sliceCap })),
        admission_requires_actual_wire_body: true,
        concurrency_contract: 'parent_serializes_campaign_dispatch_and_reconciles_other_outstanding_maxima',
      };
    } catch (error) {
      blocked = knownReason(error, 'budget_preflight_failed');
      return { ...snapshot(), ok: false, state: 'SETUP_BLOCKED', reason: blocked };
    }
  }
  function sealSnapshot() {
    if (unsupportedCostDimension) {
      sealPersisted = false;
      sealFailure = 'budget_seal_unsupported_cost_dimension';
    } else if (breach || totals().total > MAX_SAFE) {
      sealPersisted = false;
      sealFailure = 'budget_seal_proof_unsafe';
    }
    const current = snapshot();
    return sealPersisted && sealFailure === null
      ? { ...current, ok: true, sealed: true }
      : { ...current, ok: false, sealed: true, reason: sealFailure ?? 'budget_seal_not_persisted' };
  }
  function seal() {
    // Latch before every check and write. Even a persistence failure can never
    // permit another outbound reservation or consume another forward attempt.
    sealed = true;
    if (sealAttempted) {
      if (sealPersisted) {
        try { verifyOwn(); }
        catch (error) {
          sealPersisted = false;
          sealFailure = knownReason(error, 'budget_seal_persistence_failed');
        }
      }
      return sealSnapshot();
    }
    sealAttempted = true;
    try {
      check(typeof file === 'string' && sliceCap !== null && caseInfo, 'owned_budget_seal_context_required');
      check(!unsupportedCostDimension, 'budget_seal_unsupported_cost_dimension');
      check(!breach && totals().total <= MAX_SAFE, 'budget_seal_proof_unsafe');
      // The optimistic flag is included in the bytes; it becomes usable proof
      // only when the complete synchronous write and both fsyncs return.
      sealPersisted = true;
      persist();
    } catch (error) {
      sealPersisted = false;
      sealFailure = knownReason(error, 'budget_seal_persistence_failed');
    }
    return sealSnapshot();
  }
  function reserve({ id, model, body, source, transport = 'http' } = {}) {
    if (sealed) return { ok: false, reason: 'outbound_budget_sealed', bound_micros: null };
    if (blocked) return { ok: false, reason: blocked, bound_micros: null };
    let bound = null;
    try {
      check(transport === 'http' || transport === 'responses-ws' || transport === 'responses-ws-steer', 'unsupported_outbound_transport');
      check(typeof id === 'string' && id.length > 0 && id.length <= 4096, 'reservation_id_required');
      const idHash = hash(id);
      check(!records.has(idHash), 'duplicate_reservation_id');
      check(typeof source === 'string' && source.length > 0 && source.length <= 4096, 'reservation_source_required');
      const info = infos.get(model);
      check(info, 'unapproved_native_model');
      const settings = requestSettings(body, model, info, transport);
      bound = settings.bound;
      const total = totals();
      check(records.size < caseInfo[1], 'case_forward_cap_exhausted');
      check(!codex || codexAttempts() + 1 <= 32, 'codex_aggregate_forward_cap_exhausted');
      check(total.total + bound <= sliceCap, 'full_outbound_bound_exceeds_slice');
      check(campaignBefore + total.total + bound <= CAMPAIGN, 'full_outbound_bound_exceeds_campaign');
      records.set(idHash, { id_hash: idHash, source_hash: hash(source), transport, info, ...settings, status: 'IN_FLIGHT', charged: 0n, usage: null, provider_status: null, reason: null, breach: false, unsupported_cost_dimension: false, tool_use_prompt_token_count_observed: null });
      // No admission is returned until its complete maximum hold is durable.
      persist();
      return { ok: true, reason: null, bound_micros: number(bound) };
    } catch (error) {
      blocked = knownReason(error, 'budget_reservation_or_persistence_failed');
      return { ok: false, reason: blocked, bound_micros: bound === null ? null : number(bound) };
    }
  }
  function finish(record) {
    if (sealed && (unsupportedCostDimension || breach || totals().total > MAX_SAFE)) {
      sealPersisted = false;
      sealFailure = unsupportedCostDimension ? 'budget_seal_unsupported_cost_dimension' : 'budget_seal_proof_unsafe';
    }
    try {
      persist();
      return { ...snapshot(), ok: record.status === 'SETTLED' && !record.breach, reason: record.reason };
    } catch {
      blocked = 'budget_settlement_persistence_failed';
      if (sealed) {
        sealPersisted = false;
        sealFailure = blocked;
      }
      return { ...snapshot(), ok: false, reason: blocked };
    }
  }
  function lookup(id) {
    check(typeof id === 'string' && id.length > 0 && id.length <= 4096, 'reservation_id_required');
    const record = records.get(hash(id));
    check(record, 'unreserved_settlement');
    return record;
  }
  function observeGoogleToolInputDimension(record, usage) {
    if (record.info.protocol !== 'google-generate-content') return;
    if (object(usage) && own(usage, 'toolUsePromptTokenCount')) {
      const observed = usage.toolUsePromptTokenCount;
      if (!integer(observed) || observed > 0) {
        record.unsupported_cost_dimension = true;
        unsupportedCostDimension = true;
        if (positive(observed)) {
          // Preserve an actual observed count, never add dimensions or infer
          // recursive scope. Later zero/omission cannot erase this evidence.
          record.tool_use_prompt_token_count_observed = Math.max(record.tool_use_prompt_token_count_observed ?? 0, observed);
        }
      } else if (record.tool_use_prompt_token_count_observed === null) {
        record.tool_use_prompt_token_count_observed = 0;
      }
    }
    if (record.unsupported_cost_dimension) {
      blocked = GOOGLE_TOOL_INPUT_UNVERIFIED;
      fail(GOOGLE_TOOL_INPUT_UNVERIFIED);
    }
  }
  function settle({ id, usage, format, providerStatus, serviceTier } = {}) {
    let record;
    try {
      record = lookup(id);
      observeGoogleToolInputDimension(record, usage);
      const wasSettled = record.status === 'SETTLED';
      if (serviceTier === 'auto' && record.info.protocol === 'openai-responses') {
        serviceTier = record.info.omitted && record.info.omitted !== 'auto'
          ? record.info.omitted
          : record.info.tiers.length === 1 ? record.info.tiers[0] : undefined;
      }
      const newResponseTier = serviceTier !== undefined && serviceTier !== null && record.response_tier === undefined;
      if (serviceTier !== undefined && serviceTier !== null) {
        if (typeof serviceTier !== 'string' || !TIERS.has(serviceTier) || !record.info.tiers.includes(serviceTier)) {
          blocked = 'unexpected_response_service_tier';
          fail(blocked);
        }
        if (record.response_tier !== undefined && record.response_tier !== serviceTier) {
          blocked = 'conflicting_response_service_tier';
          fail(blocked);
        }
        // No undocumented aliases (including fast -> priority) are assumed.
        record.response_tier = serviceTier;
      }
      if (wasSettled && !newResponseTier) return { ...snapshot(), ok: !record.breach, duplicate: true, reason: record.reason };
      if (wasSettled && record.usage === null) return finish(record);
      const settledStatus = wasSettled ? record.provider_status : providerStatus;
      check(Number.isSafeInteger(settledStatus) && settledStatus >= 200 && settledStatus < 300, 'rejected_or_interrupted_provider_result');
      // A late observed tier reprices the existing record in place; duplicate
      // null usage never demotes a known record or adds another charge.
      const counts = wasSettled ? record.usage : usageCounts(usage, format, record.info.protocol, record.cache_ttls);
      const selectedRates = record.response_tier === undefined ? record.rates : selection(record.info, [record.response_tier]);
      const rates = record.prewarm && record.prewarm_policy_citation_sha256.length > 0 ? prewarmRates(selectedRates) : selectedRates;
      if (counts.input_tokens > record.info.input || counts.output_tokens > record.info.output) {
        record.breach = true;
        breach = true;
        blocked = 'provider_usage_bound_breach';
      }
      check(counts.cache_read_input_tokens === 0 || rates.cache_read !== null, 'cache_read_rate_unknown');
      if (record.info.protocol === 'anthropic-messages') {
        check(counts.cache_creation_input_tokens_5m === 0 || rates.cache_write !== null, 'cache_write_rate_unknown');
        check(counts.cache_creation_input_tokens_1h === 0 || rates.cache_write_1h !== null, 'cache_write_ttl_rate_unknown');
      } else check(counts.cache_creation_input_tokens === 0 || rates.cache_write !== null, 'cache_write_rate_unknown');
      const uncached = counts.input_tokens - counts.cache_read_input_tokens - counts.cache_creation_input_tokens;
      const components = [[uncached, rates.input], [counts.output_tokens, rates.output]];
      if (counts.cache_read_input_tokens) components.push([counts.cache_read_input_tokens, rates.cache_read]);
      if (record.info.protocol === 'anthropic-messages') {
        if (counts.cache_creation_input_tokens_5m) components.push([counts.cache_creation_input_tokens_5m, rates.cache_write]);
        if (counts.cache_creation_input_tokens_1h) components.push([counts.cache_creation_input_tokens_1h, rates.cache_write_1h]);
      } else if (counts.cache_creation_input_tokens) components.push([counts.cache_creation_input_tokens, rates.cache_write]);
      const actual = charge(components);
      record.breach ||= actual > record.bound;
      record.charged = record.breach && actual < record.bound ? record.bound : actual;
      record.status = 'SETTLED';
      record.provider_status = settledStatus;
      record.usage = counts;
      record.reason = record.breach ? 'provider_usage_bound_breach' : null;
      if (record.breach) { breach = true; blocked = 'provider_usage_bound_breach'; }
      return finish(record);
    } catch (error) {
      const reason = knownReason(error, 'usage_settlement_invalid');
      if (!record) { blocked = reason; return { ok: false, reason, ...snapshot() }; }
      record.status = 'UNKNOWN';
      record.reason = reason;
      if (Number.isSafeInteger(providerStatus) && providerStatus >= 100 && providerStatus <= 599) record.provider_status = providerStatus;
      // UNKNOWN holds are never released by a timeout, rejection, or no usage.
      return finish(record);
    }
  }
  function settleNonGeneration({ id, proof } = {}) {
    let record;
    try {
      record = lookup(id);
      check(!record.unsupported_cost_dimension, GOOGLE_TOOL_INPUT_UNVERIFIED);
      if (record.status === 'SETTLED') return { ok: !record.breach, duplicate: true, reason: record.reason, ...snapshot() };
      check((plan.case === 'agy' || plan.case === 'api-key-agy') && object(proof) && proof.kind === 'pinned_negative_quota_probe', 'trusted_non_generation_proof_required');
      check(proof.providerAttempts === 1 && proof.status === 400 && proof.errorStatus === 'INVALID_ARGUMENT' && proof.providerEvents === 1 && proof.costKnown === true && proof.attemptsDelta === 1 && proof.costDeltaMicros === 0 && proof.drops === 0 && proof.failoverDelta === 0, 'trusted_non_generation_proof_required');
      const event = proof.event;
      check(object(event) && typeof event.request_id === 'string' && event.request_id.length > 0 && event.request_id.length <= 4096 && event.status === 400 && event.error_code === 'upstream_4xx' && typeof ctx.upstreamId === 'string' && ctx.upstreamId.length > 0 && event.upstream_id === ctx.upstreamId && event.cost_usd_micros === 0 && event.input_tokens === 0 && event.output_tokens === 0, 'trusted_non_generation_proof_required');
      record.proof_hash = hash(JSON.stringify({ kind: proof.kind, request_id_sha256: hash(event.request_id), upstream_id_sha256: hash(ctx.upstreamId), status: 400, error_code: 'upstream_4xx', zero_cost_and_tokens: true, single_provider_attempt: true }));
      record.status = 'SETTLED';
      record.charged = 0n;
      record.provider_status = 400;
      record.reason = 'trusted_pinned_negative_probe_no_generation';
      return finish(record);
    } catch (error) {
      const reason = knownReason(error, 'trusted_non_generation_proof_required');
      if (!record) { blocked = reason; return { ok: false, reason, ...snapshot() }; }
      record.status = 'UNKNOWN';
      record.reason = reason;
      return finish(record);
    }
  }
  return Object.freeze({ reserve, settle, settleNonGeneration, snapshot, preflight, seal });
}
