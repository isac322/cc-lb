// Parent-only in-memory regressions. Importing this module does not run tests,
// read evidence or create files, directories, listeners, processes or resources.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import {
  CURRENT_NATIVE_ARTIFACT_ROOT,
  parseCurrentNativeModelEvidence,
  loadCurrentNativeModelEvidence,
} from './approved-core3-subscription-current-native.mjs';

const SUMMARY_PATH = `${CURRENT_NATIVE_ARTIFACT_ROOT}/summary.json`;
const MODEL = 'synthetic-current-profile-v9';
const sha = (bytes) => createHash('sha256').update(bytes).digest('hex');
const errorCode = (code) => ({ name: 'Error', message: `BLOCKED:current_native_${code}` });

function fixture() {
  const capture = (transport) => ({
    transport, pathname: '/v1/responses', model: { present: true, value: MODEL },
    tools: { present: false },
    additional_tools_input: {
      input_array_present: true, count: 1,
      items: [{ input_index: 0, type: { present: true, value: 'additional_tools' }, tools: { present: true, count: 1, entries: [{ type: { present: true, value: 'function' }, name: { present: true, value: 'synthetic_tool' } }] } }],
    },
  });
  const caseRecord = (name) => ({
    name,
    supports_websockets_config: name === 'ws',
    argv_shape: ['exec', '--json', '--skip-git-repo-check', '-C', '/synthetic/fixture', '<original_prompt_from_pinned_source>'],
    captures: name === 'ws' ? [capture(name), capture(name)] : [capture(name)],
    websocket_handshakes: name === 'ws' ? 2 : 0,
    other_local_requests: [],
    capture_parse_errors: 0,
    terminal_error_codes: ['setup_shape_probe_complete', 'setup_shape_probe_complete'],
    exit_code: 1, signal: null, timeout: false,
    result: 'CAPTURED_TERMINAL_REJECTION_EXPECTED_NOT_CODING_SUCCESS',
  });
  return {
    classification: 'SETUP_SHAPE_PROBE_ONLY_NOT_PROVIDER_QA',
    result: 'CAPTURED_HTTP_AND_WS_SHAPES',
    cli: { path: '/synthetic/codex/bin/codex', sha256: 'b'.repeat(64), version: 'codex-cli synthetic-test' },
    cases: [caseRecord('http'), caseRecord('ws')],
    safety: { actual_credentials_loaded: false, provider_forwarding: false, fresh_synthetic_auth_only: true },
    model_override: { CODEX_QA_MODEL_passed: false, generated_config_model_present: false, argv_model_flag_present: false },
  };
}

function parseFixture(value = fixture()) {
  const raw = JSON.stringify(value);
  return parseCurrentNativeModelEvidence(raw, { expectedSha256: sha(raw), sourcePath: SUMMARY_PATH });
}

function rejectsMutation(change, code) {
  const value = fixture();
  change(value);
  assert.throws(() => parseFixture(value), errorCode(code));
}

export async function runCurrentNativeEvidenceTests() {
  const value = fixture();
  const raw = JSON.stringify(value);
  const options = { expectedSha256: sha(raw), sourcePath: SUMMARY_PATH };
  const originalOptions = { ...options };
  const originalMetadata = structuredClone(value);
  const observation = parseCurrentNativeModelEvidence(raw, options);
  assert.equal(observation.model, MODEL, 'observed model is arbitrary, never a fixed historical model');
  assert.deepEqual(observation.cli, value.cli);
  assert.deepEqual(observation.provenance, {
    kind: 'LOCAL_SYNTHETIC_SETUP_NOT_AUTHENTICATED_SESSION',
    evidence_path: SUMMARY_PATH, evidence_sha256: options.expectedSha256,
  });
  assert.deepEqual(Object.keys(observation).sort(), ['cli', 'current_user_session_default_proven', 'model', 'no_model_override', 'provenance', 'provider_qa_verified']);
  assert.equal(observation.no_model_override, true);
  assert.equal(observation.current_user_session_default_proven, false);
  assert.equal(observation.provider_qa_verified, false);
  assert(Object.isFrozen(observation));
  assert(Object.isFrozen(observation.cli));
  assert(Object.isFrozen(observation.provenance));
  assert.throws(() => { observation.model = 'replaced'; }, TypeError);
  assert.throws(() => { observation.cli.version = 'replaced'; }, TypeError);
  assert.throws(() => { observation.provenance.kind = 'replaced'; }, TypeError);
  assert.deepEqual(options, originalOptions);
  assert.deepEqual(value, originalMetadata);
  const bytes = Buffer.from(raw);
  const originalBytes = Buffer.from(bytes);
  assert.deepEqual(parseCurrentNativeModelEvidence(bytes, options), observation);
  assert.deepEqual(bytes, originalBytes, 'exact byte input is not mutated');

  assert.throws(() => parseCurrentNativeModelEvidence(raw, { ...options, expectedSha256: 'a'.repeat(64) }), errorCode('metadata_sha256_mismatch'));
  assert.throws(() => parseCurrentNativeModelEvidence(`${raw}\n`, options), errorCode('metadata_sha256_mismatch'), 'whitespace changes exact-byte identity');
  const malformed = '{"secret-canary-not-for-errors":';
  assert.throws(() => parseCurrentNativeModelEvidence(malformed, { ...options, expectedSha256: 'a'.repeat(64) }), errorCode('metadata_sha256_mismatch'), 'hash validation precedes JSON decoding');
  assert.throws(() => parseCurrentNativeModelEvidence(malformed, { ...options, expectedSha256: sha(malformed) }), errorCode('metadata_json_invalid'));
  const invalidUtf8 = Buffer.from([0xff, 0xfe]);
  assert.throws(() => parseCurrentNativeModelEvidence(invalidUtf8, { ...options, expectedSha256: sha(invalidUtf8) }), errorCode('metadata_json_invalid'));
  for (const bad of [null, {}, [], 0]) assert.throws(() => parseCurrentNativeModelEvidence(bad, options), errorCode('metadata_bytes_invalid'));
  assert.throws(() => parseCurrentNativeModelEvidence('', options), errorCode('metadata_size_invalid'));
  assert.throws(() => parseCurrentNativeModelEvidence('x'.repeat(2 * 1024 * 1024 + 1), options), errorCode('metadata_size_invalid'));
  assert.throws(() => parseCurrentNativeModelEvidence('é'.repeat(1024 * 1024 + 1), options), errorCode('metadata_size_invalid'), 'byte limit is not a character limit');
  assert.throws(() => parseCurrentNativeModelEvidence(raw, null), errorCode('options_invalid'));
  for (const expectedSha256 of [undefined, null, true, 'bad', 'A'.repeat(64)]) assert.throws(() => parseCurrentNativeModelEvidence(raw, { ...options, expectedSha256 }), errorCode('expected_sha256_invalid'));

  for (const flag of ['CODEX_QA_MODEL_passed', 'generated_config_model_present', 'argv_model_flag_present']) {
    for (const bad of [true, undefined, null, 0, 'false']) rejectsMutation((v) => { v.model_override[flag] = bad; }, 'model_override_detected');
  }
  rejectsMutation((v) => { delete v.model_override; }, 'override_metadata_missing');
  for (const args of [['--model', 'unapproved'], ['--model=unapproved'], ['-m', 'unapproved'], ['-m=unapproved'], ['-c', 'model="unapproved"'], ['--config=model="unapproved"']]) {
    rejectsMutation((v) => { v.cases[0].argv_shape.push(...args); }, 'model_override_detected');
  }
  rejectsMutation((v) => { delete v.cases[0].argv_shape; }, 'argv_metadata_invalid');
  rejectsMutation((v) => { v.cases[1].captures[1].model.value = 'different-synthetic-model'; }, 'captured_models_conflict');
  for (const bad of [undefined, null, {}, [], '']) rejectsMutation((v) => { v.cases[0].model = bad; delete v.cases[0].captures[0].model; }, 'capture_model_invalid');
  for (const bad of ['', ' ', ' padded', 'line\nbreak', 'model/alias', 'x'.repeat(201), null, 42]) rejectsMutation((v) => { v.cases[0].captures[0].model.value = bad; }, 'capture_model_invalid');
  rejectsMutation((v) => { v.cases[0].captures[0].model.present = false; }, 'capture_model_invalid');
  for (const bad of [undefined, null, [], [fixture().cases[0]]]) rejectsMutation((v) => { v.cases = bad; }, 'http_ws_cases_required');
  rejectsMutation((v) => { v.cases[1].name = 'http'; }, 'http_ws_cases_required');
  for (const bad of [undefined, null, []]) rejectsMutation((v) => { v.cases[0].captures = bad; }, 'captures_missing');
  rejectsMutation((v) => { v.cases[0].captures[0].transport = 'ws'; }, 'capture_metadata_invalid');
  rejectsMutation((v) => { v.cases[0].captures[0].pathname = '/other'; }, 'capture_metadata_invalid');
  rejectsMutation((v) => { v.cases[0].result = 'PASS'; }, 'case_setup_result_invalid');
  rejectsMutation((v) => { v.cases[1].capture_parse_errors = 1; }, 'case_setup_result_invalid');
  rejectsMutation((v) => { v.cases[1].timeout = true; }, 'case_setup_result_invalid');
  rejectsMutation((v) => { v.cases[1].terminal_error_codes = []; }, 'case_setup_result_invalid');
  rejectsMutation((v) => { v.cases[1].websocket_handshakes = 0; }, 'case_transport_invalid');
  rejectsMutation((v) => { delete v.cli; }, 'cli_identity_invalid');
  for (const key of ['path', 'version', 'sha256']) rejectsMutation((v) => { delete v.cli[key]; }, 'cli_identity_invalid');
  for (const bad of ['', 'relative/bin', '/synthetic/../credentials', '/line\nbreak']) rejectsMutation((v) => { v.cli.path = bad; }, 'cli_identity_invalid');
  for (const bad of ['', ' ', '\u0000', null]) rejectsMutation((v) => { v.cli.version = bad; }, 'cli_identity_invalid');
  rejectsMutation((v) => { v.cli.sha256 = 'invalid'; }, 'cli_identity_invalid');
  rejectsMutation((v) => { v.classification = 'PROVIDER_QA'; }, 'setup_shape_result_required');
  rejectsMutation((v) => { v.result = 'FAILED'; }, 'setup_shape_result_required');
  for (const flag of ['actual_credentials_loaded', 'provider_forwarding']) {
    for (const bad of [true, undefined, 'false']) rejectsMutation((v) => { v.safety[flag] = bad; }, 'local_synthetic_safety_required');
  }
  rejectsMutation((v) => { delete v.safety; }, 'local_synthetic_safety_required');
  rejectsMutation((v) => { v.safety.fresh_synthetic_auth_only = false; }, 'local_synthetic_safety_required');
  const withoutTools = fixture();
  for (const entry of withoutTools.cases) for (const capture of entry.captures) {
    delete capture.tools;
    delete capture.additional_tools_input;
  }
  assert.equal(parseFixture(withoutTools).model, MODEL, 'tool metadata is not required for model observation or interpreted as search-off');
  assert.equal(parseFixture({ ...fixture(), cases: fixture().cases.reverse() }).model, MODEL, 'case order does not change identity');

  // All reader calls below fail argument validation before filesystem I/O.
  // No tests open the approved artifact, credential files, or synthetic files.
  for (const bad of [undefined, null, 'summary.json', '/tmp/other/summary.json', `${CURRENT_NATIVE_ARTIFACT_ROOT}/credentials.json`, `${CURRENT_NATIVE_ARTIFACT_ROOT}/nested/summary.json`, `${CURRENT_NATIVE_ARTIFACT_ROOT}/../summary.json`, `${CURRENT_NATIVE_ARTIFACT_ROOT}/./summary.json`, `${CURRENT_NATIVE_ARTIFACT_ROOT}//summary.json`, `${SUMMARY_PATH}\u0000`]) {
    assert.throws(() => parseCurrentNativeModelEvidence(raw, { ...options, sourcePath: bad }), errorCode('evidence_path_invalid'));
    await assert.rejects(loadCurrentNativeModelEvidence({ path: bad, sha256: options.expectedSha256 }), errorCode('evidence_path_invalid'));
  }
  for (const bad of [undefined, 'bad', true]) await assert.rejects(loadCurrentNativeModelEvidence({ path: SUMMARY_PATH, sha256: bad }), errorCode('expected_sha256_invalid'));
  await assert.rejects(loadCurrentNativeModelEvidence(null), errorCode('options_invalid'));
  return Object.freeze({ kind: 'PURE_IN_MEMORY_REGRESSIONS', actual_evidence_read: false, provider_qa_verified: false, current_user_session_default_proven: false });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runCurrentNativeEvidenceTests();
  console.log('Pure current-native evidence regressions completed; actual reader/provider/user-session QA NOT RUN.');
}
