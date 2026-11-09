#!/usr/bin/env node
// Verify that the authored public documentation describes the shipped provider
// kinds, native routes, scope boundaries, and prompt-cache eligibility.
//
// This check intentionally uses only Node's standard library so it can run with
// either `node` or `bun`. It never writes files. Use --root <dir> to inspect a
// scratch copy of the repository.

import { existsSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const argv = process.argv.slice(2);
let rootArg;
for (let i = 0; i < argv.length; i += 1) {
  const arg = argv[i];
  if (arg === '--root') {
    rootArg = argv[++i];
    if (!rootArg) {
      console.error('FAIL --root requires a directory');
      process.exit(1);
    }
  } else if (arg.startsWith('--root=')) {
    rootArg = arg.slice('--root='.length);
    if (!rootArg) {
      console.error('FAIL --root requires a directory');
      process.exit(1);
    }
  } else {
    console.error(`FAIL unknown argument '${arg}'`);
    process.exit(1);
  }
}

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(rootArg ?? path.join(scriptDir, '..'));
const failures = [];
const contents = new Map();

function fail(message) {
  failures.push(message);
}

function readText(relativePath) {
  if (contents.has(relativePath)) return contents.get(relativePath);
  const absolutePath = path.join(repoRoot, relativePath);
  if (!existsSync(absolutePath) || !statSync(absolutePath).isFile()) {
    fail(`missing ${relativePath}`);
    contents.set(relativePath, '');
    return '';
  }
  try {
    const text = readFileSync(absolutePath, 'utf8');
    contents.set(relativePath, text);
    return text;
  } catch (error) {
    fail(`cannot read ${relativePath}: ${error.message}`);
    contents.set(relativePath, '');
    return '';
  }
}

function requirePattern(label, text, pattern) {
  if (!pattern.test(text)) fail(`${label}: missing ${pattern.source}`);
}

function requireAllKinds(label, text) {
  for (const kind of KINDS) {
    if (!text.includes(kind)) fail(`${label}: missing upstream kind ${kind}`);
  }
}

function requireNativeRoutes(label, text) {
  const routeLines = text.split(/\r?\n/);
  const lineWith = (pattern) => routeLines.some((line) => pattern.test(line));

  if (!lineWith(/\bPOST\s+`?\/?(?:anthropic\/)?v1\/messages`?(?![\w/])/i)) {
    fail(`${label}: missing Anthropic POST /v1/messages`);
  }
  if (!lineWith(/\bPOST\s+`?\/?(?:anthropic\/)?v1\/messages\/count_tokens`?/i)) {
    fail(`${label}: missing Anthropic POST /v1/messages/count_tokens`);
  }
  if (!lineWith(/\bPOST\s+`?\/?(?:openai\/)?v1\/responses`?(?![\w/])/i)) {
    fail(`${label}: missing OpenAI POST /v1/responses`);
  }
  if (!lineWith(/\bGET\s+`?\/?(?:openai\/)?v1\/responses`?(?![\w/]).*\bwebsocket\b/i)) {
    fail(`${label}: missing OpenAI GET /v1/responses WebSocket`);
  }

  const hasGeminiPath = lineWith(
    /\bPOST\s+`?\/?(?:gemini\/)?v1beta\/models\/\{model\}:generateContent`?/i,
  );
  if (!hasGeminiPath) fail(`${label}: missing Gemini generateContent route`);
  if (!text.includes(':streamGenerateContent')) {
    fail(`${label}: missing Gemini :streamGenerateContent route`);
  }
  if (!text.includes(':countTokens')) {
    fail(`${label}: missing Gemini :countTokens route`);
  }
}

function hasNegativeTranslationBoundary(text) {
  const lines = text.split(/\r?\n/);
  return lines.some((line, index) => {
    const context = lines.slice(Math.max(0, index - 1), index + 2).join(' ');
    return /(?:\b(?:does|do) not translate\b|\bnot translate\b|\b(?:unsupported|excluded|outside)\b[\s\S]{0,120}\bcross[- ]provider\s+translation\b|\bcross[- ]provider\s+translation\b[\s\S]{0,120}\b(?:unsupported|excluded|outside)\b)/i.test(
      context,
    );
  });
}

function requireNegativeTranslationBoundary(label, text) {
  if (!hasNegativeTranslationBoundary(text)) {
    fail(`${label}: missing negative cross-provider translation boundary`);
  }
}

function requireNativeBoundary(label, text) {
  requirePattern(`${label}: native protocol boundary`, text, /native|provider-native/i);
  requireNegativeTranslationBoundary(label, text);
  requirePattern(`${label}: Bedrock scope`, text, /\bBedrock\b/i);
  requirePattern(`${label}: Vertex scope`, text, /\bVertex\b/i);
}

const KINDS = [
  'anthropic_api_key',
  'anthropic_oauth',
  'openai_api_key',
  'openai_oauth',
  'gemini_api_key',
  'gemini_oauth',
];

// These pages carry the canonical public scope. The other overview pages only
// need a provider-aware summary; setup examples are checked separately.
const canonicalScopePages = [
  ['upstreams page', 'site/src/content/docs/concepts/upstreams.md'],
  ['trust page', 'site/src/content/docs/trust/index.md'],
];
const overviewPages = [
  ['site landing page', 'site/src/pages/index.astro'],
  ['site docs index', 'site/src/content/docs/index.md'],
  ['concepts index', 'site/src/content/docs/concepts/index.md'],
  ['getting-started page', 'site/src/content/docs/getting-started/index.md'],
];

function requireProviderOverview(label, text) {
  requirePattern(`${label}: Anthropic coverage`, text, /\bAnthropic\b/i);
  requirePattern(`${label}: OpenAI coverage`, text, /\bOpenAI\b/i);
  requirePattern(`${label}: Gemini coverage`, text, /\bGemini\b/i);
  requirePattern(`${label}: native protocol boundary`, text, /native|provider-native/i);
  requireNegativeTranslationBoundary(label, text);
}

for (const [label, relativePath] of canonicalScopePages) {
  const text = readText(relativePath);
  requireAllKinds(label, text);
  requireNativeRoutes(label, text);
  requireNativeBoundary(label, text);
}
for (const [label, relativePath] of overviewPages) {
  requireProviderOverview(label, readText(relativePath));
}

const installPage = readText('site/src/content/docs/getting-started/install.md');

const endpointReferencePath = 'site/src/content/docs/reference/source/runtime-management.md';
const endpointReference = readText(endpointReferencePath);
requirePattern(
  'endpoint reference: Anthropic route family',
  endpointReference,
  /\/anthropic\/v1\/messages[\s\S]{0,80}\/anthropic\/v1\/messages\/count_tokens/i,
);
requirePattern('endpoint reference: OpenAI route family', endpointReference, /\/openai\/v1\/responses/i);
requirePattern(
  'endpoint reference: Gemini route family',
  endpointReference,
  /\/gemini\/v1beta\/models\/\{model\}:generateContent[\s\S]{0,80}:streamGenerateContent[\s\S]{0,80}:countTokens/i,
);
for (const prefix of ['/anthropic/v1/', '/openai/v1/', '/gemini/v1beta/']) {
  if (!endpointReference.includes(prefix)) {
    fail(`endpoint reference: missing provider-prefixed route family ${prefix}...`);
  }
}

// Only the standalone HTML guide needs the HTML-specific stale-claim scan.
// The site landing page is already checked as an overview page; recursive
// traversal would duplicate those assertions and inspect unrelated pages.
const htmlFiles = ['docs/cc-lb-guide.html'];

const htmlGuide = readText('docs/cc-lb-guide.html');
const htmlGuideChecks = [
  ['Anthropic messages route', /POST\s+<code>\/v1\/messages<\/code>/i],
  ['Anthropic count_tokens route', /<code>\/v1\/messages\/count_tokens<\/code>/i],
  ['OpenAI responses route', /POST\s+<code>\/v1\/responses<\/code>/i],
  ['OpenAI responses WebSocket', /GET\s+<code>\/v1\/responses<\/code>\s*WebSocket/i],
  ['Gemini generateContent route', /POST\s+<code>\/v1beta\/models\/\{model\}:generateContent<\/code>/i],
  ['Gemini streamGenerateContent route', /<code>:streamGenerateContent<\/code>/i],
  ['Gemini countTokens route', /<code>:countTokens<\/code>/i],
];
for (const [label, pattern] of htmlGuideChecks) {
  requirePattern(`HTML operator guide: ${label}`, htmlGuide, pattern);
}
requirePattern(
  'HTML operator guide: unsupported provider scope',
  htmlGuide,
  /지원하지 않는 provider 경로[\s\S]{0,220}\bBedrock\b[\s\S]{0,80}\bVertex\b[\s\S]{0,80}\bSigV4\b[\s\S]{0,80}\bADC\b/i,
);
requirePattern(
  'HTML operator guide: no cross-provider translation',
  htmlGuide,
  /cross-provider[\s\S]{0,120}(?:변환\s*없음|변환은 지원하지 않습니다|does not translate|not supported)/i,
);

const staleClaimPatterns = [
  ['Anthropic-only claim', /\b(?:Anthropic[- ]only|only\s+Anthropic)\b/i],
  ['no-WebSocket claim', /\b(?:no|without|does not support|doesn\x27t support)\s+(?:a\s+)?(?:provider\s+)?websockets?\b/i],
  ['cloud setup claim', /\b(?:cloud\s+(?:setup|translation)|(?:setup|translation)\s+(?:for|with|via)\s+(?:AWS|Google|Azure)\s+Cloud)\b/i],
  ['two-kind claim', /\btwo\s+supported\s+upstream\s+kinds\b/i],
  [
    'positive provider translation claim',
    /\b(?:cc-lb|the\s+(?:proxy|gateway))\s+(?:translates?|supports?|provides?)\b[^.\n]{0,120}\b(?:between\s+providers|cross[- ]provider\s+translation)\b/i,
  ],
];

function markdownSections(text) {
  const headings = [...text.matchAll(/^#{1,6}\s+(.+)$/gm)];
  const sections = [];
  if (headings.length && headings[0].index > 0) {
    sections.push({ heading: '', text: text.slice(0, headings[0].index) });
  }
  for (let index = 0; index < headings.length; index += 1) {
    const heading = headings[index];
    const bodyStart = heading.index + heading[0].length;
    const bodyEnd = headings[index + 1]?.index ?? text.length;
    sections.push({ heading: heading[1].trim(), text: text.slice(bodyStart, bodyEnd) });
  }
  return sections.length ? sections : [{ heading: '', text }];
}

function sentenceFragments(text) {
  return text
    .split(/(?:\r?\n)+|(?<=[.!?])\s+/u)
    .map((fragment) => fragment.trim())
    .filter(Boolean);
}

function isAllowedAnthropicSpecificExample(sectionHeading, sentence, allowAnthropicExample) {
  if (!allowAnthropicExample) return false;
  const anthropicExampleHeading =
    /\bAnthropic\b/i.test(sectionHeading) &&
    /\b(?:example|request|upstream|api[- ]key|messages|oauth|setup|flow)\b/i.test(sectionHeading);
  return (
    anthropicExampleHeading &&
    /\bAnthropic\b/i.test(sentence) &&
    /\b(?:example|this request|this flow|this upstream|shown|specific)\b/i.test(sentence)
  );
}

function isAllowedScopedAnthropicClaim(claim, sentence) {
  return (
    claim === 'Anthropic-only claim' &&
    /\bAnthropic-only\s+`?workspace_id`?\b/i.test(sentence) &&
    /\bnot used\b/i.test(sentence)
  );
}

function scanStaleClaims(label, text, { allowAnthropicExample = false } = {}) {
  for (const section of markdownSections(text)) {
    for (const sentence of sentenceFragments(`${section.heading}\n${section.text}`)) {
      for (const [claim, pattern] of staleClaimPatterns) {
        if (
          pattern.test(sentence) &&
          !isAllowedScopedAnthropicClaim(claim, sentence) &&
          !isAllowedAnthropicSpecificExample(section.heading, sentence, allowAnthropicExample)
        ) {
          fail(`${label}: rejects ${claim}`);
        }
      }
    }
  }
}

const htmlLegacyClaimPatterns = [
  ['aws_sigv4 credential marker', /\baws_sigv4\b/i],
  ['gcp_oauth credential marker', /\bgcp_oauth\b/i],
  ['AWS_PROFILE credential marker', /\bAWS_PROFILE\b/],
  ['GOOGLE_APPLICATION_CREDENTIALS credential marker', /\bGOOGLE_APPLICATION_CREDENTIALS\b/],
  ['event-stream-to-SSE claim', /\bevent[-\s]+stream(?:\s*(?:-|→|to)\s*)SSE\b/i],
  ['cloud error-normalization claim', /(?:\bcloud\b[\s-]*)?(?:error\s+JSON|error)[\s\S]{0,60}\bnormaliz(?:ation|e)\b/i],
  ['Bedrock model curl', /\bcurl\b[\s\S]{0,240}\b(?:bedrock-runtime|bedrock)\b/i],
  ['uc-mix-provider use case', /\buc-mix-provider\b/i],
  ['verified Bedrock/Vertex claim', /(?:\b(?:Bedrock|Vertex)\b[\s\S]{0,120}\b(?:verified|검증됨)\b|\b(?:verified|검증됨)\b[\s\S]{0,120}\b(?:Bedrock|Vertex)\b)/i],
];

for (const relativePath of htmlFiles) {
  const text = readText(relativePath);
  for (const [label, pattern] of [...staleClaimPatterns, ...htmlLegacyClaimPatterns]) {
    if (pattern.test(text)) fail(`${relativePath}: rejects ${label}`);
  }
}
for (const [label, relativePath] of [...canonicalScopePages, ...overviewPages]) {
  scanStaleClaims(label, readText(relativePath), { allowAnthropicExample: true });
}
scanStaleClaims('install page', installPage, { allowAnthropicExample: true });

// The install path keeps a complete Anthropic example, then points operators
// to provider-specific API-key and OAuth setup without requiring an Anthropic
// credential first.
const installDocs = `${readText('site/src/content/docs/getting-started/index.md')}\n${installPage}`;
requirePattern(
  'install docs: Anthropic API-key example',
  installPage,
  /curl[\s\S]{0,240}\/admin\/v1\/upstreams[\s\S]{0,500}"kind":\s*"anthropic_api_key"[\s\S]{0,180}"api_key_value"/i,
);
requirePattern('install docs: README pointer', installDocs, /README/i);
requirePattern('install docs: runtime-management pointer', installDocs, /runtime[- ]management/i);
requirePattern(
  'install docs: OpenAI API-key setup',
  installPage,
  /Create an OpenAI API-key upstream[\s\S]{0,700}"kind":\s*"openai_api_key"[\s\S]{0,180}"api_key_value"/i,
);
requirePattern(
  'install docs: Gemini API-key setup',
  installPage,
  /Create a Gemini API-key upstream[\s\S]{0,700}"kind":\s*"gemini_api_key"[\s\S]{0,180}"api_key_value"/i,
);
requirePattern('install docs: interactive OAuth link', installPage, /\]\(\/docs\/reference\/admin-api\/#oauth-flow\)/i);
requirePattern(
  'install docs: OAuth bundle link',
  installPage,
  /\]\(\/docs\/reference\/source\/runtime-management\/#cliapi-oauth-bundle-import\)/i,
);
requirePattern(
  'install docs: Antigravity credentials link',
  installPage,
  /\]\(\/docs\/reference\/configuration\/#subscription-oauth-providers\)/i,
);
requirePattern(
  'install docs: Antigravity terms warning link',
  installPage,
  /Antigravity terms warning\]\(\/docs\/reference\/admin-api\/#oauth-flow\)/i,
);

// Keep-alive scope is deliberately narrower than the six-provider proxy scope.
const promptCache = readText('site/src/content/docs/concepts/prompt-cache.md');
const operatorGuide = readText('docs/cache-keepalive-operator.md');
const cacheChecks = [
  ['Anthropic OAuth zero-output renewal', /Anthropic OAuth[\s\S]{0,220}max_tokens:\s*0/i],
  [
    'opted-in OpenAI Responses native budget',
    /opted[- ]in[\s\S]{0,180}OpenAI Responses[\s\S]{0,260}(?:max_output_tokens|refresh budget|prompt_cache_options\.ttl)/i,
  ],
  ['GPT-5.6 boundary', /GPT[- ]5\.6/i],
  ['Anthropic API-key disabled', /Anthropic API[- ]key[\s\S]{0,160}?(?:unsupported|disabled|skipped|not supported)/i],
];
for (const [label, pattern] of cacheChecks) {
  requirePattern(`prompt-cache page: ${label}`, promptCache, pattern);
}
requirePattern(
  'prompt-cache page: Gemini unsupported and separate evidence',
  promptCache,
  /Gemini[\s\S]{0,180}(?:background prompt-cache replay|background replay)[\s\S]{0,100}unsupported[\s\S]{0,180}Gemini cache evidence/i,
);
requirePattern(
  'operator guide: Anthropic zero-output renewal',
  operatorGuide,
  /Anthropic[\s\S]{0,100}zero-output[\s\S]{0,100}renewals?/i,
);
requirePattern(
  'operator guide: opted-in OpenAI native budget',
  operatorGuide,
  /opted[- ]in[\s\S]{0,120}OpenAI Responses[\s\S]{0,300}(?:prompt_cache_options\.ttl|max_output_tokens|refresh budget)/i,
);
requirePattern('operator guide: unsupported-provider terminal', operatorGuide, /unsupported_provider/i);
const openAiExclusion = /\b(?:unsupported|excluded|disabled|not supported|never replay)\b/i;
function extractMarkdownSection(text, headingPattern) {
  const headingMatch = text.match(headingPattern);
  if (!headingMatch || headingMatch.index === undefined) return '';

  const bodyStart = headingMatch.index + headingMatch[0].length;
  const remainder = text.slice(bodyStart);
  const nextHeadingOffset = remainder.search(/^###\s+/m);
  const sectionEnd = nextHeadingOffset === -1 ? text.length : bodyStart + nextHeadingOffset;
  return text.slice(headingMatch.index, sectionEnd);
}

function hasBlanketOpenAiExclusion(section) {
  return section.split(/(?<=[.!?])\s+/).some((sentence) => {
    return /\bOpenAI Responses\b/i.test(sentence) && openAiExclusion.test(sentence);
  });
}

const openAiSection = extractMarkdownSection(promptCache, /^###\s+OpenAI Responses\b[^\r\n]*$/m);
if (!openAiSection) fail('prompt-cache page: missing OpenAI Responses section');
if (hasBlanketOpenAiExclusion(openAiSection)) {
  fail('prompt-cache page: rejects blanket OpenAI exclusion');
}
requirePattern(
  'prompt-cache page: OpenAI provider gating',
  promptCache,
  /Keep-alive firing is provider-gated:[^.!?\r\n]*\bOpenAI Responses sessions may fire\b/i,
);
const genericZeroOutput = /\b(?:all|any|every|each)\s+(?:provider|upstream|request)s?\b[\s\S]{0,120}(?:zero-output|max_tokens:\s*0)/i;
if (genericZeroOutput.test(promptCache)) fail('prompt-cache page: rejects generic zero-output claim');
// These files are controls: the docs checker must not become a way to remove
// the repository's public-scope and positioning references.
const readme = readText('README.md');
const positioning = readText('positioning.yml');
requirePattern(
  'README control',
  readme,
  /^## Supported scope\s*$[\s\S]{0,1800}(?:anthropic_api_key|anthropic_oauth)[\s\S]{0,1200}(?:openai_api_key|openai_oauth)[\s\S]{0,1200}(?:gemini_api_key|gemini_oauth)[\s\S]{0,1200}(?:Bedrock|Vertex)[\s\S]{0,400}(?:not|outside|unsupported)/im,
);
requirePattern(
  'positioning control',
  positioning,
  /^\s*proof_inventory:\s*$[\s\S]{0,1800}\b(?:Anthropic|OpenAI|Gemini)\b[\s\S]{0,1200}\bnot_claimable:\s*$[\s\S]{0,800}(?:Bedrock|Vertex|unsupported)/im,
);
requirePattern('positioning unsupported-scope control', positioning, /^\s+not_claimable:\s*$/m);

if (failures.length) {
  for (const failure of failures) console.error(`FAIL ${failure}`);
  console.error(`docs-provider-parity: ${failures.length} failure(s)`);
  process.exit(1);
}

console.log('docs-provider-parity: authored provider and prompt-cache surfaces match the shipped contract');
