import { describe, expect, test } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { findBlockedTerm, renderSourceNotes } from './sync-docs';

const runtimeSource = 'docs/runtime-management.md';

describe('source documentation policy', () => {
  test.each([
    'OpenAI Responses uses POST /v1/responses and a WebSocket upgrade.',
    'Gemini generation supports generateContent and streamGenerateContent.',
    'OpenAI and Gemini OAuth accept access-only bundles with never_refresh: true.',
  ])('allows implemented runtime reference facts: %s', (content) => {
    expect(findBlockedTerm(content, runtimeSource)).toBeUndefined();
  });

  test.each([
    'docs/upstream-warmup.md',
    'docs/scheduler.md',
    'docs/plugin-author-guide.md',
    'docs/consistency-contract.md',
  ])('keeps provider claims blocked outside the runtime reference: %s', (source) => {
    expect(findBlockedTerm('OpenAI Responses', source)).toBe('OpenAI');
    expect(findBlockedTerm('Gemini generation', source)).toBe('Gemini');
  });

  test.each([
    ['Bedrock generation', 'Bedrock'],
    ['Vertex generation', 'Vertex'],
    ['official OpenAI integration', 'official'],
    ['Gemini endorsed integration', 'endorsed'],
    ['/Users/operator/secret', '/Users/'],
    ['/data/private/secret', '/data/'],
    ['.omo/evidence/private.json', '.omo/'],
    ['~/.config/cc-lb/key', '~/.config/'],
    ['production-grade gateway', 'production-grade'],
    ['enterprise-grade gateway', 'enterprise-grade'],
    ['battle-tested gateway', 'battle-tested'],
    ['highly available deployment', 'highly available'],
    ['99.999% availability', '99.999%'],
    ['SLA guarantee', 'SLA'],
    ['D-arch-3', 'D-arch-3'],
    ['Universal multi-provider LLM gateway', 'Universal multi-provider LLM gateway'],
    ['Full Gemini API compatibility', 'Full Gemini API compatibility'],
    ['Complete OpenAI API support', 'Complete OpenAI API support'],
  ])('rejects unsupported or private runtime claims: %s', (content, blocked) => {
    expect(findBlockedTerm(content, runtimeSource)).toBe(blocked);
  });

  test('keeps unrelated provider-free reference prose accepted', () => {
    expect(findBlockedTerm('Database-backed runtime management.', 'docs/scheduler.md'))
      .toBeUndefined();
  });
});

describe('source documentation consumers', () => {
  test('renders supported technical sections from the current runtime source', async () => {
    const source = await readFile(
      new URL('../../docs/runtime-management.md', import.meta.url),
      'utf8',
    );
    const rendered = renderSourceNotes(source, runtimeSource);

    expect(rendered).toContain('`candidatesTokenCount + thoughtsTokenCount`');
    const bundles = [...rendered.matchAll(/```json[ \t]*\r?\n([\s\S]*?)```/g)]
      .filter(([, json]) => /"never_refresh"\s*:/.test(json))
      .map(([, json]) => JSON.parse(json))
      .map((example) => example.oauth_tokens ?? example);
    const accessOnly = bundles.filter((bundle) => bundle.never_refresh === true);

    expect(accessOnly).toContainEqual(expect.objectContaining({
      access_token: expect.stringMatching(/\S/),
      refresh_token: '',
      expires_at_unix_secs: expect.any(Number),
      scopes: expect.any(Array),
      never_refresh: true,
    }));
    for (const bundle of accessOnly) {
      expect(bundle.refresh_token_expires_at_unix_secs ?? null).toBeNull();
    }
    expect(rendered).toContain('`POST /v1/responses`');
    expect(rendered).toContain('`:streamGenerateContent`');
  });

  test('selects technical sections, sanitizes local paths, and rewrites relative links', () => {
    const source = [
      '## Admin v1 REST API',
      'OpenAI Responses and Gemini generation.',
      '`~/.config/cc-lb/proxy-key`',
      '[Contract](consistency-contract.md)',
      '## Private operational notes',
      '/data/private/evidence.json',
    ].join('\n');
    const rendered = renderSourceNotes(source, runtimeSource);

    expect(rendered).toContain('OpenAI Responses and Gemini generation.');
    expect(rendered).toContain('a local file');
    expect(rendered).toContain(
      'https://github.com/isac322/cc-lb/blob/master/docs/consistency-contract.md',
    );
    expect(rendered).not.toContain('/data/private/');
    expect(rendered).not.toContain('Private operational notes');
  });

  test('keeps a selected ancestor active after a nested selected heading', () => {
    const source = [
      '## Admin v1 REST API',
      'Parent content.',
      '### Status and Export API',
      'Status content.',
      '### CLI/API OAuth bundle import',
      'Access-only credentials.',
      '## Unselected operational notes',
      '/data/private/evidence.json',
    ].join('\n');
    const rendered = renderSourceNotes(source, runtimeSource);

    expect(rendered).toContain('Parent content.');
    expect(rendered).toContain('Status content.');
    expect(rendered).toContain('Access-only credentials.');
    expect(rendered).not.toContain('Unselected operational notes');
    expect(rendered).not.toContain('/data/private/');
  });

  test('ends a separately selected subsection at its unselected sibling', () => {
    const source = [
      '## Unselected parent',
      '### Status and Export API',
      'Selected status content.',
      '### Unselected sibling',
      '/data/private/evidence.json',
    ].join('\n');
    const rendered = renderSourceNotes(source, runtimeSource);

    expect(rendered).toContain('Selected status content.');
    expect(rendered).not.toContain('Unselected parent');
    expect(rendered).not.toContain('Unselected sibling');
    expect(rendered).not.toContain('/data/private/');
  });

  test.each([
    'Vertex generation',
    'Bedrock generation',
    'official OpenAI integration',
    'Full Gemini API compatibility',
    'Universal multi-provider LLM gateway',
  ])('rejects prohibited claims before rendering a mirror: %s', (claim) => {
    const source = `## Admin v1 REST API\n\n${claim}\n`;
    expect(() => renderSourceNotes(source, runtimeSource)).toThrow('Sync filter rejected');
  });

  test('does not extend the runtime provider allowlist to another source', () => {
    const source = '## What it does\n\nOpenAI Responses\n';
    expect(() => renderSourceNotes(source, 'docs/upstream-warmup.md'))
      .toThrow('Sync filter rejected');
  });
});
