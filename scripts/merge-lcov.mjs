#!/usr/bin/env node
// Deterministically merges multiple LCOV tracefiles by source path (SF),
// summing line/function/branch hit counts and recomputing LF/LH/FNF/FNH/BRF/BRH.
import { mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';

export function mergeLcovTexts(texts) {
  const files = new Map();

  for (const text of texts) {
    let current = null;
    for (const rawLine of text.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (!line) continue;

      if (line.startsWith('SF:')) {
        const sf = line.slice(3);
        current = files.get(sf);
        if (!current) {
          current = {
            sf,
            tn: '',
            functions: new Map(), // fnKey (name) -> lineNum
            fnHits: new Map(),    // name -> count
            lines: new Map(),     // lineNum -> { count, checksum }
            branches: new Map(),  // "line,block,branch" -> count ("-" or integer)
          };
          files.set(sf, current);
        }
        continue;
      }
      if (!current) continue;

      if (line === 'end_of_record') {
        current = null;
        continue;
      }
      if (line.startsWith('TN:')) {
        if (!current.tn) current.tn = line.slice(3);
      } else if (line.startsWith('FN:')) {
        const rest = line.slice(3);
        const comma = rest.indexOf(',');
        if (comma !== -1) {
          const fnLine = Number(rest.slice(0, comma));
          const fnName = rest.slice(comma + 1);
          if (!current.functions.has(fnName) || fnLine < current.functions.get(fnName)) {
            current.functions.set(fnName, fnLine);
          }
          if (!current.fnHits.has(fnName)) current.fnHits.set(fnName, 0);
        }
      } else if (line.startsWith('FNDA:')) {
        const rest = line.slice(5);
        const comma = rest.indexOf(',');
        if (comma !== -1) {
          const hits = Number(rest.slice(0, comma));
          const fnName = rest.slice(comma + 1);
          current.fnHits.set(fnName, (current.fnHits.get(fnName) ?? 0) + (Number.isFinite(hits) ? hits : 0));
          if (!current.functions.has(fnName)) current.functions.set(fnName, 0);
        }
      } else if (line.startsWith('DA:')) {
        const parts = line.slice(3).split(',');
        const lineNo = Number(parts[0]);
        const hits = Number(parts[1]);
        const checksum = parts[2];
        const prev = current.lines.get(lineNo);
        const addedHits = Number.isFinite(hits) ? hits : 0;
        if (!prev) {
          current.lines.set(lineNo, { count: addedHits, checksum });
        } else {
          prev.count += addedHits;
          if (!prev.checksum && checksum) prev.checksum = checksum;
        }
      } else if (line.startsWith('BRDA:')) {
        const parts = line.slice(5).split(',');
        if (parts.length >= 4) {
          const key = `${parts[0]},${parts[1]},${parts[2]}`;
          const takenRaw = parts[3];
          const prev = current.branches.get(key);
          if (takenRaw === '-') {
            if (prev === undefined) current.branches.set(key, '-');
          } else {
            const count = Number(takenRaw);
            const numeric = Number.isFinite(count) ? count : 0;
            if (prev === undefined || prev === '-') {
              current.branches.set(key, numeric);
            } else {
              current.branches.set(key, prev + numeric);
            }
          }
        }
      }
      // LF, LH, FNF, FNH, BRF, BRH are always recomputed from merged records.
    }
  }

  const out = [];
  for (const sf of [...files.keys()].sort()) {
    const rec = files.get(sf);
    if (rec.tn) out.push(`TN:${rec.tn}`);
    out.push(`SF:${sf}`);

    const fnEntries = [...rec.functions.entries()].sort((a, b) =>
      a[1] !== b[1] ? a[1] - b[1] : a[0].localeCompare(b[0]),
    );
    for (const [fnName, fnLine] of fnEntries) {
      out.push(`FN:${fnLine},${fnName}`);
    }
    let fnHitCount = 0;
    for (const [fnName] of fnEntries) {
      const hits = rec.fnHits.get(fnName) ?? 0;
      if (hits > 0) fnHitCount += 1;
      out.push(`FNDA:${hits},${fnName}`);
    }
    out.push(`FNF:${fnEntries.length}`);
    out.push(`FNH:${fnHitCount}`);

    const brEntries = [...rec.branches.entries()].sort((a, b) => {
      const [l1, bl1, br1] = a[0].split(',').map(Number);
      const [l2, bl2, br2] = b[0].split(',').map(Number);
      return l1 - l2 || bl1 - bl2 || br1 - br2;
    });
    let brHitCount = 0;
    for (const [key, taken] of brEntries) {
      if (typeof taken === 'number' && taken > 0) brHitCount += 1;
      out.push(`BRDA:${key},${taken}`);
    }
    if (brEntries.length > 0) {
      out.push(`BRF:${brEntries.length}`);
      out.push(`BRH:${brHitCount}`);
    }

    const daEntries = [...rec.lines.entries()].sort((a, b) => a[0] - b[0]);
    let lineHitCount = 0;
    for (const [lineNo, { count, checksum }] of daEntries) {
      if (count > 0) lineHitCount += 1;
      out.push(checksum ? `DA:${lineNo},${count},${checksum}` : `DA:${lineNo},${count}`);
    }
    out.push(`LF:${daEntries.length}`);
    out.push(`LH:${lineHitCount}`);
    out.push('end_of_record');
  }

  return `${out.join('\n')}\n`;
}

function main() {
  const args = process.argv.slice(2);
  let output = null;
  const inputs = [];
  for (let i = 0; i < args.length; i += 1) {
    if ((args[i] === '-o' || args[i] === '--output') && args[i + 1]) {
      output = args[++i];
    } else {
      inputs.push(args[i]);
    }
  }

  if (!output || inputs.length === 0) {
    console.error('Usage: node scripts/merge-lcov.mjs -o <out.lcov> <in1.lcov> [in2.lcov ...]');
    process.exit(2);
  }

  const texts = [];
  for (const input of inputs) {
    const st = statSync(input, { throwIfNoEntry: false });
    if (!st || !st.isFile() || st.size === 0) {
      console.error(`merge-lcov: missing or empty LCOV input: ${input}`);
      process.exit(1);
    }
    const content = readFileSync(input, 'utf8');
    if (!content.includes('SF:')) {
      console.error(`merge-lcov: LCOV input contains no SF records: ${input}`);
      process.exit(1);
    }
    texts.push(content);
  }

  const merged = mergeLcovTexts(texts);
  if (!merged.includes('SF:')) {
    console.error('merge-lcov: merged output has no SF records');
    process.exit(1);
  }
  mkdirSync(path.dirname(path.resolve(output)), { recursive: true });
  writeFileSync(output, merged, 'utf8');
  console.log(`merge-lcov: merged ${inputs.length} files into ${output}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
