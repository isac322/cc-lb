// Hidden fixture check (not copied into the client project). Run by the driver after each turn:
//   node <this file> <turn> <projectDir>
// Prints one JSON line {turn, checks:[{name,ok,detail}], ok}.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [turn, project] = [Number(process.argv[2]), process.argv[3]];
const checks = [];
const check = (name, fn) => {
  try {
    const r = fn();
    checks.push({ name, ok: r === true, detail: r === true ? null : String(r) });
  } catch (e) {
    checks.push({ name, ok: false, detail: `${e?.name}: ${e?.message}` });
  }
};
const throwsRange = (fn) => {
  try {
    fn();
    return "did not throw";
  } catch (e) {
    return e instanceof RangeError ? true : `threw ${e?.name}`;
  }
};

const mod = await import(`${pathToFileURL(join(project, "src", "cart.js")).href}?t=${Date.now()}`);
const testSrc = readFileSync(join(project, "test", "cart.test.js"), "utf8");
const ORIGINAL_TESTS = ["subtotal multiplies price by quantity", "subtotal of an empty cart is zero", "applyDiscount takes a percentage off"];

// Turn 1 behavior (must keep holding after turn 2).
check("subtotal multiplies price by quantity", () => mod.subtotal([{ price: 3, quantity: 2 }, { price: 5, quantity: 1 }]) === 11 || "wrong total");
check("subtotal empty cart", () => mod.subtotal([]) === 0 || "nonzero");
check("applyDiscount 25% of 200", () => mod.applyDiscount(200, 25) === 150 || "wrong discount");
check("original tests kept", () => ORIGINAL_TESTS.every((n) => testSrc.includes(n)) || "an original test was removed or renamed");

if (turn >= 2) {
  check("applyDiscount 0% boundary valid", () => mod.applyDiscount(200, 0) === 200 || "0 not accepted as no discount");
  check("applyDiscount 100% boundary valid", () => mod.applyDiscount(200, 100) === 0 || "100 not accepted as full discount");
  check("applyDiscount below 0 throws RangeError", () => throwsRange(() => mod.applyDiscount(200, -1)));
  check("applyDiscount above 100 throws RangeError", () => throwsRange(() => mod.applyDiscount(200, 101)));
  check("new tests reference RangeError", () => /RangeError/.test(testSrc) || "test file never mentions RangeError");
  check("new tests cover below-0 and above-100", () => (/-\s*\d/.test(testSrc) && /\b(10[1-9]|1[1-9]\d|[2-9]\d\d)\b/.test(testSrc)) || "no negative or >100 percent literal in tests");
}

console.log(JSON.stringify({ turn, checks, ok: checks.every((c) => c.ok) }));
