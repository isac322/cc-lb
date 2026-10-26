import assert from "node:assert/strict";
import test from "node:test";

import { slugify } from "../src/slugify.js";

test("lowercases and joins words with a dash", () => {
  assert.equal(slugify("Hello World"), "hello-world");
});

test("collapses punctuation and surrounding whitespace", () => {
  assert.equal(slugify("  Hello,  World!  "), "hello-world");
});

test("strips accents", () => {
  assert.equal(slugify("Crème brûlée"), "creme-brulee");
});
