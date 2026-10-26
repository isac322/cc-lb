import assert from "node:assert/strict";
import test from "node:test";

import { mean, sum } from "../src/stats.js";

test("sum adds values", () => {
  assert.equal(sum([1, 2, 3]), 6);
});

test("sum of empty list is zero", () => {
  assert.equal(sum([]), 0);
});

test("mean averages values", () => {
  assert.equal(mean([2, 4, 6]), 4);
});

test("mean of empty list throws RangeError", () => {
  assert.throws(() => mean([]), {
    name: "RangeError",
    message: "mean of empty list",
  });
});
