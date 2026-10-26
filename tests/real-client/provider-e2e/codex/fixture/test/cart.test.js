import assert from "node:assert/strict";
import { test } from "node:test";
import { applyDiscount, subtotal } from "../src/cart.js";

test("subtotal multiplies price by quantity", () => {
  assert.equal(
    subtotal([
      { price: 3, quantity: 2 },
      { price: 5, quantity: 1 },
    ]),
    11,
  );
});

test("subtotal of an empty cart is zero", () => {
  assert.equal(subtotal([]), 0);
});

test("applyDiscount takes a percentage off", () => {
  assert.equal(applyDiscount(200, 25), 150);
});
