// Tiny shopping-cart helpers used by the cc-lb Codex QA fixture.

/**
 * Sum of price * quantity for every line item.
 * @param {{ price: number, quantity: number }[]} items
 */
export function subtotal(items) {
  let total = 0;
  for (const item of items) {
    total += item.price;
  }
  return total;
}

/**
 * Apply a percentage discount to a total.
 * @param {number} total
 * @param {number} percent
 */
export function applyDiscount(total, percent) {
  return total - (total * percent) / 100;
}
