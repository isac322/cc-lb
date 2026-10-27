# stats fixture

Tiny scratch project for cc-lb client QA.

`src/stats.js` exports small numeric helpers.

Contract:

- `sum(values)` returns the total of a numeric array; an empty array sums to `0`.
- `mean(values)` returns the arithmetic mean. It must throw a `RangeError` with the
  message `mean of empty list` when `values` is empty (it must never return `NaN`).

Run the tests with `node --test` from the project root.
