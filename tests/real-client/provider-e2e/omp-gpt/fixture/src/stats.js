export function sum(values) {
  let total = 0;
  for (const value of values) {
    total += value;
  }
  return total;
}

export function mean(values) {
  return sum(values) / values.length;
}
