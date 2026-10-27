// Turns arbitrary titles into URL slugs such as "hello-world".
export function slugify(input) {
  return String(input)
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "-");
}
