import assert from "node:assert/strict";
import test from "node:test";

import { releaseAliases } from "./release_aliases.mjs";

test("prerelease never updates moving aliases", () => {
  assert.deepEqual(releaseAliases("2.0.0-rc.1", []), []);
});

test("new patch updates both aliases", () => {
  assert.deepEqual(releaseAliases("1.2.3", ["cc-lb-v1.2.2"]), ["1.2", "1"]);
});

test("older patch updates neither alias", () => {
  assert.deepEqual(releaseAliases("1.2.3", ["cc-lb-v1.2.4"]), []);
});

test("older minor updates only the minor alias", () => {
  assert.deepEqual(releaseAliases("1.2.3", ["cc-lb-v1.3.0"]), ["1.2"]);
});

test("new major updates both aliases", () => {
  assert.deepEqual(releaseAliases("2.0.0", ["cc-lb-v1.9.9"]), ["2.0", "2"]);
});

test("later prerelease does not block stable aliases", () => {
  assert.deepEqual(releaseAliases("1.2.3", ["cc-lb-v1.2.4-rc.1"]), ["1.2", "1"]);
});

test("unrelated and malformed tags are ignored", () => {
  assert.deepEqual(
    releaseAliases("1.2.3", ["v9.9.9", "cc-lb-vbogus", "cc-lb-v1.2.2"]),
    ["1.2", "1"],
  );
});
