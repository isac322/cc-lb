const SERVER_SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z.-]+))?$/;

export function parseServerSemVer(value) {
  const match = SERVER_SEMVER.exec(value);
  if (!match) throw new Error(`invalid server SemVer: ${value}`);

  const prerelease = match[4] ?? "";
  if (
    prerelease &&
    prerelease
      .split(".")
      .some(
        (identifier) =>
          identifier === "" ||
          (/^\d+$/.test(identifier) &&
            identifier.length > 1 &&
            identifier.startsWith("0")),
      )
  ) {
    throw new Error(`invalid server SemVer prerelease: ${value}`);
  }

  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    prerelease,
  };
}

function compare(left, right) {
  return (
    left.major - right.major ||
    left.minor - right.minor ||
    left.patch - right.patch
  );
}

export function releaseAliases(version, publishedTags) {
  const current = parseServerSemVer(version);
  if (current.prerelease) return [];

  const published = publishedTags.flatMap((tag) => {
    if (!tag.startsWith("cc-lb-v")) return [];
    try {
      const parsed = parseServerSemVer(tag.slice("cc-lb-v".length));
      return parsed.prerelease ? [] : [parsed];
    } catch {
      return [];
    }
  });

  const aliases = [];
  if (
    !published.some(
      (item) =>
        item.major === current.major &&
        item.minor === current.minor &&
        compare(item, current) > 0,
    )
  ) {
    aliases.push(`${current.major}.${current.minor}`);
  }
  if (
    !published.some(
      (item) => item.major === current.major && compare(item, current) > 0,
    )
  ) {
    aliases.push(String(current.major));
  }
  return aliases;
}
