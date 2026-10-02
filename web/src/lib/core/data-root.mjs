// Plain .mjs (same pattern as run-cli-support.mjs / tracker-table.mjs) so
// tests/lib/data-root.test.mjs can import it directly under Node. Import it with
// the .mjs extension included.

/**
 * Resolve the career-ops data root, matching the core's `path-resolver.mjs`
 * EXACTLY.
 *
 * WHY THIS EXISTS — the divergence it closes:
 *
 * `web/`'s resolver honored only `CAREER_OPS_ROOT`, then fell back to the
 * parent directory. The core honors `CAREER_OPS_ROOT` → `CAREER_OPS_DATA_DIR`
 * → a `.career-ops-data` marker file → its own directory (AGENTS.md, "Path
 * Resolution Override & Precedence"). A user with a marker file therefore had
 * the CLI and the web app reading two different data roots, with nothing
 * reporting the disagreement.
 *
 * THE SUBTLE HALF — the resolution BASE, not just the order:
 *
 * `path-resolver.mjs` resolves relative values against `__dirname`, i.e. the
 * CORE CHECKOUT. `web/` runs with `process.cwd() === <core>/web`, so resolving
 * a relative value against the cwd points one directory too deep. A marker
 * containing `../shared-data` would mean `<core>/../shared-data` to the CLI and
 * `<core>/web/../shared-data` — i.e. `<core>/shared-data` — to the web app.
 * Same string, two directories, no error. `coreRoot` is therefore a required
 * argument here: it is the one value that must agree with the core's
 * `__dirname`, and making it explicit keeps it from being silently re-derived.
 *
 * NOT IMPLEMENTED BY IMPORTING THE CORE, deliberately: `path-resolver.mjs`
 * derives `__dirname` from `import.meta.url`. Bundled into the Next server
 * output, that URL becomes the bundle's location rather than the checkout's, so
 * importing it would resolve relative values against `.next/server/...`. The
 * import would look more DRY and be silently wrong. `tests/lib/data-root.test.mjs`
 * pins the two implementations together instead.
 *
 * Absolute env values and marker contents are unaffected — `path.resolve` returns
 * an absolute input unchanged — so every existing install keeps its current root.
 *
 * @param {string} coreRoot Absolute path to the career-ops checkout (path-resolver's `__dirname`).
 * @param {(p: string) => string | null} readMarker Reads a file, or null when absent/unreadable.
 * @param {NodeJS.ProcessEnv} env
 * @param {(...parts: string[]) => string} resolve `path.resolve`
 * @param {(...parts: string[]) => string} join `path.join`
 * @returns {string} Absolute data root.
 */
export function resolveDataRoot(coreRoot, readMarker, env, resolve, join) {
  // Precedence order is the core's, including CAREER_OPS_ROOT winning over
  // CAREER_OPS_DATA_DIR when both are set.
  const fromEnv = env.CAREER_OPS_ROOT?.trim() || env.CAREER_OPS_DATA_DIR?.trim();
  if (fromEnv) return resolve(coreRoot, fromEnv);

  const marker = readMarker(join(coreRoot, ".career-ops-data"));
  if (marker) {
    const content = marker.trim();
    // An empty or whitespace-only marker is not a root — the core falls through
    // to its default rather than resolving to `coreRoot` by accident, and a
    // disagreement here would be invisible.
    if (content) return resolve(coreRoot, content);
  }

  return coreRoot;
}

/**
 * Resolve the active tracker with the same precedence and canonicalization as
 * core/path-resolver.mjs. Kept in this plain module because server-side web
 * reads are synchronous, while the core checkout is selected at runtime and
 * cannot be bundled as a static Next dependency. The parity test in
 * tests/lib/data-root.test.mjs pins this mirror to the core implementation.
 *
 * @param {string} rootDir Resolved career-ops data root.
 * @param {NodeJS.ProcessEnv} env
 * @param {(p: string) => boolean} exists `fs.existsSync`
 * @param {(p: string) => string} resolve `path.resolve`
 * @param {(...parts: string[]) => string} join `path.join`
 * @param {(p: string) => string} realpath `fs.realpathSync`
 * @returns {string} Canonical absolute tracker path.
 */
export function resolveTrackerPath(rootDir, env, exists, resolve, join, realpath) {
  const override = env.CAREER_OPS_TRACKER?.trim();
  const raw = override
    ? override
    : exists(join(rootDir, "data/applications.md"))
      ? join(rootDir, "data/applications.md")
      : join(rootDir, "applications.md");
  const absolute = resolve(raw);
  try {
    return realpath(absolute);
  } catch {
    return absolute;
  }
}
