/**
 * Parse an application or follow-up identifier without prefix coercion.
 * @param {unknown} value
 * @returns {number | null}
 */
export function parseFollowupId(value) {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) && value > 0 ? value : null;
  }
  if (typeof value !== "string") return null;

  const trimmed = value.trim();
  if (!/^[1-9]\d*$/.test(trimmed)) return null;

  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

const FOLLOWUP_PIN_RE = /^\s*-\s+next\s+#([1-9]\d*)\s/i;

/**
 * Whether a follow-up log line pins the exact application number.
 * @param {string} line
 * @param {number} appNum
 * @returns {boolean}
 */
export function isFollowupPinForApp(line, appNum) {
  const match = FOLLOWUP_PIN_RE.exec(line);
  return match !== null && Number(match[1]) === appNum;
}
