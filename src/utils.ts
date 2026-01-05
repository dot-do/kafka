/**
 * Utility functions for Kafdo
 */

/**
 * Safely parse JSON with error handling.
 * Returns the parsed value on success, or the fallback value on failure.
 *
 * @param jsonString - The JSON string to parse
 * @param fallback - The fallback value to return if parsing fails
 * @returns The parsed JSON value or the fallback
 */
export function safeJsonParse<T>(jsonString: string, fallback: T): T {
  try {
    return JSON.parse(jsonString) as T
  } catch {
    return fallback
  }
}
