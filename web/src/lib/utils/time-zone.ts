/**
 * Which time zone to write dates in for the person reading the page.
 *
 * The server renders first and runs in UTC, so it has to be told. In order:
 * the browser's own zone, which the page saves in the `tz` cookie after its
 * first load (exact); then Vercel's guess from the visitor's IP address, the
 * `x-vercel-ip-timezone` request header (right on the first visit, wrong
 * behind a VPN); then the league's home zone, Mountain time.
 */
export const LEAGUE_TIME_ZONE = 'America/Denver';
export const TIME_ZONE_COOKIE = 'tz';

/** True for an IANA zone the runtime knows, such as "America/Los_Angeles". */
export function isTimeZone(value: string | null | undefined): value is string {
	if (!value || value.length > 64) return false;
	try {
		new Intl.DateTimeFormat('en-US', { timeZone: value });
		return true;
	} catch {
		return false;
	}
}

export function resolveTimeZone(
	cookie: string | null | undefined,
	header: string | null | undefined
): string {
	if (isTimeZone(cookie)) return cookie;
	if (isTimeZone(header)) return header;
	return LEAGUE_TIME_ZONE;
}

/** The browser's own zone, or null where it can't say (and on the server). */
export function browserTimeZone(): string | null {
	if (typeof Intl === 'undefined') return null;
	const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
	return isTimeZone(zone) ? zone : null;
}

/** The zone for a server render, from the request's cookie and Vercel's header. */
export function requestTimeZone(event: {
	cookies: { get(name: string): string | undefined };
	request: Request;
}): string {
	return resolveTimeZone(
		event.cookies.get(TIME_ZONE_COOKIE),
		event.request.headers.get('x-vercel-ip-timezone')
	);
}

/**
 * "Nov 9, 2026" in the given zone, or null when there's no date to show (no
 * value, or one that doesn't parse). Naming the zone is what keeps the server
 * render and the browser on the same day for one timestamp.
 */
export function formatShortDate(
	value: string | Date | null | undefined,
	timeZone: string = LEAGUE_TIME_ZONE
): string | null {
	if (value == null || value === '') return null;
	const date = value instanceof Date ? value : new Date(value);
	if (Number.isNaN(date.getTime())) return null;
	return date.toLocaleDateString('en-US', {
		month: 'short',
		day: 'numeric',
		year: 'numeric',
		timeZone
	});
}
