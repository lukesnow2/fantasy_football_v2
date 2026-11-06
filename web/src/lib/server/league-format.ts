import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';

/**
 * A season's format: how long the regular season is, how many teams make the
 * playoffs, when trading closes.
 *
 * Read from the league's Yahoo settings on edw.dim_league (migration 011).
 * Finished seasons were backfilled from their brackets; the season being
 * played fills in on its next pipeline load. Until then, and for a warehouse
 * the pipeline hasn't migrated yet, the format is inferred: the regular-season
 * length of the most recent season with a playoff game, and six teams.
 */
export interface LeagueFormat {
	season: number;
	/** Weeks in the regular season (the playoffs start the week after). */
	regularSeasonWeeks: number;
	playoffStartWeek: number;
	playoffTeams: number;
	/** ISO date trading closes, when Yahoo supplied it. */
	tradeEndDate: string | null;
	source: 'league-settings' | 'inferred';
}

export const DEFAULT_PLAYOFF_TEAMS = 6;
const DEFAULT_REGULAR_SEASON_WEEKS = 14;

type Row = Record<string, unknown>;

const positive = (value: unknown): number | null => {
	const n = Number(value);
	return Number.isInteger(n) && n > 0 ? n : null;
};

const isoDate = (value: unknown): string | null =>
	typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null;

/**
 * The format from a dim_league row (stored settings, any of which may be
 * missing) plus the inferred regular-season length. Pure, for testing.
 */
export function resolveLeagueFormat(
	season: number,
	stored: Row | undefined,
	inferredRegularSeasonWeeks: number | null
): LeagueFormat {
	const start = positive(stored?.playoffStartWeek);
	const teams = positive(stored?.numPlayoffTeams);
	const regularSeasonWeeks = start
		? start - 1
		: (inferredRegularSeasonWeeks ?? DEFAULT_REGULAR_SEASON_WEEKS);
	return {
		season,
		regularSeasonWeeks,
		playoffStartWeek: regularSeasonWeeks + 1,
		playoffTeams: teams ?? DEFAULT_PLAYOFF_TEAMS,
		tradeEndDate: isoDate(stored?.tradeEndDate),
		source: start && teams ? 'league-settings' : 'inferred'
	};
}

/** Whether trading has closed: only claimed when the deadline is known. */
export function tradeDeadlinePassed(
	format: LeagueFormat,
	today: Date = new Date()
): boolean | null {
	if (!format.tradeEndDate) return null;
	// Yahoo's deadline is a US calendar date and the day itself is still open.
	// Compare against the date in the westernmost US zone, so it's never
	// called passed while it's still the deadline day anywhere in the league
	// (UTC rolls over at 5pm Pacific).
	const localDate = new Intl.DateTimeFormat('en-CA', {
		timeZone: 'America/Los_Angeles',
		year: 'numeric',
		month: '2-digit',
		day: '2-digit'
	}).format(today);
	return localDate > format.tradeEndDate;
}

export async function getLeagueFormat(season: number): Promise<LeagueFormat> {
	const [storedRows, inferredRows] = await Promise.all([
		// Read through to_jsonb so this still runs against a warehouse the
		// pipeline hasn't migrated yet: a missing column reads as null.
		db.execute(sql`
			SELECT to_jsonb(dl) ->> 'playoff_start_week' AS playoff_start_week,
			       to_jsonb(dl) ->> 'num_playoff_teams'  AS num_playoff_teams,
			       to_jsonb(dl) ->> 'trade_end_date'     AS trade_end_date
			FROM edw.dim_league dl
			WHERE dl.season_year = ${season}
			ORDER BY dl.league_key DESC
			LIMIT 1
		`),
		db.execute(sql`
			SELECT MAX(dw.week_number) AS weeks
			FROM edw.fact_matchup fm
			JOIN edw.dim_week dw ON dw.week_key = fm.week_key
			WHERE fm.season_year = (
				SELECT MAX(season_year) FROM edw.fact_matchup
				WHERE is_playoffs AND season_year <= ${season}
			)
			AND NOT fm.is_playoffs AND NOT fm.is_consolation
		`)
	]);
	const stored = (Array.from(storedRows) as Row[])[0];
	const inferred = positive((Array.from(inferredRows) as Row[])[0]?.weeks);
	return resolveLeagueFormat(season, stored, inferred);
}
