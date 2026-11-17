/**
 * SQL over edw.vw_trade_analysis for the trades that swung a title.
 *
 * production_differential is side A's points minus side B's, so "50 or more
 * to the champion" depends on which side won the title. The test used to be
 * production_differential >= 50 on either side's title, which missed every
 * trade side B won the title on and counted side A's titles only. The
 * absolute value would count swings AWAY from the champion too (the champion
 * "got" players and a "+247.9" they actually gave up).
 */

/** The champion's points gained in the trade (negative if they lost the swing). */
export const CHAMPION_GAIN =
	'CASE WHEN team_a_champion = 1 THEN production_differential ' +
	'WHEN team_b_champion = 1 THEN -production_differential END';

/** One side won that season's title and gained 50+ more points in the trade. */
export const SWUNG_A_TITLE = `(${CHAMPION_GAIN}) >= 50`;
