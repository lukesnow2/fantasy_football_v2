/**
 * Hall of Fame rules shared by its page (WP7) and the manager profiles (WP6).
 */

/**
 * The tier a Hall of Fame rank falls in, by the same cut-offs the Hall of
 * Fame API groups its tiers by: 1 to 3 Elite, 4 to 6 Hall of Fame, 7 to 10
 * Hall of Very Good, 11 to 15 Solid Contributor, then Developing Legacy.
 */
export function hofTier(rank: number): string {
	if (rank <= 3) return 'Hall of Fame Elite';
	if (rank <= 6) return 'Hall of Fame';
	if (rank <= 10) return 'Hall of Very Good';
	if (rank <= 15) return 'Solid Contributor';
	return 'Developing Legacy';
}
