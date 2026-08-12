import { describe, expect, it } from 'vitest';
import { findMentionSpans, findMentions } from './mentions';
import type { RosterMember } from './types';

// The real roster. "Gabe" and "Gabe the Younger" both being on it is the reason
// longest-first matching exists.
const ROSTER: RosterMember[] = [
	{ managerKey: 14, name: 'Luke S', displayName: 'Luke S', profileImageUrl: null },
	{ managerKey: 5, name: 'Craig', displayName: 'Craig', profileImageUrl: null },
	{ managerKey: 8, name: 'Gabe Flores', displayName: 'Gabe Flores', profileImageUrl: null },
	{ managerKey: 9, name: 'Gabe the Younger', displayName: 'Gabe the Younger', profileImageUrl: null },
	{ managerKey: 15, name: 'Nick', displayName: 'Nick', profileImageUrl: null },
	{ managerKey: 18, name: 'Trevor', displayName: 'Trevor', profileImageUrl: null }
];

describe('findMentions', () => {
	it('resolves a plain mention', () => {
		expect(findMentions('hey @Trevor you around?', ROSTER)).toEqual([18]);
	});

	it('prefers the longest matching name', () => {
		// Shortest-first would resolve this to Gabe Flores, or to nobody.
		expect(findMentions('@Gabe the Younger is on the clock', ROSTER)).toEqual([9]);
		expect(findMentions('@Gabe Flores is not', ROSTER)).toEqual([8]);
	});

	it('is case-insensitive', () => {
		expect(findMentions('@trevor and @NICK', ROSTER)).toEqual([18, 15]);
	});

	it('de-duplicates and preserves first-appearance order', () => {
		expect(findMentions('@Nick @Trevor @Nick', ROSTER)).toEqual([15, 18]);
	});

	it('does not fire inside an email address', () => {
		expect(findMentions('mail me at luke@Nick.com', ROSTER)).toEqual([]);
	});

	it('does not match a longer word that merely starts with a name', () => {
		expect(findMentions('@Nickel and dime', ROSTER)).toEqual([]);
	});

	it('ignores a doubled @', () => {
		expect(findMentions('@@Trevor', ROSTER)).toEqual([]);
	});

	it('returns nothing for text with no @ at all', () => {
		expect(findMentions('no mentions here', ROSTER)).toEqual([]);
	});

	it('returns nothing against an empty roster', () => {
		expect(findMentions('@Trevor', [])).toEqual([]);
	});
});

describe('findMentionSpans', () => {
	it('reports the exact character range of each mention', () => {
		const content = 'ping @Nick now';
		const spans = findMentionSpans(content, ROSTER);
		expect(spans).toHaveLength(1);
		expect(content.slice(spans[0].start, spans[0].end)).toBe('@Nick');
		expect(spans[0].managerKey).toBe(15);
	});

	it('spans the full name for a multi-word mention', () => {
		const content = 'cc @Gabe the Younger about it';
		const spans = findMentionSpans(content, ROSTER);
		expect(content.slice(spans[0].start, spans[0].end)).toBe('@Gabe the Younger');
	});
});
