#!/usr/bin/env node
/**
 * One-time extraction of the constitution text out of the Svelte component and
 * into data/constitution-v1.json.
 *
 * Run once, commit the JSON, then the seed script depends on data rather than on
 * parsing a component that is about to be rewritten. Kept in the repo so the
 * provenance of the seed file is auditable — this is where the text came from.
 *
 * Usage: node scripts/extract-constitution-v1.js
 */

import fs from 'node:fs';

const SOURCE = new URL('../src/routes/constitution/+page.svelte', import.meta.url);
const OUT = new URL('../data/constitution-v1.json', import.meta.url);

const source = fs.readFileSync(SOURCE, 'utf8');

/** Pull a top-level `const <name> = [ ... ];` array literal out of the script block. */
function extractArray(name) {
	const start = source.indexOf(`const ${name} = [`);
	if (start === -1) throw new Error(`Could not find "const ${name} = ["`);

	const open = source.indexOf('[', start);
	let depth = 0;
	let inString = null;

	for (let i = open; i < source.length; i++) {
		const ch = source[i];
		const prev = source[i - 1];

		if (inString) {
			if (ch === inString && prev !== '\\') inString = null;
			continue;
		}
		if (ch === '"' || ch === "'" || ch === '`') {
			inString = ch;
			continue;
		}
		if (ch === '[') depth++;
		else if (ch === ']') {
			depth--;
			if (depth === 0) return source.slice(open, i + 1);
		}
	}
	throw new Error(`Unbalanced array literal for ${name}`);
}

/**
 * Evaluate an object-literal array that references Svelte icon components.
 * The icons are bound to their own names so `icon: Trophy` evaluates to the
 * string "Trophy", which is what we store — resolved back to a component
 * client-side.
 */
function evaluateLiteral(literal) {
	const iconNames = [
		'BookOpen', 'FileText', 'Users', 'Trophy', 'Calendar', 'Target', 'Zap',
		'ChevronDown', 'ChevronRight', 'Edit3', 'Plus', 'Vote', 'MessageSquare',
		'X', 'Check', 'Clock', 'Trash2'
	];
	const args = iconNames.join(', ');
	const values = iconNames.map((n) => JSON.stringify(n)).join(', ');
	// eslint-disable-next-line no-new-func
	return new Function(args, `return ${literal};`)(...JSON.parse(`[${values}]`));
}

/**
 * Depth comes from leading spaces, not from the numeral.
 *
 * "I." at column 0 is depth 0 while "i." at column 8 is depth 2 — the token
 * alone cannot tell them apart, which matters for Article 1's Snow Rule
 * sub-sub-clauses.
 */
function parseClause(raw) {
	const leading = raw.match(/^ */)[0].length;
	const depth = leading >= 8 ? 2 : leading >= 4 ? 1 : 0;
	const trimmed = raw.trim();

	// Strip the stored numeral — labels are regenerated from position so that
	// deleting clause II does not leave the article reading I, III, IV.
	const withoutLabel = trimmed.replace(/^([IVXivx]+|[a-z])\.\s*/, '');

	return { depth, body: withoutLabel || trimmed };
}

/** Fold a flat, indentation-ordered list into a tree. */
function nest(flat) {
	const roots = [];
	const stack = [];

	for (const item of flat) {
		const node = { depth: item.depth, body: item.body, children: [] };
		while (stack.length > 0 && stack[stack.length - 1].depth >= node.depth) stack.pop();

		if (stack.length === 0) roots.push(node);
		else stack[stack.length - 1].children.push(node);

		stack.push(node);
	}
	return roots;
}

const sections = evaluateLiteral(extractArray('constitutionSections')).map((s, index) => ({
	sectionId: s.id,
	title: s.title,
	kind: 'article',
	icon: s.icon,
	sortOrder: index + 1,
	clauses: nest(s.content.map(parseClause))
}));

// Appendix 1 becomes an ordinary section: each scoring category is a depth-0
// clause with its rules as depth-1 children. One uniform tree means the same
// edit/add/delete machinery works on scoring with no special case — which is
// what fixes "Appendix 1 proposals silently do nothing" structurally rather
// than by patching a lookup.
const scoring = evaluateLiteral(extractArray('scoringSystem'));
sections.push({
	sectionId: 'appendix1',
	title: 'Appendix 1: Scoring System',
	kind: 'appendix',
	icon: 'Target',
	sortOrder: sections.length + 1,
	clauses: scoring.map((category) => ({
		depth: 0,
		body: category.category,
		children: category.rules.map((rule) => ({ depth: 1, body: rule, children: [] }))
	}))
});

const countClauses = (nodes) =>
	nodes.reduce((n, c) => n + 1 + countClauses(c.children), 0);

fs.writeFileSync(
	OUT,
	JSON.stringify(
		{
			_comment:
				'Extracted verbatim from constitution/+page.svelte by scripts/extract-constitution-v1.js. This is version 1 of the constitution; every later version is produced by an amendment passing.',
			sections
		},
		null,
		'\t'
	) + '\n'
);

console.log(`Wrote ${sections.length} sections, ${countClauses(sections.flatMap((s) => s.clauses))} clauses.`);
for (const s of sections) {
	console.log(`  ${s.sectionId.padEnd(10)} ${countClauses(s.clauses).toString().padStart(3)} clauses  ${s.title}`);
}
