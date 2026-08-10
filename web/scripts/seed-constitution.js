#!/usr/bin/env node
/**
 * Seed version 1 of the constitution from data/constitution-v1.json.
 *
 * Idempotent: a no-op if version 1 already exists. Every later version is
 * produced by an amendment passing, never by re-running this.
 *
 * Usage: DATABASE_URL=postgres://... node scripts/seed-constitution.js
 */

import fs from 'node:fs';
import postgres from 'postgres';
import { customAlphabet } from 'nanoid';

const url = process.env.DATABASE_URL;
if (!url) {
	console.error('DATABASE_URL is not set.');
	process.exit(1);
}

const isLocal = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
const sql = postgres(url, { ssl: isLocal ? false : 'require', max: 1, prepare: false });

const nanoid = customAlphabet('0123456789abcdefghijklmnopqrstuvwxyz', 12);

const { sections } = JSON.parse(
	fs.readFileSync(new URL('../data/constitution-v1.json', import.meta.url), 'utf8')
);

const ROMAN = [
	[1000, 'm'], [900, 'cm'], [500, 'd'], [400, 'cd'], [100, 'c'], [90, 'xc'],
	[50, 'l'], [40, 'xl'], [10, 'x'], [9, 'ix'], [5, 'v'], [4, 'iv'], [1, 'i']
];

function toRoman(n) {
	let rest = n;
	let out = '';
	for (const [value, numeral] of ROMAN) {
		while (rest >= value) {
			out += numeral;
			rest -= value;
		}
	}
	return out;
}

/** Mirrors labelFor() in src/lib/server/constitution/labels.ts. */
function labelFor(depth, ordinal) {
	if (depth <= 0) return toRoman(ordinal).toUpperCase();
	if (depth === 1) return String.fromCharCode(96 + ordinal);
	return toRoman(ordinal);
}

try {
	const [existing] = await sql`select version_key from app.constitution_version where version_no = 1`;
	if (existing) {
		console.log('Version 1 already seeded — nothing to do.');
		process.exit(0);
	}

	let sectionCount = 0;
	let clauseCount = 0;

	await sql.begin(async (tx) => {
		const [version] = await tx`
			insert into app.constitution_version (version_no, effective_at, note)
			values (1, now(), 'Original constitution as of January 2025')
			returning version_key`;

		for (const section of sections) {
			const [row] = await tx`
				insert into app.constitution_section
					(version_key, section_id, title, kind, icon, sort_order)
				values (${version.version_key}, ${section.sectionId}, ${section.title},
				        ${section.kind}, ${section.icon}, ${section.sortOrder})
				returning section_key`;
			sectionCount++;

			// Depth-first, parents before children, so parent_key is always known.
			const insertClauses = async (nodes, parentKey, depth) => {
				for (const [index, node] of nodes.entries()) {
					const ordinal = index + 1;
					const [clause] = await tx`
						insert into app.constitution_clause
							(section_key, parent_key, clause_uid, depth, sort_order, label, body)
						values (${row.section_key}, ${parentKey}, ${nanoid()}, ${depth},
						        ${ordinal}, ${labelFor(depth, ordinal)}, ${node.body})
						returning clause_key`;
					clauseCount++;

					if (node.children?.length) {
						await insertClauses(node.children, clause.clause_key, depth + 1);
					}
				}
			};

			await insertClauses(section.clauses, null, 0);
		}
	});

	console.log(`Seeded constitution v1: ${sectionCount} sections, ${clauseCount} clauses.`);
} finally {
	await sql.end();
}
