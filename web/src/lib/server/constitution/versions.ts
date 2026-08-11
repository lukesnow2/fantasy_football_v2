import { asc, desc, eq, lte, sql } from 'drizzle-orm';
import { nanoid } from 'nanoid';
import { db, type Tx } from '$lib/server/db';
import {
	constitutionClause,
	constitutionSection,
	constitutionVersion
} from '$lib/server/db/schema';
import { labelFor } from './labels';

export interface ClauseNode {
	clauseKey: number;
	clauseUid: string;
	parentKey: number | null;
	depth: number;
	sortOrder: number;
	label: string;
	body: string;
	children: ClauseNode[];
}

export interface SectionView {
	sectionKey: number;
	sectionId: string;
	title: string;
	kind: string;
	icon: string | null;
	sortOrder: number;
	clauses: ClauseNode[];
}

/**
 * The version in force right now.
 *
 * "Newest whose effective_at has arrived" rather than an is_current flag: there
 * is no invariant to keep in sync, and a proposal that passes today with effect
 * from the 2027 season simply is not current yet.
 */
export async function getCurrentVersion(tx: Tx = db, now: Date = new Date()) {
	// Ordered by effective date first, version number only as a tiebreak.
	// Ordering by versionNo alone diverges from effective order the moment one
	// amendment is dated further out than another, and the later-numbered but
	// earlier-dated version would win.
	const [version] = await tx
		.select()
		.from(constitutionVersion)
		.where(lte(constitutionVersion.effectiveAt, now))
		.orderBy(desc(constitutionVersion.effectiveAt), desc(constitutionVersion.versionNo))
		.limit(1);

	return version ?? null;
}

/**
 * The newest version in the chain, in force or not.
 *
 * This is what an amendment must be built on. Cloning from the *currently
 * effective* version instead loses every amendment queued ahead of it: with the
 * form defaulting effectiveSeason to next year, two proposals passing in the
 * same season both clone v1, and whichever lands second silently drops the
 * first one's edit while still recording an amendment row and mailing the
 * league that it passed.
 */
export async function getLatestVersion(tx: Tx = db) {
	const [version] = await tx
		.select()
		.from(constitutionVersion)
		.orderBy(desc(constitutionVersion.versionNo))
		.limit(1);

	return version ?? null;
}

/** Versions passed but not yet in force, for the "coming next season" section. */
export async function getPendingVersions(tx: Tx = db, now: Date = new Date()) {
	return tx
		.select()
		.from(constitutionVersion)
		.where(sql`${constitutionVersion.effectiveAt} > ${now}`)
		.orderBy(asc(constitutionVersion.effectiveAt));
}

/** Load one version as a nested tree, ready to render. */
export async function loadVersionTree(tx: Tx, versionKey: number): Promise<SectionView[]> {
	const sections = await tx
		.select()
		.from(constitutionSection)
		.where(eq(constitutionSection.versionKey, versionKey))
		.orderBy(asc(constitutionSection.sortOrder));

	if (sections.length === 0) return [];

	const clauses = await tx
		.select()
		.from(constitutionClause)
		.innerJoin(
			constitutionSection,
			eq(constitutionClause.sectionKey, constitutionSection.sectionKey)
		)
		.where(eq(constitutionSection.versionKey, versionKey))
		.orderBy(asc(constitutionClause.sortOrder));

	const bySection = new Map<number, ClauseNode[]>();
	const byKey = new Map<number, ClauseNode>();

	for (const row of clauses) {
		const c = row.constitution_clause;
		const node: ClauseNode = {
			clauseKey: c.clauseKey,
			clauseUid: c.clauseUid,
			parentKey: c.parentKey,
			depth: c.depth,
			sortOrder: c.sortOrder,
			label: c.label,
			body: c.body,
			children: []
		};
		byKey.set(node.clauseKey, node);
		if (!bySection.has(c.sectionKey)) bySection.set(c.sectionKey, []);
		bySection.get(c.sectionKey)!.push(node);
	}

	// Second pass so a child can attach to a parent that appeared later.
	for (const node of byKey.values()) {
		if (node.parentKey != null) {
			byKey.get(node.parentKey)?.children.push(node);
		}
	}

	return sections.map((s) => ({
		sectionKey: s.sectionKey,
		sectionId: s.sectionId,
		title: s.title,
		kind: s.kind,
		icon: s.icon,
		sortOrder: s.sortOrder,
		clauses: (bySection.get(s.sectionKey) ?? [])
			.filter((c) => c.parentKey == null)
			.sort((a, b) => a.sortOrder - b.sortOrder)
	}));
}

/**
 * Copy an entire version into a new one.
 *
 * clause_uid is carried across verbatim — it is the only handle that survives a
 * clone, and therefore the only thing a proposal can safely target. parent_key
 * has to be remapped through the returned uid->newKey map because the serial
 * keys are all freshly assigned.
 */
export async function cloneVersion(
	tx: Tx,
	fromVersionKey: number,
	options: { effectiveAt: Date; note: string; amendmentKey?: number | null }
): Promise<{ versionKey: number; versionNo: number; clauseKeyByUid: Map<string, number> }> {
	const [{ maxNo }] = await tx
		.select({ maxNo: sql<number>`coalesce(max(${constitutionVersion.versionNo}), 0)::int` })
		.from(constitutionVersion);

	const [created] = await tx
		.insert(constitutionVersion)
		.values({
			versionNo: maxNo + 1,
			effectiveAt: options.effectiveAt,
			note: options.note,
			amendmentKey: options.amendmentKey ?? null
		})
		.returning();

	const sections = await tx
		.select()
		.from(constitutionSection)
		.where(eq(constitutionSection.versionKey, fromVersionKey))
		.orderBy(asc(constitutionSection.sortOrder));

	const clauseKeyByUid = new Map<string, number>();

	for (const section of sections) {
		const [newSection] = await tx
			.insert(constitutionSection)
			.values({
				versionKey: created.versionKey,
				sectionId: section.sectionId,
				title: section.title,
				kind: section.kind,
				icon: section.icon,
				sortOrder: section.sortOrder
			})
			.returning();

		const clauses = await tx
			.select()
			.from(constitutionClause)
			.where(eq(constitutionClause.sectionKey, section.sectionKey))
			.orderBy(asc(constitutionClause.depth), asc(constitutionClause.sortOrder));

		// Ordered by depth so a parent is always inserted before its children and
		// its new key is available for the remap.
		const newKeyByOldKey = new Map<number, number>();

		for (const clause of clauses) {
			const [inserted] = await tx
				.insert(constitutionClause)
				.values({
					sectionKey: newSection.sectionKey,
					parentKey: clause.parentKey != null ? (newKeyByOldKey.get(clause.parentKey) ?? null) : null,
					clauseUid: clause.clauseUid,
					depth: clause.depth,
					sortOrder: clause.sortOrder,
					label: clause.label,
					body: clause.body
				})
				.returning({ clauseKey: constitutionClause.clauseKey });

			newKeyByOldKey.set(clause.clauseKey, inserted.clauseKey);
			clauseKeyByUid.set(clause.clauseUid, inserted.clauseKey);
		}
	}

	return { versionKey: created.versionKey, versionNo: created.versionNo, clauseKeyByUid };
}

/**
 * Renumber and relabel a sibling group after an insert or delete.
 *
 * Without this, deleting clause II leaves the article reading I, III, IV.
 */
export async function resequenceSiblings(
	tx: Tx,
	sectionKey: number,
	parentKey: number | null,
	depth: number
): Promise<void> {
	const siblings = await tx
		.select({ clauseKey: constitutionClause.clauseKey, sortOrder: constitutionClause.sortOrder })
		.from(constitutionClause)
		.where(
			parentKey == null
				? sql`${constitutionClause.sectionKey} = ${sectionKey} and ${constitutionClause.parentKey} is null`
				: sql`${constitutionClause.sectionKey} = ${sectionKey} and ${constitutionClause.parentKey} = ${parentKey}`
		)
		.orderBy(asc(constitutionClause.sortOrder));

	for (const [index, sibling] of siblings.entries()) {
		const ordinal = index + 1;
		await tx
			.update(constitutionClause)
			.set({ sortOrder: ordinal, label: labelFor(depth, ordinal) })
			.where(eq(constitutionClause.clauseKey, sibling.clauseKey));
	}
}

export function newClauseUid(): string {
	return nanoid(12);
}
