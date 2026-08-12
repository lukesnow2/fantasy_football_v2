#!/usr/bin/env node
/**
 * Copy the emoji dataset out of node_modules and into static/.
 *
 * emoji-picker-element fetches its ~440kB dataset from jsDelivr by default. We
 * point it at our own origin instead (see EMOJI_DATA_SOURCE in
 * src/lib/chat/constants.ts), which means the file has to actually be there.
 *
 * The copy is committed so a checkout works without a build, and this script
 * runs on every build so it can never drift from the installed package version.
 * Failing loudly matters: a silently missing dataset is an emoji picker that
 * renders an empty grid, which looks like a bug in the picker rather than a
 * missing file.
 */

import { copyFileSync, existsSync, mkdirSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const source = resolve(here, '../node_modules/emoji-picker-element-data/en/emojibase/data.json');
const target = resolve(here, '../static/emoji/data.json');

if (!existsSync(source)) {
	console.error(
		`[sync-emoji-data] Missing ${source}\n` +
			'Install dev dependencies first: npm install --include=dev'
	);
	process.exit(1);
}

mkdirSync(dirname(target), { recursive: true });
copyFileSync(source, target);

const count = JSON.parse(readFileSync(target, 'utf8')).length;
console.log(`[sync-emoji-data] ${count} emoji -> static/emoji/data.json`);
