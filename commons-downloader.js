#!/usr/bin/env node
// Wikidata -> Commons paintings downloader (1500–present), Node 18+
// - Queries Wikidata SPARQL for paintings with P571 (inception) year
// - Gets Commons file titles from P18
// - Checks license via Commons imageinfo extmetadata
// - Downloads originals via Special:FilePath, dedupes by SHA256, validates

import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import {
	access,
	mkdir,
	readFile,
	rename,
	stat,
	unlink,
	writeFile,
} from 'node:fs/promises';
import { join } from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as sleep } from 'node:timers/promises';

const args = process.argv.slice(2);
function flag(name, d) {
	const i = args.indexOf(`--${name}`);
	if (i === -1) return d;
	const v = args[i + 1];
	return !v || v.startsWith('--') ? true : v;
}

const OUT_DIR = flag('out', 'images-commons');
const LIMIT = Number(flag('limit', '200')); // stop after N saves (default 200)
const BATCH = Math.max(1, Number(flag('batch', '200'))); // SPARQL rows per page (<= 1000)
const OFFSET = Math.max(0, Number(flag('offset', '0'))); // SPARQL offset start
const DELAY_MS = Math.max(0, Number(flag('delay', '150'))); // polite delay per image
const CONCURRENCY = Math.max(1, Number(flag('concurrency', '3')));
const CHECKPOINT = flag('checkpoint', '.commons.ckpt.json');
const NDJSON = flag('ndjson', 'commons-metadata.ndjson');
const HASHIDX = flag('hashindex', '.commons-hash-index.ndjson');
const DEBUG = !!flag('debug', false);

// Year range
const YEAR_FROM = Number(flag('from', '1850'));
const YEAR_TO = Number(flag('to', '2100'));

// endpoints
const WD_SPARQL = 'https://query.wikidata.org/sparql';
const COMMONS_API = 'https://commons.wikimedia.org/w/api.php';

async function ensureDir(p) {
	await mkdir(p, { recursive: true });
}
async function exists(p) {
	try {
		await access(p);
		return true;
	} catch {
		return false;
	}
}
async function appendNDJSON(path, obj) {
	await writeFile(path, JSON.stringify(obj) + '\n', { flag: 'a' });
}
async function saveJSON(path, obj) {
	await writeFile(path, JSON.stringify(obj, null, 2));
}
async function loadJSON(path) {
	if (!(await exists(path))) return null;
	try {
		return JSON.parse(await readFile(path, 'utf8'));
	} catch {
		return null;
	}
}
function sanitize(s) {
	return String(s || '')
		.replace(/[<>:\"/\\|?*\x00-\x1F]/g, '_')
		.replace(/\s+/g, ' ')
		.trim()
		.slice(0, 180);
}

function filename(baseTitle, artist, year) {
	const t = sanitize(baseTitle || 'Untitled');
	const a = sanitize(artist || 'Unknown');
	const y = year ? String(year) : '';
	return `${a}${y ? ` (${y})` : ''} - ${t}`;
}

// hash + validate
const seen = new Map();
async function loadHashIndex() {
	if (!(await exists(HASHIDX))) return;
	const txt = await readFile(HASHIDX, 'utf8');
	for (const line of txt.split(/\r?\n/)) {
		if (!line.trim()) continue;
		try {
			const r = JSON.parse(line);
			if (r.sha256 && r.path)
				seen.set(r.sha256, { path: r.path, bytes: r.bytes || 0 });
		} catch {}
	}
}
function looksImage(ct) {
	return (
		ct &&
		(ct.toLowerCase().startsWith('image/') ||
			ct.toLowerCase().includes('octet-stream'))
	);
}
function magicKind(buf) {
	if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xd8) return 'jpg';
	if (
		buf.length >= 8 &&
		buf[0] === 0x89 &&
		buf[1] === 0x50 &&
		buf[2] === 0x4e &&
		buf[3] === 0x47
	)
		return 'png';
	if (buf.length >= 12) {
		const riff = Buffer.from(buf.subarray(0, 4)).toString('ascii') === 'RIFF';
		const webp = Buffer.from(buf.subarray(8, 12)).toString('ascii') === 'WEBP';
		if (riff && webp) return 'webp';
	}
	return null;
}
function extFor(kind) {
	return kind === 'png' ? '.png' : kind === 'webp' ? '.webp' : '.jpg';
}

async function downloadValidated(url, destBase) {
	const res = await fetch(url, {
		redirect: 'follow',
		headers: { Accept: '*/*', 'User-Agent': 'commons-downloader/1.0' },
	});
	if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);

	const tmp = destBase + '.part';
	let head = Buffer.alloc(0);
	const hash = createHash('sha256');
	class Tap extends Transform {
		_transform(chunk, enc, cb) {
			try {
				hash.update(chunk);
				if (head.length < 64) {
					head = Buffer.concat([head, chunk]);
					if (head.length > 64) head = head.subarray(0, 64);
				}
				this.push(chunk);
				cb();
			} catch (e) {
				cb(e);
			}
		}
	}
	await pipeline(Readable.fromWeb(res.body), new Tap(), createWriteStream(tmp));
	const st = await stat(tmp);
	if (st.size < 1024) {
		await unlink(tmp);
		throw new Error(`Too small: ${st.size} bytes`);
	}

	const ct = res.headers.get('content-type') || '';
	const kind = magicKind(head);
	if (!looksImage(ct) && !kind) {
		await unlink(tmp);
		throw new Error(`Non-image: ${ct || 'unknown'}`);
	}

	const sha = hash.digest('hex');
	if (seen.has(sha)) {
		await unlink(tmp);
		return {
			skipped: true,
			duplicateOf: seen.get(sha).path,
			sha256: sha,
			bytes: seen.get(sha).bytes,
		};
	}

	const final = destBase + extFor(kind || 'jpg');
	await rename(tmp, final);
	await writeFile(
		HASHIDX,
		JSON.stringify({ sha256: sha, path: final, bytes: st.size }) + '\n',
		{ flag: 'a' }
	);
	return { skipped: false, path: final, sha256: sha, bytes: st.size };
}

async function fetchWikidataBatchPaged({
	yearFrom,
	yearTo,
	limit = 100,
	offset = 0,
}) {
	const query = `
SELECT ?item ?itemLabel ?creatorLabel ?year ?file WHERE {
  ?item wdt:P31 wd:Q3305213 .            # instance of painting
  ?item wdt:P18 ?file .                   # has image (Commons file)
  OPTIONAL { ?item wdt:P571 ?date . BIND(YEAR(?date) AS ?year) }
  OPTIONAL { ?item wdt:P170 ?creator . }  # creator
  FILTER(bound(?year) && ?year >= ${yearFrom} && ?year <= ${yearTo})
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY ?item
LIMIT ${limit}
OFFSET ${offset}
`.trim();

	const body = new URLSearchParams({ query, format: 'json' });
	const url = WD_SPARQL;
	const needsRetry = (code) => [429, 500, 502, 503, 504].includes(code);

	for (let attempt = 1; attempt <= 6; attempt++) {
		const res = await fetch(url, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
				Accept: 'application/sparql-results+json',
				'User-Agent': 'commons-downloader/1.1',
			},
			body,
		});
		if (res.ok) {
			const js = await res.json();
			const rows = js?.results?.bindings || [];
			return rows.map((r) => ({
				qid: r.item?.value?.split('/').pop(),
				title: r.itemLabel?.value,
				creator: r.creatorLabel?.value,
				year: r.year ? Number(r.year.value) : null,
				commonsFile: r.file?.value,
			}));
		}
		if (!needsRetry(res.status)) throw new Error(`WD HTTP ${res.status}`);
		const backoff =
			Math.min(15000, 500 * Math.pow(2, attempt - 1)) +
			Math.floor(Math.random() * 200);
		if (DEBUG)
			console.warn(`WD ${res.status}, retry ${attempt}/6 in ${backoff} ms…`);
		await new Promise((r) => setTimeout(r, backoff));
	}
	throw new Error('WD retries exhausted');
}

function* yearSlices(from, to, span) {
	let a = from;
	while (a <= to) {
		const b = Math.min(a + span - 1, to);
		yield [a, b];
		a = b + 1;
	}
}

// Commons license check for a batch of titles
async function fetchCommonsInfoChunk(titles) {
	// titles: up to ~20 items per chunk
	const params = new URLSearchParams({
		action: 'query',
		prop: 'imageinfo',
		iiprop: 'url|mime|extmetadata',
		format: 'json',
		// MediaWiki API accepts POST with large payloads safely
		titles: titles.join('|'),
	});
	const res = await fetch(COMMONS_API, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
			'User-Agent': 'commons-downloader/1.2',
		},
		body: params,
	});
	if (!res.ok) throw new Error(`Commons HTTP ${res.status}`);
	const js = await res.json();
	const pages = js?.query?.pages || {};
	const out = new Map();
	for (const k of Object.keys(pages)) {
		const p = pages[k];
		const t = p?.title;
		const ii = Array.isArray(p?.imageinfo) ? p.imageinfo[0] : null;
		if (!t || !ii) continue;
		const meta = ii?.extmetadata || {};
		const short = meta?.LicenseShortName?.value || meta?.License?.value || '';
		const pd = /public\s*domain/i.test(short);
		const cc0 = /cc0/i.test(short);
		out.set(t, {
			url: ii.url,
			mime: ii.mime,
			licenseShort: short,
			isOpen: pd || cc0,
		});
	}
	return out;
}

async function fetchCommonsInfo(titles, chunkSize = 20) {
	// merge results across chunks
	const merged = new Map();
	for (let i = 0; i < titles.length; i += chunkSize) {
		const slice = titles.slice(i, i + chunkSize);
		const part = await fetchCommonsInfoChunk(slice);
		for (const [k, v] of part.entries()) merged.set(k, v);
	}
	return merged;
}

function commonsTitleFromUrl(commonsUrl) {
	// commonsUrl might be a full "https://upload.wikimedia.org/..." or "http://commons.wikimedia.org/wiki/Special:FilePath/Title"
	// We want a stable "File:Title.ext"
	try {
		const u = new URL(commonsUrl);
		if (/\/wiki\/Special:FilePath\//i.test(u.pathname)) {
			const file = decodeURIComponent(
				u.pathname.split('/wiki/Special:FilePath/')[1]
			);
			// If no "File:" prefix, add it
			return file.startsWith('File:') ? file : `File:${file}`;
		}
	} catch {}
	// Fallback: assume already a File: title
	return commonsUrl.startsWith('File:') ? commonsUrl : null;
}

function filePathUrl(fileTitle) {
	// download original via redirect
	const title = encodeURIComponent(fileTitle.replace(/^File:/, ''));
	return `https://commons.wikimedia.org/wiki/Special:FilePath/${title}?download`;
}

async function runPool(items, worker, conc) {
	const q = [...items];
	const running = new Set();
	while (q.length > 0 || running.size > 0) {
		while (q.length > 0 && running.size < conc) {
			const it = q.shift();
			const p = (async () => {
				await worker(it);
			})().finally(() => running.delete(p));
			running.add(p);
		}
		if (running.size > 0) await Promise.race(running);
	}
}

async function main() {
	await ensureDir(OUT_DIR);
	await loadHashIndex();

	const ck = (await loadJSON(CHECKPOINT)) || {
		sliceFrom: YEAR_FROM,
		sliceTo: Math.min(YEAR_FROM + 24, YEAR_TO),
		offset: 0,
		saved: 0,
		done: false,
	};
	let { sliceFrom, sliceTo, offset, saved, done } = ck;

	const HARD = LIMIT > 0 ? LIMIT : Infinity;
	const span = 25; // size of each year window
	const pageSize = Math.max(1, Math.min(500, Number(flag('batch', '100'))));

	// resume logic: pick up at current sliceFrom..sliceTo
	const slices = Array.from(yearSlices(YEAR_FROM, YEAR_TO, span));
	let startIndex = slices.findIndex(
		([a, b]) => a === sliceFrom && b === sliceTo
	);
	if (startIndex === -1) startIndex = 0;

	for (let si = startIndex; si < slices.length && !done && saved < HARD; si++) {
		const [a, b] = slices[si];
		if (DEBUG)
			console.log(
				`${new Date().toISOString()} : slice ${a}-${b} (offset=${offset})`
			);

		while (!done && saved < HARD) {
			let rows;
			try {
				rows = await fetchWikidataBatchPaged({
					yearFrom: a,
					yearTo: b,
					limit: pageSize,
					offset,
				});
			} catch (e) {
				// if a slice keeps failing, skip to next slice
				console.warn(`slice ${a}-${b} error: ${e.message || e}; moving on`);
				break;
			}
			if (DEBUG) console.log(`offset=${offset} got=${rows.length}`);
			if (!rows.length) break;

			// build titles to license-check on Commons
			const titles = [];
			const ctx = [];
			for (const r of rows) {
				const title = commonsTitleFromUrl(r.commonsFile);
				if (!title) continue;
				titles.push(title);
				ctx.push({ title, r });
			}
			if (titles.length) {
				await processBatch(titles, ctx, () => saved++, HARD);
			}

			offset += pageSize;
			await saveJSON(CHECKPOINT, {
				sliceFrom: a,
				sliceTo: b,
				offset,
				saved,
				done: false,
			});
			if (saved >= HARD) break;
		}

		// next slice
		offset = 0;
		await saveJSON(CHECKPOINT, {
			sliceFrom: slices[si + 1]?.[0] ?? a,
			sliceTo: slices[si + 1]?.[1] ?? b,
			offset,
			saved,
			done: false,
		});
	}

	await saveJSON(CHECKPOINT, { sliceFrom, sliceTo, offset, saved, done: true });
	console.log(
		`${new Date().toISOString()} : Done. Saved ${saved} images. Metadata -> ${NDJSON}`
	);
}

async function processBatch(titles, ctx, incSaved, HARD) {
	// 1) license check on Commons
	const info = await fetchCommonsInfo(titles);
	// 2) keep only PD/CC0
	const todo = [];
	for (const { title, r } of ctx) {
		const meta = info.get(title);
		if (!meta) continue;
		if (!meta.isOpen) continue;
		todo.push({ title, meta, r });
	}
	// 3) download in a small pool
	await runPool(
		todo,
		async ({ title, meta, r }) => {
			if (LIMIT > 0 && incSaved.count >= HARD) return;
			const url = filePathUrl(title);
			const base = join(OUT_DIR, filename(r.title, r.creator, r.year));
			try {
				const res = await downloadValidated(url, base);
				await appendNDJSON(NDJSON, {
					qid: r.qid,
					title: r.title,
					creator: r.creator,
					year: r.year,
					commons_title: title,
					license: meta.licenseShort,
					source_url: meta.url,
					sha256: res.sha256,
					bytes: res.bytes,
				});
				incSaved.count = (incSaved.count || 0) + 1;
				if (incSaved.count % 25 === 0)
					console.log(
						`${new Date().toISOString()} : Saved ${
							incSaved.count
						} images so far…`
					);
			} catch (e) {
				if (DEBUG) console.warn('skip', title, '-', e.message || e);
			}
			if (DELAY_MS) await sleep(DELAY_MS);
		},
		CONCURRENCY
	);
}
main().catch((e) => {
	console.error(new Date().toISOString());
	console.error(e);
	process.exit(1);
});
