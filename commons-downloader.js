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
// const LIMIT = Number(flag('limit', '200')); // stop after N saves (default 200)
// const BATCH = Math.max(1, Number(flag('batch', '200'))); // SPARQL rows per page (<= 1000)
// const OFFSET = Math.max(0, Number(flag('offset', '0'))); // SPARQL offset start
const DELAY_MS = Math.max(0, Number(flag('delay', '150'))); // polite delay per image
// Wikidata SPARQL pacing and timeouts
const WD_DELAY_MS = Math.max(0, Number(flag('wdDelay', '2000'))); // pause between SPARQL pages
const WD_TIMEOUT_MS = Math.max(5000, Number(flag('wdTimeout', '45000'))); // per SPARQL request timeout
// Commons/HTTP timeouts
const HTTP_TIMEOUT_MS = Math.max(5000, Number(flag('httpTimeout', '30000')));
const RESTART_DELAY_MS = Math.max(1000, Number(flag('restartDelay', '15000'))); // wait before auto-restart after a crash
const CONCURRENCY = Math.max(1, Number(flag('concurrency', '2')));
const CHECKPOINT = flag('checkpoint', '.commons.ckpt.json');
const NDJSON = flag('ndjson', 'commons-metadata.ndjson');
const HASHIDX = flag('hashindex', '.commons-hash-index.ndjson');
const DEBUG = !!flag('debug', true);
const NO_NDJSON = !!flag('noNdjson', true);
const NO_LICENSE_FILTER = !!flag('noLicenseFilter', true);
const USER_AGENT = String(flag('ua', 'commons-downloader/1.3'));
// Allowed licenses (comma-separated tokens). Defaults to PD and CC0 only.
// Accepts tokens: PD, CC0, CC-BY, CC-BY-SA, ANY-CC
const LICENSES = String(flag('licenses', 'ANY-CC'))
	.split(',')
	.map((s) => s.trim().toUpperCase())
	.filter(Boolean);

// Year range
const YEAR_FROM = Number(flag('from', '1800'));
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
	// retry a few times on transient failures or 5xx/429
	const needsRetry = (code) => [429, 500, 502, 503, 504].includes(code);
	let res;
	for (let attempt = 1; attempt <= 5; attempt++) {
		try {
			const ac = new AbortController();
			const t = setTimeout(() => ac.abort(), HTTP_TIMEOUT_MS);
			res = await fetch(url, {
				redirect: 'follow',
				headers: { Accept: '*/*', 'User-Agent': USER_AGENT },
				signal: ac.signal,
			});
			clearTimeout(t);
			if (res.ok) break;
			if (!needsRetry(res.status))
				throw new Error(`HTTP ${res.status} for ${url}`);
		} catch (e) {
			// network/timeout -> retry
			if (attempt >= 5) throw e;
		}
		const backoff =
			Math.min(15000, 500 * Math.pow(2, attempt - 1)) +
			Math.floor(Math.random() * 200);
		if (DEBUG)
			console.warn(`download retry ${attempt}/5 in ${backoff} ms for ${url}`);
		await new Promise((r) => setTimeout(r, backoff));
	}
	if (!res || !res.ok) throw new Error(`HTTP ${res?.status} for ${url}`);

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
	// 	const query = `
	// SELECT ?item ?itemLabel ?creatorLabel ?year ?file WHERE {
	//   ?item wdt:P31 wd:Q3305213 .            # instance of painting
	//   ?item wdt:P18 ?file .                   # has image (Commons file)
	//   OPTIONAL { ?item wdt:P571 ?date . BIND(YEAR(?date) AS ?year) }
	//   OPTIONAL { ?item wdt:P170 ?creator . }  # creator
	//   FILTER(bound(?year) && ?year >= ${yearFrom} && ?year <= ${yearTo})
	//   SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
	// }
	// ORDER BY ?item
	// LIMIT ${limit}
	// OFFSET ${offset}
	// `.trim();

	const query = `
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?file WHERE {
  ?item wdt:P571 ?date ;
		wdt:P18  ?file ;
		wdt:P31/wdt:P279* ?type .
  FILTER(
	?date >= "${yearFrom}-01-01T00:00:00Z"^^xsd:dateTime &&
	?date <= "${yearTo}-12-31T23:59:59Z"^^xsd:dateTime
  )

  VALUES ?type {
	wd:Q1183543  # scientific instrument
	wd:Q11019    # machine
	wd:Q39546    # tool
	wd:Q68       # computer
	wd:Q338      # telescope
	wd:Q11009    # microscope
	wd:Q42889    # vehicle
	wd:Q7397     # software
	wd:Q869     # mineral
    wd:Q42603   # fossil
    wd:Q8063    # rock
    wd:Q756     # plant
    wd:Q729     # animal
    wd:Q677     # microorganism
	wd:Q16521   # anatomical structure
	wd:Q1075    # chemical compound
	wd:Q11173   # chemical element
	wd:Q11423   # alloy
	wd:Q11436   # chemical mixture
	wd:Q12136   # polymer
}

  # Exclude artworks and buildings
  MINUS { ?item wdt:P31/wdt:P279* wd:Q3305213 } # painting
  MINUS { ?item wdt:P31/wdt:P279* wd:Q838948 }  # work of art
  MINUS { ?item wdt:P31/wdt:P279* wd:Q811979 }  # architectural structure
  MINUS { ?item wdt:P31/wdt:P279* wd:Q24398318 } # religious building
}
LIMIT ${limit}
OFFSET ${offset}
`.trim();

	const body = new URLSearchParams({ query, format: 'json' });
	const url = WD_SPARQL;
	const needsRetry = (code) => [429, 500, 502, 503, 504].includes(code);
	let lastStatus = null;
	let lastError = null;

	for (let attempt = 1; attempt <= 6; attempt++) {
		let res;
		try {
			const ac = new AbortController();
			const t = setTimeout(() => ac.abort(), WD_TIMEOUT_MS);
			res = await fetch(url, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
					Accept: 'application/sparql-results+json',
					'User-Agent': USER_AGENT,
				},
				body,
				signal: ac.signal,
			});
			clearTimeout(t);
			lastStatus = res.status;
			if (res.ok) {
				const js = await res.json();
				const rows = js?.results?.bindings || [];
				return rows
					.map((r) => r.file?.value)
					.filter((v) => typeof v === 'string' && v.length > 0);
			}
			if (!needsRetry(res.status)) throw new Error(`WD HTTP ${res.status}`);
			if (DEBUG) {
				// Try to read a short error snippet (non-fatal if it fails)
				try {
					const txt = await res.text();
					const snippet = (txt || '').slice(0, 200).replace(/\s+/g, ' ');
					console.warn(`WD ${res.status} body: ${snippet}`);
				} catch {}
			}
		} catch (e) {
			lastError = e;
			if (attempt >= 6) throw e;
		}
		const backoff =
			Math.min(15000, 500 * Math.pow(2, attempt - 1)) +
			Math.floor(Math.random() * 200);
		if (DEBUG)
			console.warn(
				`WD ${lastStatus ?? 'ERR'}, retry ${attempt}/6 in ${backoff} ms…`
			);
		await new Promise((r) => setTimeout(r, backoff));
	}
	throw new Error(
		`WD retries exhausted${
			lastStatus
				? ` (last status ${lastStatus})`
				: lastError
				? ` (${lastError.message || lastError})`
				: ''
		}`
	);
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
	const needsRetry = (code) => [429, 500, 502, 503, 504].includes(code);
	let js;
	for (let attempt = 1; attempt <= 6; attempt++) {
		let res;
		try {
			const ac = new AbortController();
			const t = setTimeout(() => ac.abort(), HTTP_TIMEOUT_MS);
			res = await fetch(COMMONS_API, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
					'User-Agent': USER_AGENT,
				},
				body: params,
				signal: ac.signal,
			});
			clearTimeout(t);
			if (res.ok) {
				js = await res.json();
				break;
			}
			if (!needsRetry(res.status))
				throw new Error(`Commons HTTP ${res.status}`);
		} catch (e) {
			if (attempt >= 6) throw e;
		}
		const backoff =
			Math.min(15000, 500 * Math.pow(2, attempt - 1)) +
			Math.floor(Math.random() * 200);
		if (DEBUG) console.warn(`Commons retry ${attempt}/6 in ${backoff} ms…`);
		await new Promise((r) => setTimeout(r, backoff));
	}
	if (!js) throw new Error('Commons retries exhausted');
	const pages = js?.query?.pages || {};
	const out = new Map();
	for (const k of Object.keys(pages)) {
		const p = pages[k];
		const t = p?.title;
		const ii = Array.isArray(p?.imageinfo) ? p.imageinfo[0] : null;
		if (!t || !ii) continue;
		const meta = ii?.extmetadata || {};
		const short = meta?.LicenseShortName?.value || meta?.License?.value || '';
		const licenseUp = short.toUpperCase();
		// Normalize separators to '-' and collapse repeats
		const norm = licenseUp.replace(/[^A-Z0-9]+/g, '-').replace(/-+/g, '-');
		// Examples covered:
		//  - 'Public domain', 'PUBLIC-DOMAIN', 'PD', 'PD-US'
		//  - 'CC BY 4.0', 'CC-BY', 'Creative Commons Attribution'
		//  - 'CC BY-SA 4.0', 'CC-BY-SA', 'Attribution-ShareAlike'
		const pd = /(^|-)PD(-|$)/.test(norm) || /PUBLIC-DOMAIN/.test(norm);
		const cc0 = /CC0/.test(norm);
		const ccby = /CC-BY(?!-SA)/.test(norm) || /ATTRIBUTION/.test(licenseUp);
		const ccbysa = /CC-BY-SA/.test(norm) || /ATTRIBUTION-SHAREALIKE/.test(norm);
		const anycc = /^CC-/.test(norm) || licenseUp.includes('CREATIVE COMMONS');
		let isOpen = false;
		if (LICENSES.includes('PD') && pd) isOpen = true;
		if (LICENSES.includes('CC0') && cc0) isOpen = true;
		if (LICENSES.includes('CC-BY') && ccby) isOpen = true;
		if (LICENSES.includes('CC-BY-SA') && ccbysa) isOpen = true;
		if (LICENSES.includes('ANY-CC') && anycc) isOpen = true;
		out.set(t, {
			url: ii.url,
			mime: ii.mime,
			licenseShort: short,
			isOpen,
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

function createQuota(limit, initialSaved = 0) {
	if (!isFinite(limit) || limit <= 0) {
		return {
			take() {
				// unlimited: always allow; return a no-op token
				return { commit() {}, release() {} };
			},
			left() {
				return Infinity;
			},
		};
	}
	let remaining = Math.max(0, limit - (initialSaved || 0));
	return {
		take() {
			if (remaining <= 0) return null;
			remaining--;
			let committed = false;
			return {
				commit() {
					committed = true;
				},
				release() {
					if (!committed) remaining++;
				},
			};
		},
		left() {
			return remaining;
		},
	};
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

	// const HARD = LIMIT > 0 ? LIMIT : Infinity;
	const HARD = Infinity;
	const quota = createQuota(HARD, saved);
	const span = Math.max(1, Number(flag('yearsPerSlice', '2'))); // size of each year window
	const pageSize = Math.max(1, Math.min(500, Number(flag('batch', '20'))));

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

		const pageRetries = Math.max(0, Number(flag('pageRetries', '3')));
		while (!done && saved < HARD) {
			let rows;
			let pageOk = false;
			for (let attempt = 0; attempt <= pageRetries; attempt++) {
				try {
					rows = await fetchWikidataBatchPaged({
						yearFrom: a,
						yearTo: b,
						limit: pageSize,
						offset,
					});
					pageOk = true;
					break;
				} catch (e) {
					if (attempt >= pageRetries) {
						console.warn(
							`slice ${a}-${b} offset=${offset} error after ${
								attempt + 1
							} tries: ${e.message || e}; moving on`
						);
						break;
					}
					const backoff =
						2000 * (attempt + 1) + Math.floor(Math.random() * 400);
					if (DEBUG)
						console.warn(
							`slice ${a}-${b} offset=${offset} retry ${
								attempt + 1
							}/${pageRetries} in ${backoff} ms…`
						);
					await sleep(backoff);
				}
			}
			if (!pageOk) break;
			if (DEBUG) console.log(`offset=${offset} got=${rows.length}`);
			if (!rows.length) break;

			// build titles to license-check on Commons
			const titles = [];
			for (const f of rows) {
				const title = commonsTitleFromUrl(f);
				if (!title) continue;
				titles.push(title);
			}
			if (titles.length) {
				const counter = { inc: () => (saved += 1), get: () => saved };
				await processBatch(titles, counter, HARD, quota);
			}

			offset += pageSize;
			if (WD_DELAY_MS) {
				if (DEBUG)
					console.log(
						`${new Date().toISOString()} : WD pause ${WD_DELAY_MS} ms before next page`
					);
				await sleep(WD_DELAY_MS);
			}
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

	// Finalize checkpoint as done without resetting slice progress.
	// Load the latest checkpoint on disk (which was updated during the run)
	// and just mark it done while preserving its sliceFrom/sliceTo/offset.
	const latestCk = (await loadJSON(CHECKPOINT)) || {
		sliceFrom,
		sliceTo,
		offset,
		saved,
	};
	await saveJSON(CHECKPOINT, {
		...latestCk,
		saved,
		done: true,
	});
	const summary = NO_NDJSON
		? `${new Date().toISOString()} : Done. Saved ${saved} images.`
		: `${new Date().toISOString()} : Done. Saved ${saved} images. Metadata -> ${NDJSON}`;
	console.log(summary);
}

async function processBatch(titles, counter, HARD, quota) {
	// 1) license check on Commons (retry this batch a few times if it fails transiently)
	let info;
	if (!NO_LICENSE_FILTER) {
		for (let attempt = 1; attempt <= 3; attempt++) {
			try {
				info = await fetchCommonsInfo(titles);
				break;
			} catch (e) {
				if (attempt >= 3) throw e;
				const backoff =
					1000 * attempt * attempt + Math.floor(Math.random() * 300);
				if (DEBUG)
					console.warn(
						`Batch license lookup retry ${attempt}/3 in ${backoff} ms…`
					);
				await new Promise((r) => setTimeout(r, backoff));
			}
		}
	}
	// 2) keep only PD/CC0
	const todo = [];
	for (const title of titles) {
		if (NO_LICENSE_FILTER) {
			todo.push({
				title,
				meta: { licenseShort: 'UNKNOWN', url: filePathUrl(title) },
			});
		} else {
			const meta = info.get(title);
			if (!meta) continue;
			if (!meta.isOpen) continue;
			todo.push({ title, meta });
		}
	}
	// 3) download in a small pool
	await runPool(
		todo,
		async ({ title, meta }) => {
			// if (LIMIT > 0 && counter.get() >= HARD) return;
			// Acquire a quota token to strictly enforce global limit with concurrency
			const token = quota.take();
			if (!token) return; // out of quota, skip
			const url = filePathUrl(title);
			// Build filename from Commons title only
			const baseTitle = title.replace(/^File:/, '');
			const safeBase = sanitize(baseTitle).replace(/\.[A-Za-z0-9]+$/, '');
			const base = join(OUT_DIR, safeBase);
			try {
				const res = await downloadValidated(url, base);
				// Only treat as a new saved image if not a duplicate
				if (!res.skipped) {
					if (!NO_NDJSON) {
						await appendNDJSON(NDJSON, {
							commons_title: title,
							license: meta.licenseShort,
							source_url: meta.url,
							sha256: res.sha256,
							bytes: res.bytes,
						});
					}
					counter.inc();
					token.commit();
					const total = counter.get();
					if (total % 25 === 0)
						console.log(
							`${new Date().toISOString()} : Saved ${total} images so far…`
						);
				} else if (DEBUG) {
					console.warn('duplicate, skipped', title, '->', res.duplicateOf);
					token.release();
				}
			} catch (e) {
				if (DEBUG) console.warn('skip', title, '-', e.message || e);
				token.release();
			}
			if (DELAY_MS) await sleep(DELAY_MS);
		},
		CONCURRENCY
	);
}

async function startLoop() {
	// Auto-restart the run on unhandled errors; resume from checkpoint
	let attempt = 0;
	for (;;) {
		try {
			await main();
			break; // finished successfully
		} catch (e) {
			attempt++;
			console.error(new Date().toISOString());
			console.error(e);
			console.warn(
				`${new Date().toISOString()} : Run failed (attempt ${attempt}). Restarting in ${RESTART_DELAY_MS} ms…`
			);
			await new Promise((r) => setTimeout(r, RESTART_DELAY_MS));
		}
	}
}

startLoop();
