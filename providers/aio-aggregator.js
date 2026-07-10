/**
 * All-in-One Aggregator — Nuvio Plugin
 *
 * Fetches every scraper listed in the combined manifest, executes each in an
 * isolated Function scope, calls them all in parallel, and returns one
 * deduplicated, quality-sorted flat list of streams.
 *
 * Runtime: QuickJS (quickjs-kt) — no Node built-ins, no ES modules.
 */

var MANIFEST_URL ='https://raw.githubusercontent.com/brotherguns/Plugin/refs/heads/main/combined_manifest.json';

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

/**
 * Map a quality string to a numeric tier (lower = better).
 */
function qualityRank(q) {
  if (!q) return 4;
  var s = String(q).toLowerCase();
  if (s.indexOf('2160') !== -1 || s.indexOf('4k') !== -1 || s.indexOf('uhd') !== -1) return 0;
  if (s.indexOf('1080') !== -1) return 1;
  if (s.indexOf('720')  !== -1) return 2;
  if (s.indexOf('480')  !== -1 || s.indexOf('360') !== -1 || s.indexOf('240') !== -1) return 3;
  return 4;
}

/**
 * Parse human-readable size strings ("13.2 GB", "850 MB") to bytes.
 */
function parseSizeToBytes(size) {
  if (!size) return 0;
  var m = String(size).match(/([\d.]+)\s*(tb|gb|mb|kb)/i);
  if (!m) return 0;
  var n = parseFloat(m[1]);
  if (isNaN(n)) return 0;
  var u = m[2].toLowerCase();
  if (u === 'tb') return n * 1099511627776;
  if (u === 'gb') return n * 1073741824;
  if (u === 'mb') return n * 1048576;
  if (u === 'kb') return n * 1024;
  return 0;
}

/**
 * Build a safe require() to pass into each sub-scraper scope.
 * Only cheerio and crypto-js are available in QuickJS.
 */
function buildSafeRequire() {
  return function safeRequire(name) {
    try {
      return require(name);
    } catch (e) {
      console.error('require("' + name + '") failed: ' + e.message);
      return undefined;
    }
  };
}

/**
 * Resolve the CryptoJS reference — may be a global or require()-able.
 */
function resolveCryptoJS() {
  if (typeof CryptoJS !== 'undefined') return CryptoJS;
  try { return require('crypto-js'); } catch (e) { return undefined; }
}

/**
 * Resolve __native_fetch — fall back to global fetch.
 */
function resolveNativeFetch() {
  if (typeof __native_fetch !== 'undefined') return __native_fetch;
  return fetch;
}

/* ------------------------------------------------------------------ */
/*  Scraper loader                                                    */
/* ------------------------------------------------------------------ */

/**
 * Execute a fetched scraper source string inside a fresh Function scope
 * and return its getStreams function (or null).
 */
function loadScraperFromCode(code, label) {
  // Every sub-scraper gets its own module/exports pair so that
  // `module.exports = …` inside one scraper does not clobber another.
  var mod = { exports: {} };

  // Some scrapers read these globals at load time.
  globalThis.SCRAPER_ID       = '';
  globalThis.SCRAPER_SETTINGS = {};

  var wrapper = new Function(
    'module',
    'exports',
    'require',
    'fetch',
    'console',
    'CryptoJS',
    '__native_fetch',
    code
  );

  wrapper(
    mod,
    mod.exports,
    buildSafeRequire(),
    fetch,
    console,
    resolveCryptoJS(),
    resolveNativeFetch()
  );

  // The scraper may have done either:
  //   module.exports = { getStreams }        → mod.exports replaced
  //   module.exports.getStreams = function…  → mod.exports augmented
  //   exports.getStreams = function…         → same object unless replaced
  var gs = (typeof mod.exports === 'function')
    ? mod.exports                       // unlikely but defensive
    : mod.exports.getStreams;

  if (typeof gs !== 'function') {
    console.error('Scraper [' + label + '] did not export getStreams');
    return null;
  }
  return gs;
}

/* ------------------------------------------------------------------ */
/*  Main entry point                                                  */
/* ------------------------------------------------------------------ */

async function getStreams(tmdbId, mediaType, season, episode) {

  /* ---------- 1. Fetch the combined manifest ------------------------ */
  var manifest;
  try {
    var mRes = await fetch(MANIFEST_URL);
    manifest = await mRes.json();
  } catch (e) {
    console.error('Aggregator: manifest fetch failed — ' + e.message);
    return [];
  }

  // The manifest may be a flat array or an object wrapping one.
  var entries = Array.isArray(manifest)
    ? manifest
    : (manifest.scrapers || manifest.providers || manifest.plugins || []);

  // Each entry's filename is already an absolute URL.
  var scraperUrls = [];
  for (var i = 0; i < entries.length; i++) {
    var e = entries[i];
    var url = (typeof e === 'string') ? e : (e.filename || e.url || e.file || '');
    if (url && url.indexOf('http') === 0) {
      scraperUrls.push(url);
    }
  }

  console.log('Aggregator: ' + scraperUrls.length + ' scraper URLs from manifest');

  /* ---------- 2. Fetch all scraper source files in parallel ---------- */
  var codePromises = scraperUrls.map(function (url) {
    return fetch(url)
      .then(function (r) { return r.text(); })
      .catch(function (e) {
        console.error('Aggregator: fetch failed for ' + url + ' — ' + e.message);
        return null;
      });
  });
  var codes = await Promise.all(codePromises);

  /* ---------- 3. Load each scraper in an isolated scope ------------- */
  var scraperFns = []; // { fn, label }
  for (var i = 0; i < codes.length; i++) {
    if (!codes[i]) continue;
    try {
      var fn = loadScraperFromCode(codes[i], scraperUrls[i]);
      if (fn) scraperFns.push({ fn: fn, label: scraperUrls[i] });
    } catch (e) {
      console.error('Aggregator: load error [' + scraperUrls[i] + '] — ' + e.message);
    }
  }

  console.log('Aggregator: ' + scraperFns.length + ' scrapers loaded');

  /* ---------- 4. Call every getStreams in parallel ------------------- */
  var resultSets = await Promise.all(
    scraperFns.map(function (entry) {
      return Promise.resolve()
        .then(function () {
          // Reset globals before each async call in case a scraper
          // mutates them during its own execution.
          globalThis.SCRAPER_ID       = '';
          globalThis.SCRAPER_SETTINGS = {};
          return entry.fn(tmdbId, mediaType, season, episode);
        })
        .catch(function (e) {
          console.error('Aggregator: runtime error [' + entry.label + '] — ' + e.message);
          return [];
        });
    })
  );

  /* ---------- 5. Merge & deduplicate by URL ------------------------- */
  var seen    = Object.create(null);   // prototype-less map
  var merged  = [];

  for (var i = 0; i < resultSets.length; i++) {
    var arr = resultSets[i];
    if (!Array.isArray(arr)) continue;
    for (var j = 0; j < arr.length; j++) {
      var s = arr[j];
      if (!s || typeof s !== 'object' || !s.url) continue;
      if (seen[s.url]) continue;
      seen[s.url] = true;
      merged.push(s);
    }
  }

  /* ---------- 6. Sort: quality tier ↑, then file size ↓ ------------- */
  merged.sort(function (a, b) {
    var qa = qualityRank(a.quality);
    var qb = qualityRank(b.quality);
    if (qa !== qb) return qa - qb;
    return parseSizeToBytes(b.size) - parseSizeToBytes(a.size);
  });

  console.log('Aggregator: returning ' + merged.length + ' streams');
  return merged;
}

/* ------------------------------------------------------------------ */
/*  Export                                                             */
/* ------------------------------------------------------------------ */

module.exports = { getStreams: getStreams };
