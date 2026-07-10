/**
 * All-in-One Aggregator — Nuvio Plugin
 *
 * Three-phase pipeline for maximum concurrency in single-threaded QuickJS:
 *   Phase 1  →  Promise.all all fetches (all in-flight from tick 1)
 *   Phase 2  →  Synchronous eval of every returned source string
 *   Phase 3  →  Promise.allSettled all getStreams calls
 *
 * No setTimeout, no AbortController — neither exists in QuickJS.
 * Error isolation: every scraper call is individually .catch-wrapped,
 * Promise.allSettled absorbs rejections, and a top-level catch guards
 * the entire pipeline.
 *
 * Runtime: QuickJS (quickjs-kt) — CommonJS only, no Node built-ins.
 */

var MANIFEST_URL =
  'https://raw.githubusercontent.com/brotherguns/Plugin/refs/heads/main/combined_manifest.json';

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

function qualityRank(q) {
  if (!q) return 4;
  var s = String(q).toLowerCase();
  if (s.indexOf('2160') !== -1 || s.indexOf('4k') !== -1 || s.indexOf('uhd') !== -1) return 0;
  if (s.indexOf('1080') !== -1) return 1;
  if (s.indexOf('720')  !== -1) return 2;
  if (s.indexOf('480')  !== -1 || s.indexOf('360') !== -1 || s.indexOf('240') !== -1) return 3;
  return 4;
}

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

/* Resolved once, cached for all sub-scrapers */
var _crypto;
function getCrypto() {
  if (_crypto !== undefined) return _crypto;
  if (typeof CryptoJS !== 'undefined') { _crypto = CryptoJS; return _crypto; }
  try { _crypto = require('crypto-js'); } catch (e) { _crypto = null; }
  return _crypto;
}

function safeRequire(name) {
  try { return require(name); } catch (e) { return undefined; }
}

/* ------------------------------------------------------------------ */
/*  Entry point                                                       */
/* ------------------------------------------------------------------ */

async function getStreams(tmdbId, mediaType, season, episode) {
  try {

    /* ── Phase 0: Fetch the combined manifest ────────────────────── */

    var manifest;
    try {
      var mRes = await fetch(MANIFEST_URL);
      manifest = await mRes.json();
    } catch (e) {
      console.error('AIO: manifest fetch failed — ' + e.message);
      return [];
    }

    var entries = Array.isArray(manifest)
      ? manifest
      : (manifest.scrapers || manifest.providers || manifest.plugins || []);

    var urls = [];
    for (var i = 0; i < entries.length; i++) {
      var entry = entries[i];
      var url = (typeof entry === 'string') ? entry : (entry.filename || entry.url || '');
      if (url && url.indexOf('http') === 0) urls.push(url);
    }

    if (!urls.length) {
      console.error('AIO: manifest contained 0 scraper URLs');
      return [];
    }

    console.log('AIO: ' + urls.length + ' scrapers in manifest');

    /* ── Phase 1: Fetch ALL scraper JS files simultaneously ──────── */
    /*    One Promise.all — every fetch launches on the same tick.    */
    /*    Non-200 or network error → null, skip immediately.         */

    var codes = await Promise.all(urls.map(function (url) {
      return fetch(url).then(function (r) {
        if (!r.ok) return null;
        return r.text();
      }).catch(function () {
        return null;
      });
    }));

    /* ── Phase 2: Eval all source strings synchronously ──────────── */
    /*    Pure CPU, no I/O. Runs in one pass.                        */

    var scraperFns = [];
    var crypto = getCrypto();

    for (var i = 0; i < codes.length; i++) {
      if (!codes[i]) continue;
      try {
        var mod = { exports: {} };
        globalThis.SCRAPER_ID       = '';
        globalThis.SCRAPER_SETTINGS = {};

        var wrapper = new Function(
          'module', 'exports', 'require', 'fetch', 'console', 'CryptoJS',
          codes[i]
        );
        wrapper(mod, mod.exports, safeRequire, fetch, console, crypto);

        var gs = (typeof mod.exports === 'function')
          ? mod.exports
          : mod.exports.getStreams;

        if (typeof gs === 'function') {
          scraperFns.push({ fn: gs, label: urls[i] });
        }
      } catch (e) {
        console.error('AIO: eval [' + urls[i] + '] — ' + e.message);
      }
    }

    /* Free source strings */
    codes = null;

    console.log('AIO: ' + scraperFns.length + ' scrapers loaded');

    /* ── Phase 3: Call ALL getStreams simultaneously ──────────────── */
    /*    Promise.allSettled — never rejects, always returns results. */
    /*    Each call is also individually .catch-wrapped so even if    */
    /*    allSettled somehow sees a rejection it maps to [].          */

    var settled = await Promise.allSettled(scraperFns.map(function (entry) {
      return Promise.resolve().then(function () {
        globalThis.SCRAPER_ID       = '';
        globalThis.SCRAPER_SETTINGS = {};
        return entry.fn(tmdbId, mediaType, season, episode);
      }).catch(function (e) {
        console.error('AIO: call [' + entry.label + '] — ' + e.message);
        return [];
      });
    }));

    /* Map allSettled outcomes: fulfilled → value, rejected → []      */
    var resultSets = [];
    for (var i = 0; i < settled.length; i++) {
      resultSets.push(settled[i].status === 'fulfilled' ? settled[i].value : []);
    }

    /* ── Phase 4: Merge → deduplicate → sort ─────────────────────── */

    var seen   = Object.create(null);
    var merged = [];

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

    merged.sort(function (a, b) {
      var qa = qualityRank(a.quality);
      var qb = qualityRank(b.quality);
      if (qa !== qb) return qa - qb;
      return parseSizeToBytes(b.size) - parseSizeToBytes(a.size);
    });

    console.log('AIO: returning ' + merged.length + ' streams');
    return merged;

  } catch (e) {
    /* Top-level catch — entire pipeline failure returns [] */
    console.error('AIO: top-level error — ' + e.message);
    return [];
  }
}

/* ── Export ───────────────────────────────────────────────────────── */

module.exports = { getStreams: getStreams };
