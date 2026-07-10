/**
 * All-in-One Aggregator — Nuvio Plugin
 *
 * Pipeline:
 *   Phase 1  →  Promise.all all fetches
 *   Phase 2  →  Synchronous eval of every returned source string
 *   Phase 3  →  Promise.allSettled all getStreams calls
 *   Phase 4  →  Smart merge, deduplicate, multi-factor sort
 *
 * Sort factors (in priority order):
 *   1. Quality tier  (2160p > 1080p > 720p > 480p > unknown)
 *   2. URL health    (direct video files > CDN > API > redirectors > junk)
 *   3. Format        (m3u8 > mkv/mp4 for streaming)
 *   4. File size     (larger = better encode, descending)
 *
 * Runtime: QuickJS (quickjs-kt) — CommonJS only, no Node built-ins.
 */

var MANIFEST_URL =
  'https://raw.githubusercontent.com/brotherguns/Plugin/refs/heads/main/combined_manifest.json';

/* ------------------------------------------------------------------ */
/*  Quality helpers                                                   */
/* ------------------------------------------------------------------ */

/**
 * Extract quality tier from an explicit quality string.
 * Returns 0–4 (lower = better) or -1 if nothing detected.
 */
function parseQualityString(q) {
  if (!q) return -1;
  var s = String(q).toLowerCase();
  if (s.indexOf('2160') !== -1 || s.indexOf('4k') !== -1 || s.indexOf('uhd') !== -1) return 0;
  if (s.indexOf('1080') !== -1) return 1;
  if (s.indexOf('720')  !== -1) return 2;
  if (s.indexOf('480')  !== -1 || s.indexOf('360') !== -1 || s.indexOf('240') !== -1) return 3;
  return -1;
}

/**
 * Try to infer quality from URL path and stream title/name.
 * Scrapers often embed quality in filenames even when the quality field is blank.
 */
function inferQuality(stream) {
  /* 1. Check the explicit field first */
  var rank = parseQualityString(stream.quality);
  if (rank !== -1) return rank;

  /* 2. Scan URL, title, and name for quality markers */
  var haystack = (
    (stream.url || '') + ' ' +
    (stream.title || '') + ' ' +
    (stream.name || '')
  ).toLowerCase();

  if (/2160p|\.4k\.|4k[-_. ]|uhd/i.test(haystack)) return 0;
  if (/1080p/i.test(haystack)) return 1;
  if (/720p/i.test(haystack))  return 2;
  if (/480p|360p|240p/i.test(haystack)) return 3;

  /* 3. "HD" without a specific resolution → assume 720p */
  if (/[\b\-_.]hd[\b\-_. ]/i.test(haystack) || /\.hdrip/i.test(haystack)) return 2;

  return 4; /* truly unknown */
}

/* ------------------------------------------------------------------ */
/*  URL health scoring                                                */
/* ------------------------------------------------------------------ */

/**
 * Known-bad URL patterns that are HTML pages, not video.
 * These should be pushed to the bottom regardless of quality.
 */
var BAD_URL_PATTERNS = [
  'gamerxyt.com/dl.php',
  'gamerxyt.com/hubcloud.php',
  'hubcloud.cx/drive/',
  'hubcloud.ist/drive/',
  '/generate.php',
];

/**
 * Direct video file extensions in the URL path.
 */
function hasVideoExtension(url) {
  var path = url.split('?')[0].toLowerCase();
  return /\.(m3u8|mp4|mkv|webm|avi|ts)$/i.test(path) ||
         path.indexOf('/playlist.m3u8') !== -1;
}

/**
 * Score URL health: 0 = best (direct file), 3 = worst (known junk).
 */
function urlHealthScore(url) {
  if (!url) return 3;
  var lower = url.toLowerCase();

  /* Known broken patterns → worst */
  for (var i = 0; i < BAD_URL_PATTERNS.length; i++) {
    if (lower.indexOf(BAD_URL_PATTERNS[i]) !== -1) return 3;
  }

  /* Direct video file → best */
  if (hasVideoExtension(url)) return 0;

  /* CDN / storage domains → good */
  if (lower.indexOf('r2.cloudflarestorage.com') !== -1 ||
      lower.indexOf('workers.dev') !== -1 ||
      lower.indexOf('ironbubble.site') !== -1 ||
      lower.indexOf('videocontentscdn') !== -1) return 1;

  /* Everything else (API endpoints, embed URLs) → acceptable */
  return 2;
}

/* ------------------------------------------------------------------ */
/*  Size + format helpers                                             */
/* ------------------------------------------------------------------ */

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
 * Format preference for streaming: m3u8 (adaptive) > mp4 > mkv > other.
 */
function formatScore(url) {
  if (!url) return 3;
  var lower = url.split('?')[0].toLowerCase();
  if (lower.indexOf('.m3u8') !== -1 || lower.indexOf('playlist.m3u8') !== -1) return 0;
  if (lower.indexOf('.mp4') !== -1) return 1;
  if (lower.indexOf('.mkv') !== -1) return 2;
  return 3;
}

/* ------------------------------------------------------------------ */
/*  Runtime helpers                                                   */
/* ------------------------------------------------------------------ */

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

    var codes = await Promise.all(urls.map(function (url) {
      return fetch(url).then(function (r) {
        if (!r.ok) return null;
        return r.text();
      }).catch(function () {
        return null;
      });
    }));

    /* ── Phase 2: Eval all source strings synchronously ──────────── */

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

    codes = null;

    console.log('AIO: ' + scraperFns.length + ' scrapers loaded');

    /* ── Phase 3: Call ALL getStreams simultaneously ──────────────── */

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

    var resultSets = [];
    for (var i = 0; i < settled.length; i++) {
      resultSets.push(settled[i].status === 'fulfilled' ? settled[i].value : []);
    }

    /* ── Phase 4: Merge → deduplicate → smart sort ───────────────── */

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

    /* Pre-compute sort keys to avoid recalculating per comparison */
    for (var i = 0; i < merged.length; i++) {
      var s = merged[i];
      s._qRank  = inferQuality(s);
      s._health = urlHealthScore(s.url);
      s._fmt    = formatScore(s.url);
      s._size   = parseSizeToBytes(s.size);
    }

    merged.sort(function (a, b) {
      /* 1. Quality tier — lower = better */
      if (a._qRank !== b._qRank) return a._qRank - b._qRank;

      /* 2. URL health — lower = better (direct files first, junk last) */
      if (a._health !== b._health) return a._health - b._health;

      /* 3. Format preference — m3u8 > mp4 > mkv */
      if (a._fmt !== b._fmt) return a._fmt - b._fmt;

      /* 4. File size descending — larger = better encode */
      return b._size - a._size;
    });

    /* Clean up temporary sort keys */
    for (var i = 0; i < merged.length; i++) {
      delete merged[i]._qRank;
      delete merged[i]._health;
      delete merged[i]._fmt;
      delete merged[i]._size;
    }

    console.log('AIO: returning ' + merged.length + ' streams');
    return merged;

  } catch (e) {
    console.error('AIO: top-level error — ' + e.message);
    return [];
  }
}

/* ── Export ───────────────────────────────────────────────────────── */

module.exports = { getStreams: getStreams };
