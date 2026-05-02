#!/usr/bin/env node
/**
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │ MinuteTemp Dallas — Background Worker / Cron                        │
 * │ Runs every 2 minutes, collects all available data for Dallas,       │
 * │ scores models, logs forecast vs actual comparisons.                 │
 * │                                                                      │
 * │ Setup:                                                               │
 * │   npm install node-cron axios                                        │
 * │   MT_API_KEY=your_key node worker.js                                 │
 * └──────────────────────────────────────────────────────────────────────┘
 */
'use strict';

const axios = require('axios');
const fs    = require('fs');
const path  = require('path');
const cron  = require('node-cron');

// ── CONFIG ────────────────────────────────────────────────────────────────────
const CONFIG = {
  apiKey:          process.env.MT_API_KEY || '',
  apiBase:         'https://api.minutetemp.com/api/v1',

  // Dallas stations (ASOS) — primary is DFW Int'l, secondary is Love Field
  stations:        ['KDFW', 'KDAL'],
  primaryStation:  'KDFW',

  // MinuteTemp city slug for Dallas
  city:            'dal',

  // History: how many days of past observations to keep for accuracy tracking
  historyDays:     14,

  // Data directory (JSON persistence between runs)
  dataDir:         path.join(__dirname, 'data'),

  // Cron schedule: every 2 minutes (halved to share rate limit with dashboard)
  cronSchedule:     '*/2 * * * *',

  // History pull every 6 hours
  cronScheduleHist: '0 */6 * * *',

  // Alert thresholds (console warnings)
  spreadAlertThresholdF: 5,  // warn if model spread exceeds this
  maeAlertThresholdF:    3,  // warn if best-model MAE exceeds this over 5 days
};

// ── PATHS ─────────────────────────────────────────────────────────────────────
const PATHS = {
  state:   path.join(CONFIG.dataDir, 'state.json'),
  history: path.join(CONFIG.dataDir, 'history.json'),
  scores:  path.join(CONFIG.dataDir, 'scores.json'),
  log:     path.join(CONFIG.dataDir, 'worker.log'),
};

// ── INIT ──────────────────────────────────────────────────────────────────────
if (!fs.existsSync(CONFIG.dataDir)) fs.mkdirSync(CONFIG.dataDir, { recursive: true });

// ── LOGGER ────────────────────────────────────────────────────────────────────
function log(msg, level = 'INFO') {
  const ts   = new Date().toISOString();
  const line = `[${ts}] [${level.padEnd(5)}] ${msg}`;
  console.log(line);
  fs.appendFileSync(PATHS.log, line + '\n');
}
function warn(msg)  { log(msg, 'WARN');  }
function error(msg) { log(msg, 'ERROR'); }
function ok(msg)    { log(msg, 'OK');    }

// ── PERSISTENCE ───────────────────────────────────────────────────────────────
function loadJson(filePath, fallback = {}) {
  try {
    if (fs.existsSync(filePath)) {
      return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    }
  } catch (e) { error(`Failed to load ${filePath}: ${e.message}`); }
  return fallback;
}

function saveJson(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  } catch (e) { error(`Failed to save ${filePath}: ${e.message}`); }
}

// ── API CLIENT ────────────────────────────────────────────────────────────────
const api = axios.create({
  baseURL: CONFIG.apiBase,
  headers: { 'X-API-Key': CONFIG.apiKey },
  timeout: 15000,
});

async function get(endpoint, params = {}) {
  try {
    const res = await api.get(endpoint, { params });
    return res.data?.data ?? res.data;
  } catch (e) {
    const msg = e.response?.data?.error?.message || e.response?.data?.message || e.message;
    throw new Error(`GET ${endpoint} → ${e.response?.status || 'ERR'}: ${msg}`);
  }
}

// ── SLEEP HELPER ──────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── DATA FETCHERS ─────────────────────────────────────────────────────────────

/**
 * Latest ASOS observation for a station.
 * Returns: { station, observation, daily_high_f, daily_low_f, ... }
 */
async function fetchObservation(station) {
  log(`Fetching observation: ${station}`);
  return get(`/stations/${station}/observations/latest`);
}

/**
 * All forecast model hourly data for a station.
 * Returns: { station, forecasts: [{ model_id, hourly: [...] }] }
 */
async function fetchForecast(station) {
  log(`Fetching forecast models: ${station}`);
  return get(`/stations/${station}/forecast`);
}

/**
 * Oracle model accuracy scores — real server-side MAE per model.
 * Replaces manual historical scoring.
 */
async function fetchOracleScores(station, days = 14) {
  log(`Fetching oracle scores: ${station} (${days}-day window)`);
  try {
    return await get(`/stations/${station}/oracle-scores`, {
      days,
      mode:    'day_ahead',
      rank_by: 'high',
    });
  } catch (e) {
    warn(`Oracle scores unavailable for ${station}: ${e.message}`);
    return null;
  }
}

/**
 * CLI station reports for daily high/low actuals.
 * Replaces the old /observations/historical endpoint.
 */
async function fetchDailyReports(station, days = 5) {
  log(`Fetching ${days}-day CLI reports: ${station}`);
  try {
    const data = await get(`/stations/${station}/reports/history`, { type: 'cli' });
    const reports = data?.reports || [];
    // Deduplicate by date, keep most recent revision
    const seen = new Set();
    return reports
      .filter(r => {
        if (seen.has(r.report_date)) return false;
        seen.add(r.report_date);
        return true;
      })
      .slice(0, days);
  } catch (e) {
    warn(`CLI reports unavailable for ${station}: ${e.message}`);
    return [];
  }
}

/**
 * Active weather prediction markets for the city.
 * Uses the correct /markets?city= endpoint (not /cities/:slug/brackets).
 */
async function fetchMarkets(city) {
  log(`Fetching markets: ${city}`);
  try {
    return await get(`/markets`, { city, platform: 'kalshi' });
  } catch (e) {
    warn(`Markets unavailable: ${e.message}`);
    return null;
  }
}

/**
 * Active weather events for the station (METAR/SPECI driven).
 */
async function fetchWeatherEvents(station) {
  try {
    return await get(`/stations/${station}/weather-events`);
  } catch (e) {
    warn(`Weather events unavailable for ${station}: ${e.message}`);
    return null;
  }
}

/**
 * Station metadata.
 */
async function fetchStationMeta(station) {
  try {
    return await get(`/stations/${station}`);
  } catch (e) {
    warn(`Station meta unavailable for ${station}: ${e.message}`);
    return null;
  }
}

// ── SCORING ENGINE ────────────────────────────────────────────────────────────
/**
 * Condition-Aware Reliability Score (0–100) per forecast model.
 *
 * Components:
 * A) Consensus Score (30 pts)
 *    — Proximity to ensemble mean.
 *
 * B) Oracle MAE Score (50 pts)
 *    — Real server-side MAE from MinuteTemp oracle endpoint.
 *    — 0°F MAE → 50 pts, 6°F MAE → 0 pts (linear).
 *    — Falls back to 20 pts default when oracle unavailable.
 *
 * C) Convergence Bonus (20 pts)
 *    — When overall spread is low (< 2°F), ALL models earn the bonus.
 *    — When spread is high (> 6°F), only models within ±1°F of mean earn it.
 *
 * @param {Array}  forecasts   — array of { model_id, hourly:[{ temperature_2m_f }] }
 * @param {Array}  oracleData  — array of { model_id, high_mae, high_bias, combined_mae }
 * @returns {Object}           — { [model_id]: { total, consensus, mae, convergence, temp, dev, bias } }
 */
function scoreModels(forecasts, oracleData = []) {
  const hourlyTemps = forecasts.map(f => ({
    model: f.model_id,
    temp:  f.hourly?.[0]?.temperature_2m_f ?? null,
  })).filter(m => m.temp !== null);

  if (!hourlyTemps.length) return {};

  const temps   = hourlyTemps.map(m => m.temp);
  const n       = temps.length;
  const mean    = temps.reduce((a, b) => a + b, 0) / n;
  const spread  = Math.max(...temps) - Math.min(...temps);
  const maxDev  = Math.max(...hourlyTemps.map(m => Math.abs(m.temp - mean))) || 1;

  if (spread >= CONFIG.spreadAlertThresholdF) {
    warn(`⚠ High model spread: ${spread.toFixed(1)}°F (mean ${mean.toFixed(1)}°F)`);
  }

  // Build oracle lookup
  const oracleLookup = {};
  (oracleData || []).forEach(o => { oracleLookup[o.model_id] = o; });

  const scores = {};
  hourlyTemps.forEach(({ model, temp }) => {
    const dev = Math.abs(temp - mean);
    const o   = oracleLookup[model];

    // A) Consensus (30 pts)
    const consensusScore = (1 - dev / maxDev) * 30;

    // B) Oracle MAE (50 pts)
    let maeScore = 20; // default when no oracle data
    if (o) {
      const mae = o.high_mae ?? o.combined_mae ?? 5;
      maeScore  = Math.max(0, 50 * (1 - mae / 6));
    }

    // C) Convergence bonus (20 pts)
    let convergenceScore;
    if (spread < 2) {
      convergenceScore = 20;
    } else if (spread >= 6) {
      convergenceScore = dev < 1 ? 20 : 0;
    } else {
      convergenceScore = Math.max(0, (1 - dev / (spread / 2)) * 20);
    }

    const total = Math.min(100, Math.round(consensusScore + maeScore + convergenceScore));
    scores[model] = {
      total,
      consensus:   Math.round(consensusScore),
      mae:         Math.round(maeScore),
      convergence: Math.round(convergenceScore),
      temp,
      dev:         parseFloat(dev.toFixed(2)),
      bias:        o?.high_bias   ?? null,
      mae_actual:  o?.high_mae    ?? null,
    };
  });

  return scores;
}

// ── MAIN CYCLE ────────────────────────────────────────────────────────────────
async function runCycle() {
  if (!CONFIG.apiKey) {
    error('No API key! Set MT_API_KEY env variable.');
    return;
  }

  log('─── Starting data cycle ───');
  const cycleStart = Date.now();

  // Load persisted state
  const state   = loadJson(PATHS.state,   { observations: {}, forecasts: {} });
  const scores  = loadJson(PATHS.scores,  {});
  const results = { timestamp: new Date().toISOString() };

  // 1. Fetch observations for all Dallas stations
  results.observations = {};
  for (const station of CONFIG.stations) {
    try {
      results.observations[station] = await fetchObservation(station);
      const obs = results.observations[station]?.observation;
      const hi  = results.observations[station]?.daily_high_f?.toFixed(1) ?? '?';
      const lo  = results.observations[station]?.daily_low_f?.toFixed(1)  ?? '?';
      ok(`${station} obs: ${obs?.temperature_f?.toFixed(1)}°F (high ${hi}°, low ${lo}°)`);
    } catch (e) {
      error(`Observation failed for ${station}: ${e.message}`);
    }
    await sleep(1000); // small gap between station calls
  }

  // 2. Fetch all forecast models for primary station
  let forecastData = null;
  try {
    forecastData = await fetchForecast(CONFIG.primaryStation);
    const count  = forecastData?.forecasts?.length ?? 0;
    ok(`Received ${count} forecast model(s) from MinuteTemp`);
  } catch (e) {
    error(`Forecast fetch failed: ${e.message}`);
  }

  await sleep(1000);

  // 3. Fetch oracle scores (replaces manual history MAE scoring)
  let oracleData = null;
  try {
    oracleData = await fetchOracleScores(CONFIG.primaryStation, CONFIG.historyDays);
    const count = oracleData?.scores?.length ?? 0;
    ok(`Oracle scores: ${count} model(s)`);
  } catch (e) {
    warn(`Oracle scores fetch failed: ${e.message}`);
  }

  await sleep(1000);

  // 4. Fetch markets (replaces old /cities/dal/brackets endpoint)
  const markets = await fetchMarkets(CONFIG.city);
  if (markets) ok(`Market data: ${Array.isArray(markets) ? markets.length : 0} markets`);

  await sleep(1000);

  // 5. Fetch weather events
  const events = await fetchWeatherEvents(CONFIG.primaryStation);
  if (events) {
    const active = events?.active?.length ?? 0;
    ok(`Weather events: ${active} active`);
  }

  // 6. Score models
  if (forecastData?.forecasts?.length) {
    const oracleScores = oracleData?.scores || [];
    const modelScores  = scoreModels(forecastData.forecasts, oracleScores);

    // Determine top pick
    const ranked = Object.entries(modelScores).sort((a, b) => b[1].total - a[1].total);
    if (ranked.length) {
      const [topModel, topScore] = ranked[0];
      ok(`★ Top Model: ${topModel} (score: ${topScore.total}/100 | consensus: ${topScore.consensus} | mae: ${topScore.mae} | convergence: ${topScore.convergence})`);
      if (topScore.mae_actual !== null) {
        ok(`  ${topModel} oracle MAE: ${topScore.mae_actual.toFixed(2)}°F | bias: ${topScore.bias?.toFixed(2) ?? '?'}°F`);
        if (topScore.mae_actual > CONFIG.maeAlertThresholdF) {
          warn(`  ${topModel} MAE exceeds threshold (${CONFIG.maeAlertThresholdF}°F)`);
        }
      }

      log('Full model ranking:');
      ranked.forEach(([m, s], i) => {
        log(`  ${String(i + 1).padStart(2)}. ${m.padEnd(30)} score:${String(s.total).padStart(4)} | mae_actual:${s.mae_actual != null ? s.mae_actual.toFixed(2) : '—'} | bias:${s.bias != null ? s.bias.toFixed(2) : '—'}`);
      });
    }

    // Save scores
    saveJson(PATHS.scores, {
      updatedAt: new Date().toISOString(),
      scores:    modelScores,
      ranked:    ranked.map(([m, s]) => ({ model: m, ...s })),
      spread:    Math.max(...Object.values(modelScores).map(s => s.temp ?? 0)) -
                 Math.min(...Object.values(modelScores).map(s => s.temp ?? 0)),
      mean:      Object.values(modelScores).reduce((a, s) => a + (s.temp ?? 0), 0) /
                 Object.values(modelScores).length,
    });
  }

  // 7. Save full state
  state.observations = results.observations;
  state.forecasts    = forecastData ?? state.forecasts;
  state.markets      = markets      ?? state.markets;
  state.events       = events       ?? state.events;
  state.oracle       = oracleData   ?? state.oracle;
  state.updatedAt    = new Date().toISOString();
  saveJson(PATHS.state, state);

  const elapsed = ((Date.now() - cycleStart) / 1000).toFixed(1);
  log(`─── Cycle complete in ${elapsed}s ───`);
}

// ── HISTORY CYCLE ─────────────────────────────────────────────────────────────
async function runHistoryCycle() {
  log('─── Starting history pull ───');
  const history = loadJson(PATHS.history, []);

  try {
    const rawReports = await fetchDailyReports(CONFIG.primaryStation, CONFIG.historyDays);

    if (rawReports.length) {
      // Merge with existing, dedupe by date
      const merged   = [...rawReports];
      const rawDates = new Set(rawReports.map(d => d.report_date));
      history.filter(d => !rawDates.has(d.report_date)).forEach(d => merged.push(d));
      merged.sort((a, b) => new Date(b.report_date) - new Date(a.report_date));
      const trimmed = merged.slice(0, CONFIG.historyDays);
      saveJson(PATHS.history, trimmed);
      ok(`History saved: ${trimmed.length} day(s)`);

      // Print 5-day summary
      const h5 = trimmed.slice(0, 5);
      if (h5.length) {
        log('5-Day CLI Report Summary (KDFW):');
        h5.forEach(day => {
          log(`  ${(day.report_date || '?').padEnd(12)} high: ${day.max_temp_f?.toFixed(1) ?? '?'}°F  low: ${day.min_temp_f?.toFixed(1) ?? '?'}°F`);
        });
      }
    } else {
      log('No new CLI report data from API');
    }
  } catch (e) {
    error(`History cycle failed: ${e.message}`);
  }
}

// ── STARTUP ───────────────────────────────────────────────────────────────────
async function main() {
  log('════════════════════════════════════════════════════════');
  log(' MinuteTemp · Dallas Weather Intelligence Worker');
  log(` Primary station : ${CONFIG.primaryStation}`);
  log(` City            : ${CONFIG.city}`);
  log(` Data dir        : ${CONFIG.dataDir}`);
  log(` Cycle           : every 2 minutes`);
  log(` History pull    : every 6 hours`);
  log('════════════════════════════════════════════════════════');

  if (!CONFIG.apiKey) {
    error('MT_API_KEY not set. Export it and restart.');
    error('  export MT_API_KEY="your_key_here"');
    process.exit(1);
  }

  // Run immediately on startup
  await runCycle();
  await runHistoryCycle();

  // Schedule recurring jobs
  cron.schedule(CONFIG.cronSchedule, async () => {
    try { await runCycle(); }
    catch (e) { error(`Cycle error: ${e.message}`); }
  });

  cron.schedule(CONFIG.cronScheduleHist, async () => {
    try { await runHistoryCycle(); }
    catch (e) { error(`History cycle error: ${e.message}`); }
  });

  log('Worker running. Press Ctrl+C to stop.');
}

main().catch(e => { error(`Fatal: ${e.message}`); process.exit(1); });
