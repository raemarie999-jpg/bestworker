#!/usr/bin/env node
/**
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │  MinuteTemp Dallas — Background Worker / Cron                        │
 * │  Runs every 60 seconds, collects all available data for Dallas,      │
 * │  scores models, logs forecast vs actual comparisons.                 │
 * │                                                                      │
 * │  Setup:                                                              │
 * │    npm install node-cron axios                                       │
 * │    MT_API_KEY=your_key node worker.js                                │
 * │                                                                      │
 * │  Or add to crontab (runs as script, no node-cron needed):           │
 * │    * * * * * MT_API_KEY=your_key node /path/to/worker.js            │
 * └──────────────────────────────────────────────────────────────────────┘
 */

'use strict';

const axios   = require('axios');
const fs      = require('fs');
const path    = require('path');
const cron    = require('node-cron');

// ── CONFIG ────────────────────────────────────────────────────────────────────
const CONFIG = {
  apiKey:           process.env.MT_API_KEY || '',
  apiBase:          'https://api.minutetemp.com/api/v1',

  // Dallas stations (ASOS) — primary is DFW Int'l, secondary is Love Field
  stations:         ['KDFW', 'KDAL'],
  primaryStation:   'KDFW',

  // MinuteTemp city slug for Dallas
  city:             'dal',

  // History: how many days of past observations to keep for accuracy tracking
  historyDays:      30,

  // Data directory (JSON persistence between runs)
  dataDir:          path.join(__dirname, 'data'),

  // Cron schedule: every 60 seconds
  cronSchedule:     '* * * * *',       // every minute
  cronScheduleHist: '0 */6 * * *',     // history pull every 6 hours

  // Alert thresholds (console warnings)
  spreadAlertThresholdF: 5,   // warn if model spread exceeds this
  maeAlertThresholdF:    3,   // warn if best-model MAE exceeds this over 5 days
};

// ── PATHS ─────────────────────────────────────────────────────────────────────
const PATHS = {
  state:    path.join(CONFIG.dataDir, 'state.json'),
  history:  path.join(CONFIG.dataDir, 'history.json'),
  scores:   path.join(CONFIG.dataDir, 'scores.json'),
  log:      path.join(CONFIG.dataDir, 'worker.log'),
};

// ── INIT ─────────────────────────────────────────────────────────────────────
if (!fs.existsSync(CONFIG.dataDir)) fs.mkdirSync(CONFIG.dataDir, { recursive: true });

// ── LOGGER ───────────────────────────────────────────────────────────────────
function log(msg, level = 'INFO') {
  const ts  = new Date().toISOString();
  const line = `[${ts}] [${level.padEnd(5)}] ${msg}`;
  console.log(line);
  fs.appendFileSync(PATHS.log, line + '\n');
}

function warn(msg)  { log(msg, 'WARN'); }
function error(msg) { log(msg, 'ERROR'); }
function ok(msg)    { log(msg, 'OK'); }

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
    const msg = e.response?.data?.message || e.message;
    throw new Error(`GET ${endpoint} → ${e.response?.status || 'ERR'}: ${msg}`);
  }
}

// ── DATA FETCHERS ──────────────────────────────────────────────────────────────
/**
 * Latest ASOS observation for a station.
 * Returns: { station, observation, daily_high_f, daily_low_f, ... }
 */
async function fetchObservation(station) {
  log(`Fetching observation: ${station}`);
  return get(`/stations/${station}/observations/latest`);
}

/**
 * All 20 forecast model hourly data for a station.
 * Returns: { station_id, forecasts: [{ model_id, hourly: [...] }] }
 */
async function fetchForecast(station) {
  log(`Fetching 20 forecast models: ${station}`);
  return get(`/stations/${station}/forecast`);
}

/**
 * Historical daily observations — used for accuracy scoring.
 * Falls back gracefully if the endpoint isn't on the current plan.
 */
async function fetchHistory(station, days = 5) {
  log(`Fetching ${days}-day history: ${station}`);
  try {
    return await get(`/stations/${station}/observations/historical`, { days });
  } catch (e) {
    warn(`History unavailable for ${station}: ${e.message}`);
    return [];
  }
}

/**
 * Market bracket probabilities for the city.
 */
async function fetchBrackets(city) {
  log(`Fetching market brackets: ${city}`);
  try {
    return await get(`/cities/${city}/brackets`);
  } catch (e) {
    warn(`Brackets unavailable: ${e.message}`);
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

// ── SCORING ENGINE ─────────────────────────────────────────────────────────────
/**
 * Reliability Score (0–100) per forecast model.
 *
 * Components:
 *   A) Consensus Score (40 pts)
 *      — Proximity to ensemble mean. Models closest to the consensus
 *        of all 20 models score highest. If only 2 outliers diverge,
 *        the majority cluster is rewarded.
 *
 *   B) Historical MAE Score (40 pts)
 *      — Mean Absolute Error of the model's daily-high forecast vs
 *        actual ASOS observation over the past N days.
 *        MAE 0°F → 40 pts, MAE 5°F → 0 pts (linear interpolation).
 *
 *   C) Convergence Bonus (20 pts)
 *      — When overall spread is low (< 2°F), ALL models in the tight
 *        cluster earn the bonus. When spread is high (> 6°F), only
 *        models within ±1°F of the mean earn the bonus.
 *
 * @param {Array}  forecasts    — array of { model_id, hourly:[{ temperature_2m_f }] }
 * @param {Object} histErrors   — { model_id: mae_f } from past history
 * @returns {Object}            — { [model_id]: { total, consensus, mae, convergence, temp, dev } }
 */
function scoreModels(forecasts, histErrors = {}) {
  const hourlyTemps = forecasts.map(f => ({
    model:  f.model_id,
    temp:   f.hourly?.[0]?.temperature_2m_f ?? null,
  })).filter(m => m.temp !== null);

  if (!hourlyTemps.length) return {};

  const temps   = hourlyTemps.map(m => m.temp);
  const n       = temps.length;
  const mean    = temps.reduce((a, b) => a + b, 0) / n;
  const spread  = Math.max(...temps) - Math.min(...temps);
  const maxDev  = Math.max(...hourlyTemps.map(m => Math.abs(m.temp - mean)));

  // Warn if spread is unusually high
  if (spread >= CONFIG.spreadAlertThresholdF) {
    warn(`⚠  High model spread: ${spread.toFixed(1)}°F (mean ${mean.toFixed(1)}°F)`);
  }

  const scores = {};

  hourlyTemps.forEach(({ model, temp }) => {
    const dev = Math.abs(temp - mean);

    // A) Consensus (40 pts)
    const consensusScore = maxDev > 0 ? (1 - dev / maxDev) * 40 : 40;

    // B) Historical MAE (40 pts)
    let maeScore = 20; // default when no history available
    if (model in histErrors) {
      const mae = histErrors[model];
      maeScore = Math.max(0, 40 * (1 - mae / 5)); // 0°→40, 5°→0
    }

    // C) Convergence bonus (20 pts)
    let convergenceScore;
    if (spread < 2) {
      convergenceScore = 20; // tight cluster: everyone earns it
    } else if (spread >= 6) {
      convergenceScore = dev < 1 ? 20 : 0; // wide spread: only near-mean models earn it
    } else {
      // Linear: within spread/2 earns partial credit
      convergenceScore = Math.max(0, (1 - (dev / (spread / 2))) * 20);
    }

    const total = Math.min(100, Math.round(consensusScore + maeScore + convergenceScore));

    scores[model] = {
      total,
      consensus:    Math.round(consensusScore),
      mae:          Math.round(maeScore),
      convergence:  Math.round(convergenceScore),
      temp,
      dev:          parseFloat(dev.toFixed(2)),
      mae_actual:   histErrors[model] ?? null,
    };
  });

  return scores;
}

/**
 * Derive per-model historical MAE from stored history records.
 * Each history record: { date, actual_high_f, model_forecasts: [{model_id, forecast_high_f}] }
 */
function buildHistErrors(historyRecords) {
  const sums   = {};
  const counts = {};

  historyRecords.forEach(day => {
    (day.model_forecasts || []).forEach(mf => {
      if (mf.forecast_high_f != null && day.actual_high_f != null) {
        const err = Math.abs(mf.forecast_high_f - day.actual_high_f);
        sums[mf.model_id]   = (sums[mf.model_id]   || 0) + err;
        counts[mf.model_id] = (counts[mf.model_id] || 0) + 1;
      }
    });
  });

  const mae = {};
  Object.keys(sums).forEach(m => { mae[m] = sums[m] / counts[m]; });
  return mae;
}

// ── MAIN CYCLE ─────────────────────────────────────────────────────────────────
async function runCycle() {
  if (!CONFIG.apiKey) {
    error('No API key! Set MT_API_KEY env variable.');
    return;
  }

  log('─── Starting data cycle ───');
  const cycleStart = Date.now();

  // Load persisted state
  const state   = loadJson(PATHS.state,   { observations: {}, forecasts: {} });
  const history = loadJson(PATHS.history, []);
  const scores  = loadJson(PATHS.scores,  {});

  const results = { timestamp: new Date().toISOString() };

  // 1. Fetch observations for all Dallas stations
  results.observations = {};
  for (const station of CONFIG.stations) {
    try {
      results.observations[station] = await fetchObservation(station);
      const obs = results.observations[station]?.observation;
      ok(`${station} obs: ${obs?.temperature_f?.toFixed(1)}°F (high ${results.observations[station]?.daily_high_f?.toFixed(1)}°, low ${results.observations[station]?.daily_low_f?.toFixed(1)}°)`);
    } catch (e) {
      error(`Observation failed for ${station}: ${e.message}`);
    }
  }

  // 2. Fetch all 20 forecast models for primary station
  let forecastData = null;
  try {
    forecastData = await fetchForecast(CONFIG.primaryStation);
    const count = forecastData?.forecasts?.length ?? 0;
    ok(`Received ${count} forecast model(s) from MinuteTemp`);
  } catch (e) {
    error(`Forecast fetch failed: ${e.message}`);
  }

  // 3. Fetch market brackets
  const brackets = await fetchBrackets(CONFIG.city);
  if (brackets) ok(`Bracket data: ${brackets.brackets?.length ?? 0} brackets`);

  // 4. Score models
  if (forecastData?.forecasts?.length) {
    const histErrors = buildHistErrors(history);
    const modelScores = scoreModels(forecastData.forecasts, histErrors);

    // Determine top pick
    const ranked = Object.entries(modelScores).sort((a, b) => b[1].total - a[1].total);
    if (ranked.length) {
      const [topModel, topScore] = ranked[0];
      ok(`★ Top Model: ${topModel} (score: ${topScore.total}/100 | consensus:${topScore.consensus} mae:${topScore.mae} convergence:${topScore.convergence})`);

      if (topScore.mae_actual !== null) {
        ok(`  ${topModel} 5-day MAE: ${topScore.mae_actual.toFixed(2)}°F`);
        if (topScore.mae_actual > CONFIG.maeAlertThresholdF) {
          warn(`  ${topModel} MAE exceeds threshold (${CONFIG.maeAlertThresholdF}°F)`);
        }
      }

      log(`Full model ranking:`);
      ranked.forEach(([m, s], i) => {
        log(`  ${String(i + 1).padStart(2)}. ${m.padEnd(12)} score:${String(s.total).padStart(3)} temp:${s.temp?.toFixed(1).padStart(6)}°F dev:${s.dev >= 0 ? '+' : ''}${s.dev.toFixed(1)}°`);
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

  // 5. Save full state
  state.observations = results.observations;
  state.forecasts    = forecastData ?? state.forecasts;
  state.brackets     = brackets ?? state.brackets;
  state.updatedAt    = new Date().toISOString();
  saveJson(PATHS.state, state);

  const elapsed = ((Date.now() - cycleStart) / 1000).toFixed(1);
  log(`─── Cycle complete in ${elapsed}s ───`);
}

// ── HISTORY PULL ───────────────────────────────────────────────────────────────
async function runHistoryCycle() {
  log('─── Starting history pull ───');

  const history = loadJson(PATHS.history, []);
  const state   = loadJson(PATHS.state,   {});

  try {
    const rawHist = await fetchHistory(CONFIG.primaryStation, CONFIG.historyDays);

    if (Array.isArray(rawHist) && rawHist.length) {
      // Merge with existing history (dedupe by date)
      const merged = [...rawHist];
      const rawDates = new Set(rawHist.map(d => d.date));
      history.filter(d => !rawDates.has(d.date)).forEach(d => merged.push(d));
      merged.sort((a, b) => new Date(b.date) - new Date(a.date));
      const trimmed = merged.slice(0, CONFIG.historyDays);

      // Add any cached forecasts as model_forecasts
      // (this would be populated if we were storing at forecast time)
      saveJson(PATHS.history, trimmed);
      ok(`History saved: ${trimmed.length} day(s)`);
    } else {
      log('No new history data from API (may not be on historical plan)');
    }
  } catch (e) {
    error(`History cycle failed: ${e.message}`);
  }

  // Print 5-day accuracy summary
  const h5 = history.slice(0, 5);
  if (h5.length) {
    log('5-Day Forecast vs Actual Summary (KDFW):');
    const errs = [];
    h5.forEach(day => {
      const err = Math.abs((day.consensus_forecast_f ?? 0) - (day.actual_high_f ?? 0));
      errs.push(err);
      const grade = err < 1 ? 'EXCELLENT' : err < 2.5 ? 'GOOD' : err < 4 ? 'FAIR' : 'POOR';
      log(`  ${(day.date || '?').padEnd(16)} actual: ${(day.actual_high_f?.toFixed(1) ?? '?').padStart(5)}°F  fcst: ${(day.consensus_forecast_f?.toFixed(1) ?? '?').padStart(5)}°F  err: ${err.toFixed(1).padStart(4)}°  [${grade}]`);
    });
    const avgMAE = errs.reduce((a, b) => a + b, 0) / errs.length;
    log(`  5-day avg MAE: ${avgMAE.toFixed(2)}°F`);
  }
}

// ── STARTUP ───────────────────────────────────────────────────────────────────
async function main() {
  log('════════════════════════════════════════════════════════');
  log('  MinuteTemp · Dallas Weather Intelligence Worker');
  log(`  Primary station: ${CONFIG.primaryStation}`);
  log(`  City: ${CONFIG.city}`);
  log(`  Data dir: ${CONFIG.dataDir}`);
  log(`  Cycle: every 60 seconds`);
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
