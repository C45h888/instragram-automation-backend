// Phase 8 — Per-test JSON Report Writer.
//
// Every phase-8 test writes its own JSON to:
//   tests/phase-8/reports/<suite>/<test-name>.<run-id>.json
//
// Shape:
//   {
//     run_id, test_name, suite, timestamp, started_at, finished_at,
//     status: "pass"|"fail",
//     vitest_assertion_count,
//     constitutional_summary: [...],
//     drift_findings: [...],
//     event_ids: [...],
//     timeline_sample: [...],
//     extras: {...},
//     error: null | { message, stack, label },
//     duration_ms
//   }

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const REPORT_DIR = process.env.PHASE8_REPORT_DIR
  || path.join(__dirname, '..', 'reports');

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

export class ReportWriter {
  constructor({ suite, testName }) {
    this.suite = suite;
    this.testName = testName;
    this.runId = process.env.PHASE8_RUN_ID || new Date().toISOString().replace(/[:.]/g, '-');
    this.reportPath = path.join(REPORT_DIR, suite, `${this.testName}.${this.runId}.json`);
    this.report = {
      run_id: this.runId,
      test_name: testName,
      suite,
      timestamp: new Date().toISOString(),
      started_at: Date.now(),
      finished_at: null,
      status: 'pass',
      vitest_assertion_count: 0,
      constitutional_summary: [],
      drift_findings: [],
      event_ids: [],
      timeline_sample: [],
      extras: {},
      error: null,
      duration_ms: 0,
    };
    ensureDir(path.dirname(this.reportPath));
  }

  addExtra(key, value)     { this.report.extras[key] = value; }
  addDrift(finding)        { this.report.drift_findings.push(finding); }
  addConstitutional(s)     { this.report.constitutional_summary.push(s); }
  setEventIds(ids)         { this.report.event_ids = ids; }
  setTimeline(t)           { this.report.timeline_sample = t.slice(0, 200); }
  bumpAssertions(n = 1)    { this.report.vitest_assertion_count += n; }

  fail(err, label) {
    this.report.status = 'fail';
    this.report.error = {
      message: err?.message || String(err),
      stack:   err?.stack   || null,
      label:   label || null,
    };
    this.finish();
  }

  finish() {
    this.report.finished_at = Date.now();
    this.report.duration_ms = this.report.finished_at - this.report.started_at;
    fs.writeFileSync(this.reportPath, JSON.stringify(this.report, null, 2));
    return this.reportPath;
  }
}

export function writeReportSync(opts) {
  const w = new ReportWriter(opts);
  w.finish();
  return w.reportPath;
}

export default { ReportWriter, writeReportSync };
