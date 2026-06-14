// Phase 9 — Report Writer.
// Writes a per-test summary that is OBSERVATION-ONLY — it records
// what the runtime did, not what the test told the runtime to do.
// This is the structural enforcement of the contract: the test
// cannot tell the report what happened.

import fs from 'node:fs';
import path from 'node:path';

export class ReportWriter {
  constructor({ reportDir, runId }) {
    this._reportDir = reportDir;
    this._runId = runId;
    this._assertions = 0;
    this._assertionsFailed = 0;
    this._extras = {};
    fs.mkdirSync(this._reportDir, { recursive: true });
  }

  bumpAssertions(n = 1) { this._assertions += n; }
  recordFailure(n = 1) { this._assertionsFailed += n; }
  addExtra(key, value) { this._extras[key] = value; }

  writeSummary(suite, testName) {
    const file = path.join(this._reportDir, `${suite}__${testName}.summary.json`);
    fs.writeFileSync(file, JSON.stringify({
      suite,
      testName,
      runId: this._runId,
      assertions: this._assertions,
      assertionsFailed: this._assertionsFailed,
      extras: this._extras,
    }, null, 2));
  }
}
