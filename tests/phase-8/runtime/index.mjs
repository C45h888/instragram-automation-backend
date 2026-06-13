// Phase 8 — Public Runtime Surface.
// Tests import from this module:
//   import p8 from './runtime/index.mjs';
//   await p8.webhook.deliver('message-created');
//   p8.recorder.governance(eventId, decision);
//   const writer = new p8.ReportWriter({ suite, testName });

import { WebhookFixtures } from './webhook-fixtures.mjs';
import { recorder, ConstitutionalRecorder } from './constitutional-recorder.mjs';
import { probe, CrossKernelProbe } from './cross-kernel-probe.mjs';
import { parse, inferType, eventId } from './ingress-adapter.mjs';
import { ReportWriter, writeReportSync } from './report-writer.mjs';

export const webhook = WebhookFixtures;
export { recorder, probe, ReportWriter, writeReportSync };
export const ingress = { parse, inferType, eventId };

export { ConstitutionalRecorder, CrossKernelProbe, WebhookFixtures };

export default {
  webhook,
  recorder,
  probe,
  ingress,
  ReportWriter,
  writeReportSync,
  ConstitutionalRecorder,
  CrossKernelProbe,
  WebhookFixtures,
};
