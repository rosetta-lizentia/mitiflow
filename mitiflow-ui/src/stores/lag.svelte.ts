// Reactive lag state (fed from SSE /api/v1/stream/lag).

import { createSSE } from "$lib/sse";
import type { LagReport } from "$lib/types";

let reports = $state<LagReport[]>([]);
let sse: { close: () => void } | null = null;
const MAX_REPORTS = 200;

export function startLagSSE(group?: string) {
  if (sse) sse.close();
  const url = group
    ? `/api/v1/stream/lag?group=${encodeURIComponent(group)}`
    : "/api/v1/stream/lag";

  sse = createSSE<LagReport>(url, "lag", (report) => {
    // Update existing or append, keep bounded
    const idx = reports.findIndex(
      (r) =>
        r.group_id === report.group_id && r.partition === report.partition,
    );
    if (idx >= 0) {
      reports[idx] = report;
    } else {
      reports = [...reports.slice(-(MAX_REPORTS - 1)), report];
    }
  });
}

export function stopLagSSE() {
  sse?.close();
  sse = null;
}

export function getLagReports() {
  return reports;
}

export function clearLagReports() {
  reports = [];
}
