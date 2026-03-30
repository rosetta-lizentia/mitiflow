// Recent events ring buffer (fed from SSE /api/v1/stream/events).

import { createSSE } from "$lib/sse";
import type { EventSummary } from "$lib/types";

let events = $state<EventSummary[]>([]);
let sse: { close: () => void } | null = null;
const MAX_EVENTS = 500;

export function startEventSSE(partition?: number) {
  if (sse) sse.close();
  let url = "/api/v1/stream/events";
  if (partition !== undefined) {
    url += `?partition=${partition}`;
  }

  sse = createSSE<EventSummary>(url, "message", (event) => {
    events = [...events.slice(-(MAX_EVENTS - 1)), event];
  });
}

export function stopEventSSE() {
  sse?.close();
  sse = null;
}

export function getEvents() {
  return events;
}

export function clearEvents() {
  events = [];
}
