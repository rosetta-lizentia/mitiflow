// Reactive cluster state (fed from SSE /api/v1/stream/cluster).

import { createSSE } from "$lib/sse";
import type { ClusterEvent, NodeInfo } from "$lib/types";

let nodes = $state<Record<string, NodeInfo>>({});
let connected = $state(false);
let sse: { close: () => void } | null = null;

function handleEvent(event: ClusterEvent) {
  const id = event.node_id;
  switch (event.type) {
    case "node_online":
      if (!nodes[id]) {
        nodes[id] = { online: true, last_seen: event.timestamp };
      } else {
        nodes[id].online = true;
        nodes[id].last_seen = event.timestamp;
      }
      break;
    case "node_offline":
      if (nodes[id]) {
        nodes[id].online = false;
        nodes[id].last_seen = event.timestamp;
      }
      break;
    case "node_health": {
      const { type: _, node_id: __, ...data } = event;
      if (nodes[id]) {
        nodes[id].health = data as unknown as NodeInfo["health"];
      }
      break;
    }
    case "node_status": {
      const { type: _, node_id: __, ...data } = event;
      if (nodes[id]) {
        nodes[id].status = data as unknown as NodeInfo["status"];
      }
      break;
    }
  }
}

export function startClusterSSE() {
  if (sse) return;
  connected = true;

  // Listen to all cluster event types
  const eventTypes = [
    "node_online",
    "node_offline",
    "node_health",
    "node_status",
  ] as const;

  const sources = eventTypes.map((t) =>
    createSSE<ClusterEvent>("/api/v1/stream/cluster", t, handleEvent),
  );

  sse = {
    close: () => {
      sources.forEach((s) => s.close());
      connected = false;
      sse = null;
    },
  };
}

export function stopClusterSSE() {
  sse?.close();
}

export function getClusterNodes() {
  return nodes;
}

export function isClusterConnected() {
  return connected;
}

export function setClusterNodes(initial: Record<string, NodeInfo>) {
  nodes = initial;
}
