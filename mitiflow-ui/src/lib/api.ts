// Typed fetch() wrappers for the Mitiflow REST API.

import type {
  TopicConfig,
  CreateTopicRequest,
  UpdateTopicRequest,
  ClusterStatus,
  NodeInfo,
  PartitionInfo,
  TopicLagSummary,
  PublisherInfo,
  ConsumerGroupSummary,
  DrainRequest,
  DrainResponse,
  AddOverridesRequest,
  OverrideTable,
  EventQueryResult,
  EventQueryParams,
  ResetOffsetsRequest,
} from "./types";

const BASE = "/api/v1";

class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new ApiError(res.status, body);
  }
  if (res.status === 204) return undefined as unknown as T;
  return res.json();
}

// ── Topics ──────────────────────────────────────────────────────────

export const listTopics = () => request<TopicConfig[]>("/topics");

export const getTopic = (name: string) =>
  request<TopicConfig>(`/topics/${encodeURIComponent(name)}`);

export const createTopic = (req: CreateTopicRequest) =>
  request<TopicConfig>("/topics", {
    method: "POST",
    body: JSON.stringify(req),
  });

export const updateTopic = (name: string, req: UpdateTopicRequest) =>
  request<TopicConfig>(`/topics/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify(req),
  });

export const deleteTopic = (name: string) =>
  request<void>(`/topics/${encodeURIComponent(name)}`, { method: "DELETE" });

export const getTopicPartitions = (name: string) =>
  request<PartitionInfo[]>(
    `/topics/${encodeURIComponent(name)}/partitions`,
  );

export const getTopicLag = (name: string) =>
  request<TopicLagSummary[]>(`/topics/${encodeURIComponent(name)}/lag`);

export const getTopicPublishers = (name: string) =>
  request<PublisherInfo[]>(
    `/topics/${encodeURIComponent(name)}/publishers`,
  );

// ── Cluster ─────────────────────────────────────────────────────────

export const getClusterNodes = () =>
  request<Record<string, NodeInfo>>("/cluster/nodes");

export const getNodePartitions = (id: string) =>
  request<import("./types").NodeTopicPartitions[]>(
    `/cluster/nodes/${encodeURIComponent(id)}/partitions`,
  );

export const getClusterStatus = () =>
  request<ClusterStatus>("/cluster/status");

export const getOverrides = () =>
  request<OverrideTable>("/cluster/overrides");

export const addOverrides = (req: AddOverridesRequest) =>
  request<void>("/cluster/overrides", {
    method: "POST",
    body: JSON.stringify(req),
  });

export const clearOverrides = () =>
  request<void>("/cluster/overrides", { method: "DELETE" });

export const drainNode = (id: string, req: DrainRequest = {}) =>
  request<DrainResponse>(
    `/cluster/nodes/${encodeURIComponent(id)}/drain`,
    { method: "POST", body: JSON.stringify(req) },
  );

export const undrainNode = (id: string) =>
  request<void>(
    `/cluster/nodes/${encodeURIComponent(id)}/undrain`,
    { method: "POST" },
  );

// ── Consumer Groups ─────────────────────────────────────────────────

export const listConsumerGroups = () =>
  request<ConsumerGroupSummary[]>("/consumer-groups");

export const getConsumerGroup = (id: string) =>
  request<{ group_id: string; lag: unknown[]; total_lag: number }>(
    `/consumer-groups/${encodeURIComponent(id)}`,
  );

export const resetConsumerGroup = (id: string, body: ResetOffsetsRequest) =>
  request<{ group_id: string; topic: string; partition: number; offset: number }>(
    `/consumer-groups/${encodeURIComponent(id)}/reset`,
    { method: "POST", body: JSON.stringify(body) },
  );

// ── Events ──────────────────────────────────────────────────────────

export function queryEvents(params: EventQueryParams): Promise<EventQueryResult> {
  const qs = new URLSearchParams();
  qs.set("topic", params.topic);
  if (params.partition !== undefined) qs.set("partition", String(params.partition));
  if (params.after_seq !== undefined) qs.set("after_seq", String(params.after_seq));
  if (params.before_seq !== undefined) qs.set("before_seq", String(params.before_seq));
  if (params.after_time) qs.set("after_time", params.after_time);
  if (params.before_time) qs.set("before_time", params.before_time);
  if (params.publisher_id) qs.set("publisher_id", params.publisher_id);
  if (params.key) qs.set("key", params.key);
  if (params.limit !== undefined) qs.set("limit", String(params.limit));
  return request<EventQueryResult>(`/events?${qs.toString()}`);
}
