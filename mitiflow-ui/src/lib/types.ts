// TypeScript interfaces matching the Rust API JSON types.

export interface TopicConfig {
  name: string;
  key_prefix: string;
  num_partitions: number;
  replication_factor: number;
  retention: RetentionPolicy;
  compaction: CompactionPolicy;
  required_labels: Record<string, string>;
  excluded_labels: Record<string, string>;
}

export interface RetentionPolicy {
  max_age?: string;
  max_bytes?: number;
  max_events?: number;
}

export interface CompactionPolicy {
  enabled: boolean;
  interval?: string;
}

export interface NodeInfo {
  metadata?: NodeMetadata;
  health?: NodeHealth;
  status?: NodeStatus;
  online: boolean;
  last_seen: string;
}

export interface NodeMetadata {
  node_id: string;
  capacity: number;
  labels: Record<string, string>;
  started_at: string;
}

export interface NodeHealth {
  node_id: string;
  partitions_owned: number;
  events_stored: number;
  disk_usage_bytes: number;
  store_latency_p99_us: number;
  error_count: number;
  timestamp: string;
}

export interface NodeStatus {
  node_id: string;
  partitions: PartitionStatus[];
  timestamp: string;
}

export interface PartitionStatus {
  partition: number;
  replica: number;
  state: "Starting" | "Recovering" | "Active" | "Draining" | "Stopped";
  event_count: number;
  watermark_seq: Record<string, number>;
}

export interface LagReport {
  group_id: string;
  partition: number;
  publishers: Record<string, number>;
  total: number;
  timestamp: string;
}

export interface ClusterStatus {
  total_nodes: number;
  online_nodes: number;
  total_partitions: number;
}

export interface EventSummary {
  seq: number;
  partition: number;
  publisher_id: string;
  timestamp: string;
  key?: string;
  key_expr: string;
  payload_size: number;
}

export interface AssignmentInfo {
  partition: number;
  replica: number;
  node_id: string;
  state: string;
  source: "Computed" | "Override";
}

export interface PartitionInfo {
  partition: number;
  replicas: ReplicaInfo[];
}

export interface ReplicaInfo {
  replica: number;
  node_id: string;
  state: string;
  source: string;
}

export interface PublisherInfo {
  publisher_id: string;
  partitions: Record<number, number>;
}

export interface TopicLagSummary {
  group_id: string;
  partitions: LagReport[];
  total_lag: number;
}

export interface ConsumerGroupSummary {
  group_id: string;
  total_lag: number;
}

export interface DrainResponse {
  node_id: string;
  overrides: OverrideEntry[];
}

export interface OverrideEntry {
  partition: number;
  replica: number;
  node_id: string;
  reason: string;
}

export interface OverrideTable {
  entries: OverrideEntry[];
  epoch: number;
  expires_at?: string;
}

// Cluster SSE event types
export type ClusterEvent =
  | { type: "node_online"; node_id: string; timestamp: string }
  | { type: "node_offline"; node_id: string; timestamp: string }
  | { type: "node_health"; node_id: string; [key: string]: unknown }
  | { type: "node_status"; node_id: string; [key: string]: unknown };

// Request types
export interface CreateTopicRequest {
  name: string;
  key_prefix: string;
  num_partitions: number;
  replication_factor: number;
  required_labels?: Record<string, string>;
  excluded_labels?: Record<string, string>;
}

export interface UpdateTopicRequest {
  replication_factor?: number;
  retention?: Partial<RetentionPolicy>;
  compaction?: Partial<CompactionPolicy>;
  required_labels?: Record<string, string>;
  excluded_labels?: Record<string, string>;
}

export interface DrainRequest {
  replication_factor?: number;
}

export interface AddOverridesRequest {
  entries: OverrideEntry[];
  ttl_seconds?: number;
}

// Event detail (from query-through endpoint)
export interface EventDetail {
  seq: number;
  partition: number;
  publisher_id: string;
  event_id: string;
  timestamp: string;
  key?: string;
  key_expr: string;
  payload_size: number;
  payload_base64: string;
  payload_text?: string;
}

export interface EventQueryResult {
  events: EventDetail[];
  total: number;
  has_more: boolean;
}

export interface EventQueryParams {
  topic: string;
  partition?: number;
  after_seq?: number;
  before_seq?: number;
  after_time?: string;
  before_time?: string;
  publisher_id?: string;
  key?: string;
  limit?: number;
}

// Consumer group offset reset
export type ResetStrategy = "earliest" | "latest" | { to_seq: number };

export interface ResetOffsetsRequest {
  topic: string;
  partition: number;
  strategy: ResetStrategy;
}
