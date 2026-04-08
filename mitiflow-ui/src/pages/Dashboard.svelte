<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import StatCard from "../components/StatCard.svelte";
  import StatusBadge from "../components/StatusBadge.svelte";
  import LagSparkline from "../components/LagSparkline.svelte";
  import { getClusterStatus, getClusterNodes, listTopics, listConsumerGroups, getAllClients } from "$lib/api";
  import { startClusterSSE, stopClusterSSE, getClusterNodes as getLiveNodes, setClusterNodes } from "../stores/cluster.svelte";
  import { startEventSSE, stopEventSSE, getEvents } from "../stores/events.svelte";
  import { startLagSSE, stopLagSSE, getLagReports } from "../stores/lag.svelte";
  import type { ClusterStatus, TopicConfig, ConsumerGroupSummary, TopicClients } from "$lib/types";
  import { shortId, formatBytes, timeAgo } from "$lib/format";

  let clusterStatus = $state<ClusterStatus | null>(null);
  let topics = $state<TopicConfig[]>([]);
  let groups = $state<ConsumerGroupSummary[]>([]);
  let allClients = $state<TopicClients[]>([]);
  let error = $state("");

  // Per-group lag history for sparklines (keyed by group_id)
  let lagHistory = $state<Record<string, number[]>>({});

  onMount(async () => {
    try {
      const [status, t, g, nodes, clients] = await Promise.all([
        getClusterStatus(),
        listTopics(),
        listConsumerGroups(),
        getClusterNodes(),
        getAllClients(),
      ]);
      clusterStatus = status;
      topics = t;
      groups = g;
      allClients = clients;
      setClusterNodes(nodes);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
    startClusterSSE();
    startEventSSE();
    startLagSSE();
  });

  onDestroy(() => {
    stopClusterSSE();
    stopEventSSE();
    stopLagSSE();
  });

  const liveNodes = $derived(getLiveNodes());
  const recentEvents = $derived(getEvents().toReversed().slice(0, 10));
  const lagReports = $derived(getLagReports());
  const totalPublishers = $derived(allClients.reduce((n, c) => n + c.publishers.length, 0));
  const totalConsumers = $derived(allClients.reduce((n, c) => n + c.consumers.length, 0));

  // Build lag history per group from live reports
  $effect(() => {
    for (const r of lagReports) {
      const key = r.group_id;
      const prev = lagHistory[key] ?? [];
      lagHistory[key] = [...prev.slice(-29), r.total];
    }
  });
</script>

<div class="space-y-6">
  <h1 class="text-2xl font-bold">Dashboard</h1>

  {#if error}
    <p class="text-red-400">Failed to load: {error}</p>
  {/if}

  <!-- Summary cards -->
  <div class="grid grid-cols-2 gap-4 md:grid-cols-4 lg:grid-cols-6">
    <StatCard
      label="Topics"
      value={topics.length}
    />
    <StatCard
      label="Nodes"
      value={clusterStatus?.total_nodes ?? 0}
      subtitle="{clusterStatus?.online_nodes ?? 0} online"
    />
    <StatCard
      label="Partitions"
      value={clusterStatus?.total_partitions ?? 0}
    />
    <StatCard
      label="Consumer Groups"
      value={groups.length}
    />
    <StatCard
      label="Publishers"
      value={totalPublishers}
    />
    <StatCard
      label="Consumers"
      value={totalConsumers}
    />
  </div>

  <!-- Quick topic list -->
  <section>
    <h2 class="mb-3 text-lg font-semibold text-gray-300">Topics</h2>
    {#if topics.length === 0}
      <p class="text-sm text-gray-500">No topics configured.</p>
    {:else}
      <div class="overflow-x-auto rounded-lg border border-gray-800">
        <table class="w-full text-left text-sm">
          <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
            <tr>
              <th class="px-4 py-2">Name</th>
              <th class="px-4 py-2">Partitions</th>
              <th class="px-4 py-2">RF</th>
              <th class="px-4 py-2">Key Prefix</th>
            </tr>
          </thead>
          <tbody>
            {#each topics as topic}
              <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td class="px-4 py-2 font-medium text-indigo-400">
                  <a href="#/topics/{topic.name}">{topic.name}</a>
                </td>
                <td class="px-4 py-2">{topic.num_partitions}</td>
                <td class="px-4 py-2">{topic.replication_factor}</td>
                <td class="px-4 py-2 text-gray-500">{topic.key_prefix || "—"}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <!-- Consumer groups quick view -->
  {#if groups.length > 0}
    <section>
      <h2 class="mb-3 text-lg font-semibold text-gray-300">Consumer Groups</h2>
      <div class="overflow-x-auto rounded-lg border border-gray-800">
        <table class="w-full text-left text-sm">
          <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
            <tr>
              <th class="px-4 py-2">Group ID</th>
              <th class="px-4 py-2">Total Lag</th>
              <th class="px-4 py-2">Trend</th>
            </tr>
          </thead>
          <tbody>
            {#each groups as group}
              <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td class="px-4 py-2 font-medium">
                  <a href="#/consumer-groups/{group.group_id}" class="text-indigo-400">
                    {group.group_id}
                  </a>
                </td>
                <td class="px-4 py-2">
                  <StatusBadge status={group.total_lag === 0 ? "online" : "recovering"} />
                  {group.total_lag.toLocaleString()}
                </td>
                <td class="px-4 py-2">
                  <LagSparkline values={lagHistory[group.group_id] ?? []} />
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </section>
  {/if}

  <!-- Live nodes (SSE-driven) -->
  {#if Object.keys(liveNodes).length > 0}
    <section>
      <h2 class="mb-3 text-lg font-semibold text-gray-300">Nodes (Live)</h2>
      <div class="overflow-x-auto rounded-lg border border-gray-800">
        <table class="w-full text-left text-sm">
          <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
            <tr>
              <th class="px-4 py-2">Node</th>
              <th class="px-4 py-2">Status</th>
              <th class="px-4 py-2">Last Seen</th>
            </tr>
          </thead>
          <tbody>
            {#each Object.entries(liveNodes) as [id, node]}
              <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td class="px-4 py-2 font-mono text-sm">
                  <a href="#/nodes/{id}" class="text-indigo-400">{shortId(id)}</a>
                </td>
                <td class="px-4 py-2">
                  <StatusBadge status={node.online ? "online" : "offline"} />
                </td>
                <td class="px-4 py-2 text-gray-500">{timeAgo(node.last_seen)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </section>
  {/if}

  <!-- Recent events tail -->
  {#if recentEvents.length > 0}
    <section>
      <h2 class="mb-3 text-lg font-semibold text-gray-300">Recent Events (Live)</h2>
      <div class="overflow-x-auto rounded-lg border border-gray-800">
        <table class="w-full text-left text-xs">
          <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
            <tr>
              <th class="px-3 py-2">Seq</th>
              <th class="px-3 py-2">Partition</th>
              <th class="px-3 py-2">Publisher</th>
              <th class="px-3 py-2">Key</th>
              <th class="px-3 py-2">Size</th>
              <th class="px-3 py-2">Time</th>
            </tr>
          </thead>
          <tbody>
            {#each recentEvents as evt}
              <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td class="px-3 py-1.5 font-mono">{evt.seq}</td>
                <td class="px-3 py-1.5">{evt.partition}</td>
                <td class="px-3 py-1.5 font-mono text-indigo-400">{shortId(evt.publisher_id)}</td>
                <td class="px-3 py-1.5 text-gray-400">{evt.key ?? "—"}</td>
                <td class="px-3 py-1.5">{formatBytes(evt.payload_size)}</td>
                <td class="px-3 py-1.5 text-gray-500">{evt.timestamp}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </section>
  {/if}
</div>
