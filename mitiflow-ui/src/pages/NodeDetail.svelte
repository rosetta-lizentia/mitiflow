<script lang="ts">
  import { onMount } from "svelte";
  import { getClusterNodes, getNodePartitions } from "$lib/api";
  import type { NodeInfo, NodeTopicPartitions } from "$lib/types";
  import StatusBadge from "../components/StatusBadge.svelte";
  import TimeAgo from "../components/TimeAgo.svelte";
  import { formatBytes, formatMicros, formatNumber } from "$lib/format";

  let { params }: { params: { id: string } } = $props();

  let node = $state<NodeInfo | null>(null);
  let nodeId = $state("");
  let topicPartitions = $state<NodeTopicPartitions[]>([]);
  let error = $state("");

  onMount(async () => {
    try {
      nodeId = params.id;
      const nodes = await getClusterNodes();
      // Find the node — id might be truncated, try full match first
      for (const [id, info] of Object.entries(nodes)) {
        if (id === nodeId || id.startsWith(nodeId)) {
          node = info;
          nodeId = id;
          break;
        }
      }
      if (!node) {
        error = "Node not found";
        return;
      }
      topicPartitions = await getNodePartitions(nodeId);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  });
</script>

<div class="space-y-6">
  {#if error}
    <p class="text-red-400">{error}</p>
  {/if}

  {#if node}
    <div>
      <h1 class="text-2xl font-bold">Node {nodeId.slice(0, 12)}…</h1>
      <p class="text-sm text-gray-500 font-mono">{nodeId}</p>
    </div>

    <div class="flex items-center gap-4">
      <StatusBadge status={node.online ? "online" : "offline"} />
      <span class="text-sm text-gray-400">Last seen: <TimeAgo timestamp={node.last_seen} /></span>
    </div>

    {#if node.metadata}
      <section>
        <h2 class="mb-2 text-lg font-semibold text-gray-300">Metadata</h2>
        <div class="rounded-xl border border-gray-800 bg-gray-900 p-4 text-sm">
          <p><span class="text-gray-500">Capacity:</span> {node.metadata.capacity}</p>
          <p><span class="text-gray-500">Started:</span> {node.metadata.started_at}</p>
          {#if Object.keys(node.metadata.labels).length > 0}
            <p class="mt-2"><span class="text-gray-500">Labels:</span></p>
            <div class="mt-1 flex flex-wrap gap-2">
              {#each Object.entries(node.metadata.labels) as [k, v]}
                <span class="rounded bg-gray-800 px-2 py-0.5 text-xs">{k}={v}</span>
              {/each}
            </div>
          {/if}
        </div>
      </section>
    {/if}

    {#if node.health}
      <section>
        <h2 class="mb-2 text-lg font-semibold text-gray-300">Health</h2>
        <div class="grid grid-cols-2 gap-4 md:grid-cols-4">
          <div class="rounded-xl border border-gray-800 bg-gray-900 p-3">
            <p class="text-xs text-gray-500">Partitions</p>
            <p class="text-lg font-semibold">{node.health.partitions_owned}</p>
          </div>
          <div class="rounded-xl border border-gray-800 bg-gray-900 p-3">
            <p class="text-xs text-gray-500">Events Stored</p>
            <p class="text-lg font-semibold">{formatNumber(node.health.events_stored)}</p>
          </div>
          <div class="rounded-xl border border-gray-800 bg-gray-900 p-3">
            <p class="text-xs text-gray-500">Disk Usage</p>
            <p class="text-lg font-semibold">{formatBytes(node.health.disk_usage_bytes)}</p>
          </div>
          <div class="rounded-xl border border-gray-800 bg-gray-900 p-3">
            <p class="text-xs text-gray-500">Store Latency P99</p>
            <p class="text-lg font-semibold">{formatMicros(node.health.store_latency_p99_us)}</p>
          </div>
        </div>
      </section>
    {/if}

    {#if topicPartitions.length > 0}
      <section>
        <h2 class="mb-2 text-lg font-semibold text-gray-300">Partition Assignments</h2>
        {#each topicPartitions as tp}
          <div class="mb-4">
            <h3 class="mb-1 text-sm font-medium text-gray-400">{tp.topic}</h3>
            <div class="overflow-x-auto rounded-lg border border-gray-800">
              <table class="w-full text-left text-sm">
                <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
                  <tr>
                    <th class="px-4 py-2">Partition</th>
                    <th class="px-4 py-2">Replica</th>
                    <th class="px-4 py-2">State</th>
                    <th class="px-4 py-2">Source</th>
                  </tr>
                </thead>
                <tbody>
                  {#each tp.partitions as ps}
                    <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                      <td class="px-4 py-2">{ps.partition}</td>
                      <td class="px-4 py-2">{ps.replica}</td>
                      <td class="px-4 py-2"><StatusBadge status={ps.state} /></td>
                      <td class="px-4 py-2 text-gray-500">{ps.source}</td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          </div>
        {/each}
      </section>
    {/if}
  {/if}
</div>
