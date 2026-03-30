<script lang="ts">
  import { onMount } from "svelte";
  import { push } from "svelte-spa-router";
  import { getClusterNodes, drainNode, undrainNode } from "$lib/api";
  import type { NodeInfo } from "$lib/types";
  import StatusBadge from "../components/StatusBadge.svelte";
  import TimeAgo from "../components/TimeAgo.svelte";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";
  import { shortId, formatBytes } from "$lib/format";

  let nodes = $state<Record<string, NodeInfo>>({});
  let error = $state("");
  let drainTarget = $state("");
  let showDrainConfirm = $state(false);

  async function load() {
    try {
      nodes = await getClusterNodes();
      error = "";
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  onMount(load);

  async function handleDrain() {
    try {
      await drainNode(drainTarget);
      await load();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  async function handleUndrain(nodeId: string) {
    try {
      await undrainNode(nodeId);
      await load();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  const nodeEntries = $derived(Object.entries(nodes));
</script>

<div class="space-y-4">
  <h1 class="text-2xl font-bold">Nodes</h1>

  {#if error}
    <p class="text-sm text-red-400">{error}</p>
  {/if}

  {#if nodeEntries.length === 0}
    <p class="text-gray-500">No nodes discovered yet.</p>
  {:else}
    <div class="overflow-x-auto rounded-lg border border-gray-800">
      <table class="w-full text-left text-sm">
        <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
          <tr>
            <th class="px-4 py-2">Node ID</th>
            <th class="px-4 py-2">Status</th>
            <th class="px-4 py-2">Partitions</th>
            <th class="px-4 py-2">Events</th>
            <th class="px-4 py-2">Disk</th>
            <th class="px-4 py-2">Last Seen</th>
            <th class="px-4 py-2"></th>
          </tr>
        </thead>
        <tbody>
          {#each nodeEntries as [id, node]}
            <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
              <td class="px-4 py-2">
                <button
                  class="font-mono text-xs text-indigo-400 hover:underline"
                  onclick={() => push(`/nodes/${id}`)}
                >
                  {shortId(id)}
                </button>
              </td>
              <td class="px-4 py-2">
                <StatusBadge status={node.online ? "online" : "offline"} />
              </td>
              <td class="px-4 py-2">{node.health?.partitions_owned ?? "—"}</td>
              <td class="px-4 py-2">{node.health?.events_stored?.toLocaleString() ?? "—"}</td>
              <td class="px-4 py-2">{node.health ? formatBytes(node.health.disk_usage_bytes) : "—"}</td>
              <td class="px-4 py-2"><TimeAgo timestamp={node.last_seen} /></td>
              <td class="px-4 py-2 text-right">
                <button
                  class="text-xs text-orange-400 hover:text-orange-300"
                  onclick={() => { drainTarget = id; showDrainConfirm = true; }}
                >
                  Drain
                </button>
                <button
                  class="ml-2 text-xs text-gray-400 hover:text-gray-300"
                  onclick={() => handleUndrain(id)}
                >
                  Undrain
                </button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<ConfirmDialog
  bind:open={showDrainConfirm}
  title="Drain Node"
  message="Drain node '{shortId(drainTarget)}'? Partitions will be reassigned."
  confirmLabel="Drain"
  onconfirm={handleDrain}
/>
