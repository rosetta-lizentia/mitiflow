<script lang="ts">
  import { onMount } from "svelte";
  import {
    getOverrides,
    addOverrides,
    clearOverrides,
  } from "$lib/api";
  import type { OverrideTable, AddOverridesRequest } from "$lib/types";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";

  let table = $state<OverrideTable>({ entries: [], epoch: 0 });
  let error = $state("");
  let showClearConfirm = $state(false);
  let showAddForm = $state(false);

  // Add form
  let newPartition = $state(0);
  let newReplica = $state(0);
  let newNodeId = $state("");
  let newReason = $state("");
  let newTTL = $state<number | undefined>(undefined);

  async function load() {
    try {
      table = await getOverrides();
      error = "";
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  onMount(load);

  async function handleAdd() {
    try {
      const req: AddOverridesRequest = {
        entries: [
          {
            partition: newPartition,
            replica: newReplica,
            node_id: newNodeId,
            reason: newReason || "manual",
          },
        ],
        ttl_seconds: newTTL,
      };
      await addOverrides(req);
      showAddForm = false;
      newNodeId = "";
      newReason = "";
      await load();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  async function handleClear() {
    try {
      await clearOverrides();
      await load();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }
</script>

<div class="space-y-4">
  <div class="flex items-center justify-between">
    <h1 class="text-2xl font-bold">Overrides</h1>
    <div class="flex gap-2">
      <button
        class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500"
        onclick={() => (showAddForm = !showAddForm)}
      >
        {showAddForm ? "Cancel" : "+ Add Override"}
      </button>
      {#if table.entries.length > 0}
        <button
          class="rounded-lg border border-red-800 px-4 py-2 text-sm text-red-400 hover:bg-red-950"
          onclick={() => (showClearConfirm = true)}
        >
          Clear All
        </button>
      {/if}
    </div>
  </div>

  {#if error}
    <p class="text-sm text-red-400">{error}</p>
  {/if}

  <p class="text-xs text-gray-500">Epoch: {table.epoch}{table.expires_at ? ` • Expires: ${table.expires_at}` : ""}</p>

  {#if showAddForm}
    <form
      class="space-y-3 rounded-xl border border-gray-800 bg-gray-900 p-4"
      onsubmit={(e) => { e.preventDefault(); handleAdd(); }}
    >
      <div class="grid grid-cols-2 gap-4 md:grid-cols-4">
        <div>
          <label class="mb-1 block text-xs text-gray-400">Partition</label>
          <input bind:value={newPartition} type="number" min="0" class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100" />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-400">Replica</label>
          <input bind:value={newReplica} type="number" min="0" class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100" />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-400">Node ID</label>
          <input bind:value={newNodeId} required class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100" placeholder="node-abc..." />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-400">TTL (seconds)</label>
          <input bind:value={newTTL} type="number" min="0" class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100" placeholder="none" />
        </div>
      </div>
      <div>
        <label class="mb-1 block text-xs text-gray-400">Reason</label>
        <input bind:value={newReason} class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100" placeholder="manual" />
      </div>
      <button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500">
        Add
      </button>
    </form>
  {/if}

  {#if table.entries.length === 0}
    <p class="text-gray-500">No active overrides.</p>
  {:else}
    <div class="overflow-x-auto rounded-lg border border-gray-800">
      <table class="w-full text-left text-sm">
        <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
          <tr>
            <th class="px-4 py-2">Partition</th>
            <th class="px-4 py-2">Replica</th>
            <th class="px-4 py-2">Node ID</th>
            <th class="px-4 py-2">Reason</th>
          </tr>
        </thead>
        <tbody>
          {#each table.entries as entry}
            <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
              <td class="px-4 py-2">{entry.partition}</td>
              <td class="px-4 py-2">{entry.replica}</td>
              <td class="px-4 py-2 font-mono text-xs">{entry.node_id}</td>
              <td class="px-4 py-2 text-gray-400">{entry.reason}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<ConfirmDialog
  bind:open={showClearConfirm}
  title="Clear Overrides"
  message="Remove all {table.entries.length} override entries? Partitions will revert to HRW-computed assignments."
  confirmLabel="Clear All"
  onconfirm={handleClear}
/>
