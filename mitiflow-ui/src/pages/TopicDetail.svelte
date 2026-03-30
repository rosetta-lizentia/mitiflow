<script lang="ts">
  import { onMount } from "svelte";
  import { getTopic, getTopicPartitions, getTopicLag, getTopicPublishers, updateTopic, deleteTopic } from "$lib/api";
  import { push } from "svelte-spa-router";
  import type { TopicConfig, PartitionInfo, TopicLagSummary, PublisherInfo, UpdateTopicRequest } from "$lib/types";
  import StatusBadge from "../components/StatusBadge.svelte";
  import PartitionGrid from "../components/PartitionGrid.svelte";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";
  import { shortId, formatNumber } from "$lib/format";

  let { params }: { params: { name: string } } = $props();

  let topic = $state<TopicConfig | null>(null);
  let partitions = $state<PartitionInfo[]>([]);
  let lag = $state<TopicLagSummary[]>([]);
  let publishers = $state<PublisherInfo[]>([]);
  let error = $state("");
  let showDeleteConfirm = $state(false);
  let editingRF = $state(false);
  let newRF = $state(1);
  let showSettings = $state(false);

  // Editable settings
  let editRetentionMaxAge = $state("");
  let editRetentionMaxBytes = $state("");
  let editRetentionMaxEvents = $state("");
  let editCompactionEnabled = $state(false);
  let editCompactionInterval = $state("");
  let settingsMsg = $state("");

  async function load() {
    try {
      const name = params.name;
      [topic, partitions, lag, publishers] = await Promise.all([
        getTopic(name),
        getTopicPartitions(name),
        getTopicLag(name),
        getTopicPublishers(name),
      ]);
      if (topic) {
        newRF = topic.replication_factor;
        editRetentionMaxAge = topic.retention.max_age ?? "";
        editRetentionMaxBytes = topic.retention.max_bytes?.toString() ?? "";
        editRetentionMaxEvents = topic.retention.max_events?.toString() ?? "";
        editCompactionEnabled = topic.compaction.enabled;
        editCompactionInterval = topic.compaction.interval ?? "";
      }
      error = "";
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  onMount(load);

  async function handleUpdateRF() {
    if (!topic) return;
    try {
      const req: UpdateTopicRequest = { replication_factor: newRF };
      topic = await updateTopic(topic.name, req);
      editingRF = false;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  async function handleDelete() {
    if (!topic) return;
    await deleteTopic(topic.name);
    push("/topics");
  }

  async function handleSaveSettings() {
    if (!topic) return;
    settingsMsg = "";
    try {
      const req: UpdateTopicRequest = {
        retention: {
          max_age: editRetentionMaxAge || undefined,
          max_bytes: editRetentionMaxBytes ? parseInt(editRetentionMaxBytes) : undefined,
          max_events: editRetentionMaxEvents ? parseInt(editRetentionMaxEvents) : undefined,
        },
        compaction: {
          enabled: editCompactionEnabled,
          interval: editCompactionInterval || undefined,
        },
      };
      topic = await updateTopic(topic.name, req);
      settingsMsg = "Settings saved.";
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  // Flatten partitions for the grid
  const gridPartitions = $derived(
    partitions.flatMap((p) =>
      p.replicas.map((r) => ({
        partition: p.partition,
        state: r.state,
        node_id: r.node_id,
      })),
    ),
  );
</script>

<div class="space-y-6">
  {#if error}
    <p class="text-red-400">{error}</p>
  {/if}

  {#if topic}
    <div class="flex items-center justify-between">
      <div>
        <h1 class="text-2xl font-bold">{topic.name}</h1>
        <p class="text-sm text-gray-500">Prefix: {topic.key_prefix || "—"}</p>
      </div>
      <button
        class="rounded-lg border border-red-800 px-3 py-1.5 text-xs text-red-400 hover:bg-red-950"
        onclick={() => (showDeleteConfirm = true)}
      >
        Delete Topic
      </button>
    </div>

    <!-- Config -->
    <div class="grid grid-cols-3 gap-4">
      <div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
        <p class="text-xs text-gray-500">Partitions</p>
        <p class="text-xl font-semibold">{topic.num_partitions}</p>
      </div>
      <div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
        <p class="text-xs text-gray-500">Replication Factor</p>
        {#if editingRF}
          <div class="mt-1 flex gap-2">
            <input
              bind:value={newRF}
              type="number"
              min="1"
              max="10"
              class="w-16 rounded border border-gray-700 bg-gray-800 px-2 py-1 text-sm"
            />
            <button
              class="text-xs text-indigo-400"
              onclick={handleUpdateRF}
            >
              Save
            </button>
          </div>
        {:else}
          <p class="text-xl font-semibold">
            {topic.replication_factor}
            <button class="ml-2 text-xs text-gray-500 hover:text-gray-300" onclick={() => (editingRF = true)}>
              Edit
            </button>
          </p>
        {/if}
      </div>
      <div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
        <p class="text-xs text-gray-500">Publishers</p>
        <p class="text-xl font-semibold">{publishers.length}</p>
      </div>
    </div>

    <!-- Partition grid -->
    <section>
      <h2 class="mb-2 text-lg font-semibold text-gray-300">Partition Map</h2>
      <PartitionGrid partitions={gridPartitions} numPartitions={topic.num_partitions} />
    </section>

    <!-- Partition detail table -->
    {#if partitions.length > 0}
      <section>
        <h2 class="mb-2 text-lg font-semibold text-gray-300">Partition Assignments</h2>
        <div class="overflow-x-auto rounded-lg border border-gray-800">
          <table class="w-full text-left text-sm">
            <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
              <tr>
                <th class="px-4 py-2">Partition</th>
                <th class="px-4 py-2">Replica</th>
                <th class="px-4 py-2">Node</th>
                <th class="px-4 py-2">State</th>
                <th class="px-4 py-2">Source</th>
              </tr>
            </thead>
            <tbody>
              {#each partitions as p}
                {#each p.replicas as r}
                  <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                    <td class="px-4 py-2">{p.partition}</td>
                    <td class="px-4 py-2">{r.replica}</td>
                    <td class="px-4 py-2 font-mono text-xs">{shortId(r.node_id)}</td>
                    <td class="px-4 py-2"><StatusBadge status={r.state} /></td>
                    <td class="px-4 py-2 text-gray-500">{r.source}</td>
                  </tr>
                {/each}
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}

    <!-- Lag -->
    {#if lag.length > 0}
      <section>
        <h2 class="mb-2 text-lg font-semibold text-gray-300">Consumer Group Lag</h2>
        {#each lag as summary}
          <div class="mb-3 rounded-lg border border-gray-800 bg-gray-900 p-3">
            <div class="flex items-center justify-between">
              <span class="font-medium text-indigo-400">{summary.group_id}</span>
              <span class="text-sm text-gray-400">Total lag: {formatNumber(summary.total_lag)}</span>
            </div>
          </div>
        {/each}
      </section>
    {/if}

    <!-- Settings (retention + compaction) -->
    <section>
      <button
        class="mb-2 text-sm text-gray-400 hover:text-gray-200"
        onclick={() => showSettings = !showSettings}
      >{showSettings ? "▴ Hide Settings" : "▾ Topic Settings"}</button>
      {#if showSettings}
        <div class="rounded-xl border border-gray-800 bg-gray-900 p-4 space-y-4">
          <h3 class="text-sm font-semibold text-gray-300">Retention</h3>
          <div class="grid grid-cols-3 gap-4">
            <div>
              <label class="mb-1 block text-xs text-gray-500">Max Age</label>
              <input
                class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
                placeholder="e.g. 7d"
                bind:value={editRetentionMaxAge}
              />
            </div>
            <div>
              <label class="mb-1 block text-xs text-gray-500">Max Bytes</label>
              <input
                type="number"
                class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
                bind:value={editRetentionMaxBytes}
              />
            </div>
            <div>
              <label class="mb-1 block text-xs text-gray-500">Max Events</label>
              <input
                type="number"
                class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
                bind:value={editRetentionMaxEvents}
              />
            </div>
          </div>
          <h3 class="text-sm font-semibold text-gray-300">Compaction</h3>
          <div class="grid grid-cols-2 gap-4">
            <div class="flex items-center gap-2">
              <input
                type="checkbox"
                class="rounded border-gray-700 bg-gray-800"
                bind:checked={editCompactionEnabled}
              />
              <label class="text-sm text-gray-400">Enabled</label>
            </div>
            <div>
              <label class="mb-1 block text-xs text-gray-500">Interval</label>
              <input
                class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
                placeholder="e.g. 1h"
                bind:value={editCompactionInterval}
              />
            </div>
          </div>
          <div class="flex items-center gap-3">
            <button
              class="rounded bg-indigo-600 px-4 py-1.5 text-sm text-white hover:bg-indigo-500"
              onclick={handleSaveSettings}
            >Save Settings</button>
            {#if settingsMsg}
              <span class="text-sm text-green-400">{settingsMsg}</span>
            {/if}
          </div>
        </div>
      {/if}
    </section>
  {/if}
</div>

<ConfirmDialog
  bind:open={showDeleteConfirm}
  title="Delete Topic"
  message="Are you sure you want to delete '{topic?.name}'?"
  confirmLabel="Delete"
  onconfirm={handleDelete}
/>
