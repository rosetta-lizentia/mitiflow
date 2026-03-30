<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import { getConsumerGroup, resetConsumerGroup, listTopics } from "$lib/api";
  import { startLagSSE, stopLagSSE, getLagReports } from "../stores/lag.svelte";
  import type { LagReport, TopicConfig, ResetStrategy } from "$lib/types";
  import { formatNumber, shortId } from "$lib/format";
  import LagSparkline from "../components/LagSparkline.svelte";

  let { params }: { params: { id: string } } = $props();

  let detail = $state<{ group_id: string; lag: LagReport[]; total_lag: number } | null>(null);
  let error = $state("");

  // Lag history for sparkline (keyed by partition)
  let lagHistory = $state<Record<number, number[]>>({});

  // Reset dialog state
  let showReset = $state(false);
  let resetTopic = $state("");
  let resetPartition = $state("0");
  let resetStrategyType = $state<"earliest" | "latest" | "to_seq">("latest");
  let resetSeq = $state("0");
  let resetMsg = $state("");
  let resetError = $state("");
  let topics = $state<TopicConfig[]>([]);

  onMount(async () => {
    try {
      detail = await getConsumerGroup(params.id) as typeof detail;
      topics = await listTopics();
      if (topics.length > 0) resetTopic = topics[0].name;
      startLagSSE(params.id);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  });

  onDestroy(() => {
    stopLagSSE();
  });

  const liveReports = $derived(getLagReports());

  // Accumulate lag history per partition from live reports
  $effect(() => {
    for (const r of liveReports) {
      const prev = lagHistory[r.partition] ?? [];
      lagHistory[r.partition] = [...prev.slice(-29), r.total];
    }
  });

  async function handleReset() {
    resetMsg = "";
    resetError = "";
    let strategy: ResetStrategy;
    if (resetStrategyType === "earliest") strategy = "earliest";
    else if (resetStrategyType === "latest") strategy = "latest";
    else strategy = { to_seq: parseInt(resetSeq) };
    try {
      const result = await resetConsumerGroup(params.id, {
        topic: resetTopic,
        partition: parseInt(resetPartition),
        strategy,
      });
      resetMsg = `Offset reset to ${result.offset} for partition ${result.partition}`;
    } catch (e) {
      resetError = e instanceof Error ? e.message : String(e);
    }
  }
</script>

<div class="space-y-6">
  <div class="flex items-center justify-between">
    <h1 class="text-2xl font-bold">Consumer Group: {params.id}</h1>
    <button
      class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-500"
      onclick={() => showReset = !showReset}
    >{showReset ? "Cancel" : "Reset Offsets"}</button>
  </div>

  {#if error}
    <p class="text-red-400">{error}</p>
  {/if}

  <!-- Reset offsets dialog -->
  {#if showReset}
    <div class="rounded-xl border border-amber-800/50 bg-gray-900 p-4 space-y-3">
      <h2 class="text-lg font-semibold text-amber-400">Reset Consumer Group Offsets</h2>
      <div class="grid grid-cols-2 gap-4 md:grid-cols-4">
        <div>
          <label class="mb-1 block text-xs text-gray-500">Topic</label>
          <select
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            bind:value={resetTopic}
          >
            {#each topics as t}
              <option value={t.name}>{t.name}</option>
            {/each}
          </select>
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-500">Partition</label>
          <input
            type="number"
            min="0"
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            bind:value={resetPartition}
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-500">Strategy</label>
          <select
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            bind:value={resetStrategyType}
          >
            <option value="earliest">Earliest</option>
            <option value="latest">Latest</option>
            <option value="to_seq">To Sequence</option>
          </select>
        </div>
        {#if resetStrategyType === "to_seq"}
          <div>
            <label class="mb-1 block text-xs text-gray-500">Sequence</label>
            <input
              type="number"
              min="0"
              class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
              bind:value={resetSeq}
            />
          </div>
        {/if}
      </div>
      <button
        class="rounded bg-amber-600 px-4 py-1.5 text-sm text-white hover:bg-amber-500"
        onclick={handleReset}
      >Reset Offsets</button>
      {#if resetMsg}
        <p class="text-sm text-green-400">{resetMsg}</p>
      {/if}
      {#if resetError}
        <p class="text-sm text-red-400">{resetError}</p>
      {/if}
    </div>
  {/if}

  {#if detail}
    <div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
      <p class="text-sm text-gray-500">Total Lag</p>
      <p class="text-3xl font-bold">{formatNumber(detail.total_lag)}</p>
    </div>

    {#if detail.lag.length > 0}
      <section>
        <h2 class="mb-2 text-lg font-semibold text-gray-300">Per-Partition Lag</h2>
        <div class="overflow-x-auto rounded-lg border border-gray-800">
          <table class="w-full text-left text-sm">
            <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
              <tr>
                <th class="px-4 py-2">Partition</th>
                <th class="px-4 py-2">Total Lag</th>
                <th class="px-4 py-2">Trend</th>
                <th class="px-4 py-2">Publishers</th>
              </tr>
            </thead>
            <tbody>
              {#each detail.lag as report}
                <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                  <td class="px-4 py-2">{report.partition}</td>
                  <td class="px-4 py-2">{formatNumber(report.total)}</td>
                  <td class="px-4 py-2">
                    <LagSparkline values={lagHistory[report.partition] ?? []} />
                  </td>
                  <td class="px-4 py-2">
                    <div class="flex flex-wrap gap-1">
                      {#each Object.entries(report.publishers) as [pubId, lag]}
                        <span class="rounded bg-gray-800 px-2 py-0.5 text-xs font-mono">
                          {shortId(pubId)}: {lag}
                        </span>
                      {/each}
                    </div>
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}

    <!-- Live lag updates -->
    {#if liveReports.length > 0}
      <section>
        <h2 class="mb-2 text-lg font-semibold text-gray-300">Live Lag (SSE)</h2>
        <div class="max-h-64 overflow-y-auto rounded-lg border border-gray-800 bg-gray-900 p-3 text-xs font-mono">
          {#each liveReports.toReversed().slice(0, 20) as r}
            <p class="text-gray-400">
              p{r.partition} lag={r.total} ({r.timestamp})
            </p>
          {/each}
        </div>
      </section>
    {/if}
  {/if}
</div>
