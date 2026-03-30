<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import { startEventSSE, stopEventSSE, getEvents, clearEvents } from "../stores/events.svelte";
  import { shortId, formatBytes } from "$lib/format";
  import { queryEvents, listTopics } from "$lib/api";
  import type { EventDetail as EventDetailType, TopicConfig, EventQueryParams } from "$lib/types";
  import EventDetail from "../components/EventDetail.svelte";

  type Mode = "live" | "query";
  let mode = $state<Mode>("live");

  // Filters
  let filterTopic = $state("");
  let filterPartition = $state("");
  let filterKey = $state("");
  let filterPublisher = $state("");
  let filterAfterSeq = $state("");
  let filterBeforeSeq = $state("");
  let filterLimit = $state("50");

  let topics = $state<TopicConfig[]>([]);

  // Query result state
  let queryResult = $state<EventDetailType[]>([]);
  let queryHasMore = $state(false);
  let queryLoading = $state(false);
  let queryError = $state("");

  onMount(async () => {
    try {
      topics = await listTopics();
      if (topics.length > 0 && !filterTopic) filterTopic = topics[0].name;
    } catch { /* ignore */ }
    startEventSSE(filterPartition ? parseInt(filterPartition) : undefined);
  });

  onDestroy(() => {
    stopEventSSE();
  });

  function applyLiveFilter() {
    clearEvents();
    stopEventSSE();
    startEventSSE(filterPartition ? parseInt(filterPartition) : undefined);
  }

  async function runQuery() {
    if (!filterTopic) { queryError = "Topic is required"; return; }
    queryLoading = true;
    queryError = "";
    try {
      const params: EventQueryParams = { topic: filterTopic };
      if (filterPartition) params.partition = parseInt(filterPartition);
      if (filterAfterSeq) params.after_seq = parseInt(filterAfterSeq);
      if (filterBeforeSeq) params.before_seq = parseInt(filterBeforeSeq);
      if (filterPublisher) params.publisher_id = filterPublisher;
      if (filterKey) params.key = filterKey;
      if (filterLimit) params.limit = parseInt(filterLimit);
      const result = await queryEvents(params);
      queryResult = result.events;
      queryHasMore = result.has_more;
    } catch (e) {
      queryError = e instanceof Error ? e.message : String(e);
    } finally {
      queryLoading = false;
    }
  }

  const liveEvents = $derived(getEvents());
</script>

<div class="space-y-4">
  <div class="flex items-center justify-between">
    <h1 class="text-2xl font-bold">Event Inspector</h1>
    <div class="flex rounded-lg border border-gray-700 overflow-hidden">
      <button
        class="px-3 py-1.5 text-xs transition-colors {mode === 'live' ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}"
        onclick={() => mode = "live"}
      >Live Tail</button>
      <button
        class="px-3 py-1.5 text-xs transition-colors {mode === 'query' ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}"
        onclick={() => mode = "query"}
      >Query Store</button>
    </div>
  </div>

  <!-- Filters -->
  <div class="rounded-lg border border-gray-800 bg-gray-900 p-4">
    <div class="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-6">
      {#if mode === "query"}
        <div>
          <label class="mb-1 block text-xs text-gray-500">Topic</label>
          <select
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            bind:value={filterTopic}
          >
            {#each topics as t}
              <option value={t.name}>{t.name}</option>
            {/each}
          </select>
        </div>
      {/if}
      <div>
        <label class="mb-1 block text-xs text-gray-500">Partition</label>
        <input
          type="number"
          min="0"
          class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
          placeholder="All"
          bind:value={filterPartition}
        />
      </div>
      {#if mode === "query"}
        <div>
          <label class="mb-1 block text-xs text-gray-500">Key</label>
          <input
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            placeholder="e.g. ORD-123"
            bind:value={filterKey}
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-500">Publisher</label>
          <input
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            placeholder="UUID"
            bind:value={filterPublisher}
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-500">After Seq</label>
          <input
            type="number"
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            bind:value={filterAfterSeq}
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-500">Before Seq</label>
          <input
            type="number"
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            bind:value={filterBeforeSeq}
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-500">Limit</label>
          <input
            type="number"
            min="1"
            max="1000"
            class="w-full rounded border border-gray-700 bg-gray-800 px-2 py-1.5 text-sm text-gray-100"
            bind:value={filterLimit}
          />
        </div>
      {/if}
    </div>
    <div class="mt-3 flex gap-2">
      {#if mode === "live"}
        <button
          class="rounded bg-indigo-600 px-4 py-1.5 text-xs text-white hover:bg-indigo-500"
          onclick={applyLiveFilter}
        >Apply Filter</button>
        <button
          class="rounded border border-gray-700 px-4 py-1.5 text-xs text-gray-400 hover:text-gray-200"
          onclick={clearEvents}
        >Clear</button>
      {:else}
        <button
          class="rounded bg-indigo-600 px-4 py-1.5 text-xs text-white hover:bg-indigo-500 disabled:opacity-50"
          onclick={runQuery}
          disabled={queryLoading}
        >{queryLoading ? "Querying…" : "Search"}</button>
      {/if}
    </div>
  </div>

  {#if queryError}
    <p class="text-sm text-red-400">{queryError}</p>
  {/if}

  <!-- Results -->
  {#if mode === "live"}
    <p class="text-xs text-gray-500">{liveEvents.length} events captured (live)</p>
    <div class="overflow-x-auto rounded-lg border border-gray-800">
      <table class="w-full text-left text-xs">
        <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
          <tr>
            <th class="px-3 py-2">Seq</th>
            <th class="px-3 py-2">Partition</th>
            <th class="px-3 py-2">Publisher</th>
            <th class="px-3 py-2">Key</th>
            <th class="px-3 py-2">Size</th>
            <th class="px-3 py-2">Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {#each liveEvents.toReversed().slice(0, 200) as evt}
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
  {:else}
    <p class="text-xs text-gray-500">
      {queryResult.length} events{queryHasMore ? " (more available)" : ""}
    </p>
    <div class="space-y-1">
      {#each queryResult as event}
        <EventDetail {event} />
      {:else}
        <p class="text-sm text-gray-500 py-4 text-center">No events found. Run a query above.</p>
      {/each}
    </div>
  {/if}
</div>
