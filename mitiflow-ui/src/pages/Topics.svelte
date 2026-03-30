<script lang="ts">
  import { onMount } from "svelte";
  import { push } from "svelte-spa-router";
  import { listTopics, createTopic, deleteTopic } from "$lib/api";
  import type { TopicConfig, CreateTopicRequest } from "$lib/types";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";

  let topics = $state<TopicConfig[]>([]);
  let error = $state("");
  let showCreate = $state(false);

  // Create form state
  let newName = $state("");
  let newPrefix = $state("");
  let newPartitions = $state(16);
  let newRF = $state(1);

  // Delete confirmation
  let deleteTarget = $state("");
  let showDeleteConfirm = $state(false);

  async function load() {
    try {
      topics = await listTopics();
      error = "";
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  onMount(load);

  async function handleCreate() {
    try {
      const req: CreateTopicRequest = {
        name: newName,
        key_prefix: newPrefix || newName,
        num_partitions: newPartitions,
        replication_factor: newRF,
      };
      await createTopic(req);
      showCreate = false;
      newName = "";
      newPrefix = "";
      newPartitions = 16;
      newRF = 1;
      await load();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  async function handleDelete() {
    try {
      await deleteTopic(deleteTarget);
      deleteTarget = "";
      await load();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }
</script>

<div class="space-y-4">
  <div class="flex items-center justify-between">
    <h1 class="text-2xl font-bold">Topics</h1>
    <button
      class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500"
      onclick={() => (showCreate = !showCreate)}
    >
      {showCreate ? "Cancel" : "+ Create Topic"}
    </button>
  </div>

  {#if error}
    <p class="text-sm text-red-400">{error}</p>
  {/if}

  <!-- Create form -->
  {#if showCreate}
    <form
      class="space-y-3 rounded-xl border border-gray-800 bg-gray-900 p-4"
      onsubmit={(e) => { e.preventDefault(); handleCreate(); }}
    >
      <div class="grid grid-cols-2 gap-4">
        <div>
          <label class="mb-1 block text-xs text-gray-400">Name</label>
          <input
            bind:value={newName}
            required
            class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none"
            placeholder="my-topic"
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-400">Key Prefix</label>
          <input
            bind:value={newPrefix}
            class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none"
            placeholder="defaults to name"
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-400">Partitions</label>
          <input
            bind:value={newPartitions}
            type="number"
            min="1"
            max="1024"
            class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none"
          />
        </div>
        <div>
          <label class="mb-1 block text-xs text-gray-400">Replication Factor</label>
          <input
            bind:value={newRF}
            type="number"
            min="1"
            max="10"
            class="w-full rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none"
          />
        </div>
      </div>
      <button
        type="submit"
        class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500"
      >
        Create
      </button>
    </form>
  {/if}

  <!-- Topic list -->
  {#if topics.length === 0}
    <p class="text-gray-500">No topics.</p>
  {:else}
    <div class="overflow-x-auto rounded-lg border border-gray-800">
      <table class="w-full text-left text-sm">
        <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
          <tr>
            <th class="px-4 py-2">Name</th>
            <th class="px-4 py-2">Partitions</th>
            <th class="px-4 py-2">RF</th>
            <th class="px-4 py-2">Key Prefix</th>
            <th class="px-4 py-2"></th>
          </tr>
        </thead>
        <tbody>
          {#each topics as topic}
            <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
              <td class="px-4 py-2">
                <button
                  class="font-medium text-indigo-400 hover:underline"
                  onclick={() => push(`/topics/${topic.name}`)}
                >
                  {topic.name}
                </button>
              </td>
              <td class="px-4 py-2">{topic.num_partitions}</td>
              <td class="px-4 py-2">{topic.replication_factor}</td>
              <td class="px-4 py-2 text-gray-500">{topic.key_prefix || "—"}</td>
              <td class="px-4 py-2 text-right">
                <button
                  class="text-xs text-red-400 hover:text-red-300"
                  onclick={() => { deleteTarget = topic.name; showDeleteConfirm = true; }}
                >
                  Delete
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
  bind:open={showDeleteConfirm}
  title="Delete Topic"
  message="Are you sure you want to delete topic '{deleteTarget}'? This cannot be undone."
  confirmLabel="Delete"
  onconfirm={handleDelete}
/>
