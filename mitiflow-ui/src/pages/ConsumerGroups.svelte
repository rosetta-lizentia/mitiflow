<script lang="ts">
  import { onMount } from "svelte";
  import { push } from "svelte-spa-router";
  import { listConsumerGroups } from "$lib/api";
  import type { ConsumerGroupSummary } from "$lib/types";
  import { formatNumber } from "$lib/format";

  let groups = $state<ConsumerGroupSummary[]>([]);
  let error = $state("");

  onMount(async () => {
    try {
      groups = await listConsumerGroups();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  });
</script>

<div class="space-y-4">
  <h1 class="text-2xl font-bold">Consumer Groups</h1>

  {#if error}
    <p class="text-sm text-red-400">{error}</p>
  {/if}

  {#if groups.length === 0}
    <p class="text-gray-500">No consumer groups discovered.</p>
  {:else}
    <div class="overflow-x-auto rounded-lg border border-gray-800">
      <table class="w-full text-left text-sm">
        <thead class="border-b border-gray-800 bg-gray-900/50 text-xs uppercase text-gray-500">
          <tr>
            <th class="px-4 py-2">Group ID</th>
            <th class="px-4 py-2">Total Lag</th>
          </tr>
        </thead>
        <tbody>
          {#each groups as group}
            <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
              <td class="px-4 py-2">
                <button
                  class="font-medium text-indigo-400 hover:underline"
                  onclick={() => push(`/consumer-groups/${group.group_id}`)}
                >
                  {group.group_id}
                </button>
              </td>
              <td class="px-4 py-2">{formatNumber(group.total_lag)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>
