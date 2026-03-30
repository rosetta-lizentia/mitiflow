<script lang="ts">
  import type { EventDetail } from "$lib/types";
  import { shortId } from "$lib/format";
  import PayloadViewer from "./PayloadViewer.svelte";

  let { event }: { event: EventDetail } = $props();
  let expanded = $state(false);
</script>

<div class="rounded-lg border border-gray-800 bg-gray-900/50">
  <button
    class="flex w-full items-center gap-4 px-4 py-2.5 text-left text-xs hover:bg-gray-800/30 transition-colors"
    onclick={() => expanded = !expanded}
  >
    <span class="font-mono text-gray-300 w-16">{event.seq}</span>
    <span class="w-10 text-gray-400">p{event.partition}</span>
    <span class="font-mono text-indigo-400 w-28">{shortId(event.publisher_id)}</span>
    <span class="text-gray-500 flex-1 truncate">{event.key ?? "—"}</span>
    <span class="text-gray-600 w-20 text-right">{event.payload_size} B</span>
    <span class="text-gray-600 w-44 text-right">{event.timestamp}</span>
    <span class="text-gray-600 w-5">{expanded ? "▴" : "▾"}</span>
  </button>

  {#if expanded}
    <div class="border-t border-gray-800 px-4 py-3 space-y-3">
      <div class="grid grid-cols-2 gap-2 text-xs">
        <div>
          <span class="text-gray-500">Event ID:</span>
          <span class="font-mono text-gray-300 ml-2">{event.event_id}</span>
        </div>
        <div>
          <span class="text-gray-500">Publisher:</span>
          <span class="font-mono text-gray-300 ml-2">{event.publisher_id}</span>
        </div>
        <div>
          <span class="text-gray-500">Key Expression:</span>
          <span class="font-mono text-gray-300 ml-2">{event.key_expr}</span>
        </div>
        <div>
          <span class="text-gray-500">Partition:</span>
          <span class="text-gray-300 ml-2">{event.partition}</span>
        </div>
        <div>
          <span class="text-gray-500">Sequence:</span>
          <span class="font-mono text-gray-300 ml-2">{event.seq}</span>
        </div>
        <div>
          <span class="text-gray-500">Timestamp:</span>
          <span class="text-gray-300 ml-2">{event.timestamp}</span>
        </div>
        {#if event.key}
          <div>
            <span class="text-gray-500">Key:</span>
            <span class="font-mono text-gray-300 ml-2">{event.key}</span>
          </div>
        {/if}
      </div>

      <PayloadViewer
        payloadBase64={event.payload_base64}
        payloadText={event.payload_text}
        payloadSize={event.payload_size}
      />
    </div>
  {/if}
</div>
