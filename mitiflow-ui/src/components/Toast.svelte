<script lang="ts">
  let messages = $state<{ id: number; text: string; kind: "info" | "error" }[]>([]);
  let nextId = 0;

  export function toast(text: string, kind: "info" | "error" = "info") {
    const id = nextId++;
    messages = [...messages, { id, text, kind }];
    setTimeout(() => {
      messages = messages.filter((m) => m.id !== id);
    }, 4000);
  }
</script>

<div class="pointer-events-none fixed right-4 top-4 z-50 flex flex-col gap-2">
  {#each messages as msg (msg.id)}
    <div
      class="pointer-events-auto rounded-lg border px-4 py-2 text-sm shadow-lg
        {msg.kind === 'error'
        ? 'border-red-800 bg-red-950 text-red-300'
        : 'border-gray-700 bg-gray-900 text-gray-200'}"
    >
      {msg.text}
    </div>
  {/each}
</div>
