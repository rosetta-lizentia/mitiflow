<script lang="ts">
  import { timeAgo } from "$lib/format";

  let { timestamp }: { timestamp: string } = $props();

  let display = $state("");

  $effect(() => {
    display = timeAgo(timestamp);
    const interval = setInterval(() => {
      display = timeAgo(timestamp);
    }, 5000);
    return () => clearInterval(interval);
  });
</script>

<span title={timestamp} class="text-gray-500">{display}</span>
