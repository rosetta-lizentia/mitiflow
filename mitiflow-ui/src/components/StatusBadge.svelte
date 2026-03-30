<script lang="ts">
  let { status }: { status: "online" | "offline" | "recovering" | string } = $props();

  const colors: Record<string, string> = {
    online: "bg-emerald-500/20 text-emerald-400",
    Active: "bg-emerald-500/20 text-emerald-400",
    offline: "bg-red-500/20 text-red-400",
    Stopped: "bg-red-500/20 text-red-400",
    recovering: "bg-amber-500/20 text-amber-400",
    Recovering: "bg-amber-500/20 text-amber-400",
    Starting: "bg-blue-500/20 text-blue-400",
    Draining: "bg-orange-500/20 text-orange-400",
  };

  const cls = $derived(colors[status] ?? "bg-gray-500/20 text-gray-400");
</script>

<span class="inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium {cls}">
  {status}
</span>
