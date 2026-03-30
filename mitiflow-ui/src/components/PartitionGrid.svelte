<script lang="ts">
  let {
    partitions,
    numPartitions,
  }: {
    partitions: Array<{ partition: number; state: string; node_id?: string }>;
    numPartitions: number;
  } = $props();

  const stateColor: Record<string, string> = {
    Active: "bg-emerald-600",
    Recovering: "bg-amber-600",
    Starting: "bg-blue-600",
    Draining: "bg-orange-600",
    Stopped: "bg-red-600",
  };

  function getColor(p: number): string {
    const info = partitions.find((x) => x.partition === p);
    if (!info) return "bg-gray-800";
    return stateColor[info.state] ?? "bg-gray-700";
  }
</script>

<div class="flex flex-wrap gap-1">
  {#each Array.from({ length: numPartitions }, (_, i) => i) as p}
    <div
      class="h-6 w-6 rounded {getColor(p)}"
      title="Partition {p}"
    ></div>
  {/each}
</div>
