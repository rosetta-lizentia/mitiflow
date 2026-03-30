<script lang="ts">
  let { values, width = 100, height = 24, color = "#818cf8" }: {
    values: number[];
    width?: number;
    height?: number;
    color?: string;
  } = $props();

  const points = $derived.by(() => {
    if (values.length < 2) return "";
    const max = Math.max(...values, 1);
    const step = width / (values.length - 1);
    return values
      .map((v, i) => `${i * step},${height - (v / max) * height}`)
      .join(" ");
  });
</script>

{#if values.length >= 2}
  <svg {width} {height} class="inline-block align-middle">
    <polyline
      {points}
      fill="none"
      stroke={color}
      stroke-width="1.5"
      stroke-linecap="round"
      stroke-linejoin="round"
    />
  </svg>
{:else}
  <span class="text-xs text-gray-600">—</span>
{/if}
