<script lang="ts">
  let { label, value, subtitle = "" }: { label: string; value: string | number; subtitle?: string } = $props();
</script>

<div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
  <p class="text-xs font-medium uppercase tracking-wider text-gray-500">{label}</p>
  <p class="mt-1 text-2xl font-semibold text-gray-100">{value}</p>
  {#if subtitle}
    <p class="mt-0.5 text-xs text-gray-500">{subtitle}</p>
  {/if}
</div>
