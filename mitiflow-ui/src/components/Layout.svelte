<script lang="ts">
  import Sidebar from "./Sidebar.svelte";
  import type { Snippet } from "svelte";

  let { children }: { children: Snippet } = $props();
</script>

<div class="flex h-screen overflow-hidden">
  <Sidebar />
  <main class="flex-1 overflow-y-auto p-6">
    {@render children()}
  </main>
</div>
