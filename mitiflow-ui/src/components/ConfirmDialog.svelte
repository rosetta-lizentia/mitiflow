<script lang="ts">
  let {
    open = $bindable(false),
    title,
    message,
    confirmLabel = "Confirm",
    onconfirm,
  }: {
    open: boolean;
    title: string;
    message: string;
    confirmLabel?: string;
    onconfirm: () => void;
  } = $props();

  function confirm() {
    onconfirm();
    open = false;
  }
</script>

{#if open}
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
    onclick={() => (open = false)}
  >
    <div
      class="w-full max-w-md rounded-xl border border-gray-700 bg-gray-900 p-6 shadow-2xl"
      onclick={(e) => e.stopPropagation()}
    >
      <h3 class="text-lg font-semibold text-gray-100">{title}</h3>
      <p class="mt-2 text-sm text-gray-400">{message}</p>
      <div class="mt-6 flex justify-end gap-3">
        <button
          class="rounded-lg px-4 py-2 text-sm text-gray-400 hover:text-gray-200"
          onclick={() => (open = false)}
        >
          Cancel
        </button>
        <button
          class="rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-500"
          onclick={confirm}
        >
          {confirmLabel}
        </button>
      </div>
    </div>
  </div>
{/if}
