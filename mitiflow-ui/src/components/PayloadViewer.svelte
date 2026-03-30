<script lang="ts">
  let { payloadBase64, payloadText, payloadSize }: {
    payloadBase64: string;
    payloadText?: string;
    payloadSize: number;
  } = $props();

  type ViewMode = "auto" | "json" | "text" | "hex";
  let mode = $state<ViewMode>("auto");

  // Try to parse as JSON
  const jsonParsed = $derived.by(() => {
    if (!payloadText) return null;
    try {
      return JSON.stringify(JSON.parse(payloadText), null, 2);
    } catch {
      return null;
    }
  });

  // Decode base64 to hex
  const hexView = $derived.by(() => {
    try {
      const binary = atob(payloadBase64);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
      const lines: string[] = [];
      for (let i = 0; i < bytes.length; i += 16) {
        const chunk = bytes.slice(i, i + 16);
        const hex = Array.from(chunk).map(b => b.toString(16).padStart(2, "0")).join(" ");
        const ascii = Array.from(chunk).map(b => b >= 32 && b < 127 ? String.fromCharCode(b) : ".").join("");
        lines.push(`${i.toString(16).padStart(8, "0")}  ${hex.padEnd(48)}  ${ascii}`);
      }
      return lines.join("\n");
    } catch {
      return "(decode error)";
    }
  });

  const autoContent = $derived(jsonParsed ?? payloadText ?? hexView);
  const autoMode = $derived(jsonParsed ? "json" : payloadText ? "text" : "hex");

  const displayContent = $derived.by(() => {
    if (mode === "auto") return autoContent;
    if (mode === "json") return jsonParsed ?? "(not valid JSON)";
    if (mode === "text") return payloadText ?? "(not valid UTF-8)";
    return hexView;
  });

  const displayMode = $derived(mode === "auto" ? autoMode : mode);
</script>

<div class="rounded border border-gray-700 bg-gray-950">
  <div class="flex items-center gap-2 border-b border-gray-800 px-3 py-1.5">
    <span class="text-xs text-gray-500">Payload ({payloadSize} bytes)</span>
    <div class="ml-auto flex gap-1">
      {#each ["auto", "json", "text", "hex"] as m}
        <button
          class="rounded px-2 py-0.5 text-xs transition-colors {mode === m ? 'bg-indigo-600 text-white' : 'text-gray-500 hover:text-gray-300'}"
          onclick={() => mode = m as ViewMode}
        >{m}</button>
      {/each}
    </div>
  </div>
  <pre class="max-h-80 overflow-auto whitespace-pre-wrap p-3 text-xs font-mono {displayMode === 'json' ? 'text-green-400' : displayMode === 'hex' ? 'text-amber-400' : 'text-gray-300'}">{displayContent}</pre>
</div>
