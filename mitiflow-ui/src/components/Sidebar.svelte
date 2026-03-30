<script lang="ts">
  import { link } from "svelte-spa-router";
  import {
    LayoutDashboard,
    List,
    Server,
    Users,
    Eye,
    Shuffle,
    AlertTriangle,
  } from "lucide-svelte";

  const navItems = [
    { href: "/", label: "Dashboard", icon: LayoutDashboard },
    { href: "/topics", label: "Topics", icon: List },
    { href: "/nodes", label: "Nodes", icon: Server },
    { href: "/consumer-groups", label: "Consumer Groups", icon: Users },
    { href: "/events", label: "Event Inspector", icon: Eye },
    { href: "/overrides", label: "Overrides", icon: Shuffle },
    { href: "/dlq", label: "Dead Letter Queue", icon: AlertTriangle },
  ];
</script>

<aside
  class="flex w-56 flex-col border-r border-gray-800 bg-gray-900 px-3 py-4"
>
  <a href="/" use:link class="mb-6 flex items-center gap-2 px-2 text-lg font-bold text-indigo-400">
    <svg viewBox="0 0 32 32" class="h-7 w-7" fill="none">
      <rect width="32" height="32" rx="6" fill="#6366f1" />
      <path d="M8 22V10l8 6-8 6zm8-6l8 6V10l-8 6z" fill="white" opacity="0.9" />
    </svg>
    Mitiflow
  </a>

  <nav class="flex flex-col gap-1">
    {#each navItems as { href, label, icon: Icon }}
      <a
        {href}
        use:link
        class="flex items-center gap-3 rounded-lg px-3 py-2 text-sm text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-100"
      >
        <Icon size={18} />
        {label}
      </a>
    {/each}
  </nav>

  <div class="mt-auto px-2 text-xs text-gray-600">Mitiflow v0.1</div>
</aside>
