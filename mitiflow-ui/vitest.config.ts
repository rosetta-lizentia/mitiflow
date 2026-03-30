import { defineConfig } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  plugins: [svelte()],
  resolve: {
    alias: {
      $lib: "/src/lib",
    },
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
    globals: true,
  },
});
