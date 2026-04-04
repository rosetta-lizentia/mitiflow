import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  shortId,
  formatBytes,
  formatNumber,
  formatMicros,
  timeAgo,
} from "$lib/format";

describe("shortId", () => {
  it("returns first 8 characters of a UUID", () => {
    expect(shortId("550e8400-e29b-41d4-a716-446655440000")).toBe("550e8400");
  });

  it("returns the full string if shorter than 8", () => {
    expect(shortId("abc")).toBe("abc");
  });

  it("handles empty string", () => {
    expect(shortId("")).toBe("");
  });
});

describe("formatBytes", () => {
  it("formats zero bytes", () => {
    expect(formatBytes(0)).toBe("0 B");
  });

  it("formats bytes", () => {
    expect(formatBytes(512)).toBe("512 B");
  });

  it("formats kilobytes", () => {
    expect(formatBytes(1024)).toBe("1.0 KB");
  });

  it("formats megabytes", () => {
    expect(formatBytes(1048576)).toBe("1.0 MB");
  });

  it("formats gigabytes", () => {
    expect(formatBytes(1073741824)).toBe("1.0 GB");
  });

  it("formats fractional values", () => {
    expect(formatBytes(1536)).toBe("1.5 KB");
  });
});

describe("formatNumber", () => {
  it("formats small numbers", () => {
    expect(formatNumber(42)).toBeTruthy();
  });

  it("formats zero", () => {
    expect(formatNumber(0)).toBe("0");
  });
});

describe("formatMicros", () => {
  it("formats microseconds", () => {
    expect(formatMicros(500)).toBe("500 µs");
  });

  it("formats milliseconds", () => {
    expect(formatMicros(1500)).toBe("1.5 ms");
  });

  it("formats seconds", () => {
    expect(formatMicros(2_500_000)).toBe("2.50 s");
  });

  it("formats boundary at 999 µs", () => {
    expect(formatMicros(999)).toBe("999 µs");
  });

  it("formats boundary at 1000 µs", () => {
    expect(formatMicros(1000)).toBe("1.0 ms");
  });
});

describe("timeAgo", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2025-01-15T12:00:00Z"));
  });

  it("returns seconds ago for recent timestamps", () => {
    expect(timeAgo("2025-01-15T11:59:30Z")).toBe("30s ago");
  });

  it("returns minutes ago", () => {
    expect(timeAgo("2025-01-15T11:55:00Z")).toBe("5m ago");
  });

  it("returns hours ago", () => {
    expect(timeAgo("2025-01-15T09:00:00Z")).toBe("3h ago");
  });

  it("returns days ago", () => {
    expect(timeAgo("2025-01-13T12:00:00Z")).toBe("2d ago");
  });

  it("handles future timestamps", () => {
    expect(timeAgo("2025-01-15T13:00:00Z")).toBe("just now");
  });

  afterEach(() => {
    vi.useRealTimers();
  });
});
