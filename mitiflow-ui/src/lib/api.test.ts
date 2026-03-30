import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

// Must import AFTER stubbing fetch
const api = await import("$lib/api");

describe("API client", () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  // ── Topics ──────────────────────────────────────────────────────

  describe("listTopics", () => {
    it("sends GET /api/v1/topics", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => [{ name: "t1" }],
      });

      const result = await api.listTopics();
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/topics",
        expect.objectContaining({ headers: { "Content-Type": "application/json" } }),
      );
      expect(result).toEqual([{ name: "t1" }]);
    });
  });

  describe("getTopic", () => {
    it("sends GET /api/v1/topics/:name", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ name: "my-topic", num_partitions: 4 }),
      });

      const result = await api.getTopic("my-topic");
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/topics/my-topic",
        expect.anything(),
      );
      expect(result.name).toBe("my-topic");
    });

    it("encodes special characters in topic name", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ name: "a/b" }),
      });

      await api.getTopic("a/b");
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/topics/a%2Fb",
        expect.anything(),
      );
    });
  });

  describe("createTopic", () => {
    it("sends POST with JSON body", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ name: "new-topic" }),
      });

      await api.createTopic({ name: "new-topic" } as any);
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/topics",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ name: "new-topic" }),
        }),
      );
    });
  });

  describe("updateTopic", () => {
    it("sends PUT with partial body", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ name: "t1", replication_factor: 3 }),
      });

      await api.updateTopic("t1", { replication_factor: 3 } as any);
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/topics/t1",
        expect.objectContaining({
          method: "PUT",
          body: JSON.stringify({ replication_factor: 3 }),
        }),
      );
    });
  });

  describe("deleteTopic", () => {
    it("sends DELETE and returns undefined for 204", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 204,
      });

      const result = await api.deleteTopic("old-topic");
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/topics/old-topic",
        expect.objectContaining({ method: "DELETE" }),
      );
      expect(result).toBeUndefined();
    });
  });

  // ── Error handling ────────────────────────────────────────────────

  describe("error handling", () => {
    it("throws ApiError on non-ok response", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        text: async () => "not found",
      });

      await expect(api.listTopics()).rejects.toThrow("not found");
    });

    it("includes status code in error", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        text: async () => "server error",
      });

      try {
        await api.listTopics();
        expect.fail("should have thrown");
      } catch (err: any) {
        expect(err.status).toBe(500);
      }
    });
  });

  // ── Cluster ───────────────────────────────────────────────────────

  describe("getClusterNodes", () => {
    it("sends GET /api/v1/cluster/nodes", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({}),
      });

      await api.getClusterNodes();
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/cluster/nodes",
        expect.anything(),
      );
    });
  });

  describe("drainNode", () => {
    it("sends POST to drain endpoint", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ drained: true }),
      });

      await api.drainNode("node-1", { target_node: "node-2" } as any);
      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/cluster/nodes/node-1/drain",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ target_node: "node-2" }),
        }),
      );
    });
  });

  // ── Consumer Groups ───────────────────────────────────────────────

  describe("listConsumerGroups", () => {
    it("sends GET /api/v1/consumer-groups", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => [],
      });

      const result = await api.listConsumerGroups();
      expect(result).toEqual([]);
    });
  });

  describe("resetConsumerGroup", () => {
    it("sends POST with reset body", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          group_id: "g1",
          topic: "t1",
          partition: 0,
          offset: 0,
        }),
      });

      await api.resetConsumerGroup("g1", {
        topic: "t1",
        partition: 0,
        strategy: "earliest",
      } as any);

      expect(mockFetch).toHaveBeenCalledWith(
        "/api/v1/consumer-groups/g1/reset",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            topic: "t1",
            partition: 0,
            strategy: "earliest",
          }),
        }),
      );
    });
  });

  // ── Events ────────────────────────────────────────────────────────

  describe("queryEvents", () => {
    it("builds query string from params", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ events: [], total: 0, has_more: false }),
      });

      await api.queryEvents({ topic: "my-topic", limit: 50 });

      const calledUrl = mockFetch.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/api/v1/events?");
      expect(calledUrl).toContain("topic=my-topic");
      expect(calledUrl).toContain("limit=50");
    });

    it("only includes defined params", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ events: [], total: 0, has_more: false }),
      });

      await api.queryEvents({ topic: "t1" });

      const calledUrl = mockFetch.mock.calls[0][0] as string;
      expect(calledUrl).toContain("topic=t1");
      expect(calledUrl).not.toContain("partition=");
      expect(calledUrl).not.toContain("limit=");
    });

    it("includes all filter params when provided", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ events: [], total: 0, has_more: false }),
      });

      await api.queryEvents({
        topic: "t1",
        partition: 2,
        after_seq: 10,
        before_seq: 100,
        publisher_id: "pub-123",
        key: "order-456",
        limit: 25,
      });

      const calledUrl = mockFetch.mock.calls[0][0] as string;
      expect(calledUrl).toContain("partition=2");
      expect(calledUrl).toContain("after_seq=10");
      expect(calledUrl).toContain("before_seq=100");
      expect(calledUrl).toContain("publisher_id=pub-123");
      expect(calledUrl).toContain("key=order-456");
      expect(calledUrl).toContain("limit=25");
    });
  });
});
