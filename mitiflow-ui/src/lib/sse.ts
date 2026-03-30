// SSE (Server-Sent Events) client wrapper with auto-reconnect.

export function createSSE<T>(
  url: string,
  eventType: string,
  onMessage: (data: T) => void,
): { close: () => void } {
  const source = new EventSource(url);

  source.addEventListener(eventType, (e: MessageEvent) => {
    try {
      const data: T = JSON.parse(e.data);
      onMessage(data);
    } catch {
      // ignore parse errors
    }
  });

  source.onerror = () => {
    console.warn(`SSE connection error for ${url}, reconnecting...`);
  };

  return { close: () => source.close() };
}
