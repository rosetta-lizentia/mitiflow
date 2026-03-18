# Improvement idea


- configurable encoding/decoding format. (currently fix on serde_json) ✅
- shared subscriber in durable publish (currently create a new subscriber each time it called) ✅
  - NOTE: Must use broadcast channel (not mpsc/flume) so all concurrent `publish_durable` callers receive every watermark update. Flume only delivers to one receiver.