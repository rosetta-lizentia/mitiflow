use mitiflow::store::FjallBackend;
use mitiflow::*;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = "test/qfinal";
    let key = format!("{prefix}/_store/0");
    let dir = tempfile::tempdir().unwrap();
    let backend = FjallBackend::open(dir.path(), 0).unwrap();
    let config = EventBusConfig::builder(prefix)
        .cache_size(10)
        .build()
        .unwrap();
    let mut store = EventStore::new(&session, backend, config.clone());
    store.run().await.unwrap();

    let publisher = EventPublisher::new(&session, config.clone()).await.unwrap();
    let pub_key = format!("{prefix}/p/0/test");
    for i in 0..5u32 {
        publisher
            .publish_bytes_to(&pub_key, format!("msg-{i}").into_bytes())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Query WITH accept_replies(Any) — should work now
    let r = session
        .get(&key)
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    let mut c = 0;
    while let Ok(reply) = r.recv_async().await {
        if reply.result().is_ok() {
            c += 1;
        }
    }
    eprintln!("With accept_replies(Any): {c} replies (expected 5)");

    store.shutdown();
    drop(publisher);
    session.close().await.unwrap();
}
