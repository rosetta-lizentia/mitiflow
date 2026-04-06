//! Integration tests for the topic schema registry.
//!
//! These tests validate the end-to-end schema lifecycle: registration,
//! fetching, validation, auto-configuration, and the RegisterOrValidate
//! protocol — all over real Zenoh sessions.

mod common;

use std::time::Duration;

use mitiflow::{
    CodecFormat, EventBusConfig, EventPublisher, EventSubscriber, KeyFormat, TopicSchemaMode,
};

/// RegisterOrValidate publisher registers a schema that a second session can fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_register_and_fetch() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let config = EventBusConfig::builder("test/schema_register_fetch")
        .codec(CodecFormat::Postcard)
        .num_partitions(8)
        .schema_mode(TopicSchemaMode::RegisterOrValidate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();

    // Publisher registers schema (first publisher path)
    let publisher = EventPublisher::new(&session, config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second session fetches it
    let schema = mitiflow::schema::fetch_schema(&session, "test/schema_register_fetch")
        .await
        .unwrap();
    assert_eq!(schema.codec, CodecFormat::Postcard);
    assert_eq!(schema.num_partitions, 8);
    assert_eq!(schema.key_format, KeyFormat::Unkeyed);
    assert_eq!(schema.schema_version, 1);

    drop(publisher);
    session.close().await.unwrap();
}

/// Validate mode succeeds when the local config matches the registered schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_validate_matching() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // First publisher registers the schema
    let config1 = EventBusConfig::builder("test/schema_val_match")
        .codec(CodecFormat::Postcard)
        .num_partitions(16)
        .schema_mode(TopicSchemaMode::RegisterOrValidate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let pub1 = EventPublisher::new(&session, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second publisher validates — should succeed
    let config2 = EventBusConfig::builder("test/schema_val_match")
        .codec(CodecFormat::Postcard)
        .num_partitions(16)
        .schema_mode(TopicSchemaMode::Validate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let pub2 = EventPublisher::new(&session, config2).await.unwrap();

    drop(pub1);
    drop(pub2);
    session.close().await.unwrap();
}

/// Validate mode returns TopicSchemaMismatch on codec mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_validate_mismatch() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // First publisher registers with Postcard
    let config1 = EventBusConfig::builder("test/schema_val_mismatch")
        .codec(CodecFormat::Postcard)
        .num_partitions(8)
        .schema_mode(TopicSchemaMode::RegisterOrValidate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let pub1 = EventPublisher::new(&session, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second publisher tries to validate with Json — should fail
    let config2 = EventBusConfig::builder("test/schema_val_mismatch")
        .codec(CodecFormat::Json)
        .num_partitions(8)
        .schema_mode(TopicSchemaMode::Validate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let result = EventPublisher::new(&session, config2).await;
    assert!(result.is_err(), "expected publisher creation to fail");
    let err = result.err().unwrap();
    assert!(
        matches!(err, mitiflow::Error::TopicSchemaMismatch { ref field, .. } if field == "codec"),
        "expected TopicSchemaMismatch on codec, got: {err:?}"
    );

    drop(pub1);
    session.close().await.unwrap();
}

/// AutoConfig mode overwrites local codec and num_partitions from the registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_autoconfig() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // First publisher registers with Postcard / 16 partitions
    let config1 = EventBusConfig::builder("test/schema_autoconfig")
        .codec(CodecFormat::Postcard)
        .num_partitions(16)
        .schema_mode(TopicSchemaMode::RegisterOrValidate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let pub1 = EventPublisher::new(&session, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Subscriber with wrong local config + AutoConfig — should be overwritten
    let config2 = EventBusConfig::builder("test/schema_autoconfig")
        .codec(CodecFormat::Json) // wrong, will be overwritten
        .num_partitions(4) // wrong, will be overwritten
        .schema_mode(TopicSchemaMode::AutoConfig)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let sub = EventSubscriber::new(&session, config2).await.unwrap();
    assert_eq!(sub.config().codec, CodecFormat::Postcard);
    assert_eq!(sub.config().num_partitions, 16);

    drop(pub1);
    drop(sub);
    session.close().await.unwrap();
}

/// Disabled mode creates publisher/subscriber without querying _schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_disabled_no_query() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // No schema registered — Disabled mode should not error
    let config = EventBusConfig::builder("test/schema_disabled")
        .schema_mode(TopicSchemaMode::Disabled)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let publisher = EventPublisher::new(&session, config.clone())
        .await
        .unwrap();
    let subscriber = EventSubscriber::new(&session, config).await.unwrap();

    drop(publisher);
    drop(subscriber);
    session.close().await.unwrap();
}

/// RegisterOrValidate validates when a schema already exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_register_or_validate_existing() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // First publisher registers
    let config1 = EventBusConfig::builder("test/schema_rov_existing")
        .codec(CodecFormat::MsgPack)
        .num_partitions(4)
        .schema_mode(TopicSchemaMode::RegisterOrValidate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let pub1 = EventPublisher::new(&session, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second publisher with matching config — should validate successfully
    let config2 = EventBusConfig::builder("test/schema_rov_existing")
        .codec(CodecFormat::MsgPack)
        .num_partitions(4)
        .schema_mode(TopicSchemaMode::RegisterOrValidate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let pub2 = EventPublisher::new(&session, config2).await.unwrap();

    drop(pub1);
    drop(pub2);
    session.close().await.unwrap();
}

/// from_topic() convenience constructor builds config from registered schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_from_topic_autoconfig() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Register a schema
    let config1 = EventBusConfig::builder("test/schema_from_topic")
        .codec(CodecFormat::MsgPack)
        .num_partitions(32)
        .schema_mode(TopicSchemaMode::RegisterOrValidate)
        .heartbeat(mitiflow::HeartbeatMode::Disabled)
        .build()
        .unwrap();
    let pub1 = EventPublisher::new(&session, config1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // from_topic() should fetch and apply the schema
    let config2: EventBusConfig = EventBusConfig::from_topic(&session, "test/schema_from_topic")
        .await
        .unwrap();
    assert_eq!(config2.codec, CodecFormat::MsgPack);
    assert_eq!(config2.num_partitions, 32);
    assert_eq!(config2.key_prefix, "test/schema_from_topic");

    drop(pub1);
    session.close().await.unwrap();
}
