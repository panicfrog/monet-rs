//! Engine lifecycle and pin-injection tests against the fake libcronet.

mod fake;

use std::sync::{Mutex as TestMutex, OnceLock};

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<TestMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| TestMutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

use futures_lite::future::block_on;
use monet_core::Url;
use monet_core::method::Method;
use monet_core::pin::PinConfig;
use monet_core::request::Request;
use monet_core::{CancelToken, Transport};
use monet_cronet::{CronetEngine, CronetTransport, EngineConfig, HttpCache};
use std::sync::Arc;

// A real base64 SHA-256 (43 chars + '='), valid pin shape for tests.
const HASH: &str = "r2Kn1DbzEZBROoLdOZLRIyD0WfTGWhCsFlusjMO6Zjo=";

#[test]
fn engine_start_shutdown_and_version() {
    let _guard = test_lock();
    fake::reset();
    let mut engine = CronetEngine::start(EngineConfig::default()).unwrap();
    assert!(engine.version().starts_with("cronet-fake/"));
    engine.shutdown().unwrap();
    // A second shutdown is a no-op.
    engine.shutdown().unwrap();
    let (started, shutdowns, _, _) = fake::engine_events();
    assert_eq!(started, 1);
    assert_eq!(shutdowns, 1);
}

#[test]
fn user_agent_and_storage_path_reach_the_engine() {
    let _guard = test_lock();
    fake::reset();
    let dir = std::env::temp_dir().join("monet-cronet-test-cache");
    let config = EngineConfig {
        user_agent: Some("monet-test/1.0".into()),
        http_cache: HttpCache::Disk {
            path: dir.clone(),
            max_size: 1024 * 1024,
        },
        ..Default::default()
    };
    let _engine = CronetEngine::start(config).unwrap();

    let (_, _, user_agent, storage_path) = fake::engine_events();
    assert_eq!(user_agent.as_deref(), Some("monet-test/1.0"));
    assert_eq!(storage_path, Some(dir.to_string_lossy().into_owned()));
    assert!(dir.exists(), "cache directory is created before start");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pins_are_injected_in_cronet_format() {
    let _guard = test_lock();
    fake::reset();
    let pins = PinConfig::from_json(&format!(
        r#"{{"api.example.com": {{"publicKeyHashes": ["{HASH}", "{HASH}"], "includeSubdomains": true, "expirationDate": "2099-01-01"}}}}"#
    ))
    .unwrap();
    let config = EngineConfig {
        pins,
        ..Default::default()
    };
    let engine = Arc::new(CronetEngine::start(config).unwrap());

    let recorded = fake::recorded_pins();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].host, "api.example.com");
    assert_eq!(recorded[0].hashes, vec![format!("sha256/{HASH}"); 2]);
    assert!(recorded[0].include_subdomains);
    // 2099-01-01 UTC midnight in milliseconds since the epoch
    // (129 years, 32 of them leap: 47117 days * 86400000).
    assert_eq!(recorded[0].expiration_ms, 4_070_908_800_000);

    // The engine still answers requests after pin injection.
    let transport = CronetTransport::new(engine);
    let resp = block_on(transport.execute(
        Request::new(Method::Get, Url::parse("https://api.example.com/").unwrap()),
        CancelToken::new(),
    ))
    .unwrap();
    assert_eq!(resp.status, 200);
}
