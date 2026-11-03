//! End-to-end transport tests against the fake libcronet.

mod fake;

use std::sync::{Mutex as TestMutex, OnceLock};

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<TestMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| TestMutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

use bytes::Bytes;
use futures_lite::StreamExt;
use futures_lite::future::block_on;
use monet_core::Url;
use monet_core::error::{Error, PlatformErrorCode};
use monet_core::method::Method;
use monet_core::request::Request;
use monet_core::{CancelToken, HeaderMap, Transport};
use monet_cronet::{CronetEngine, CronetTransport, EngineConfig};
use std::sync::Arc;

fn url() -> Url {
    Url::parse("https://api.example.com/v1/things").unwrap()
}

fn transport() -> (CronetTransport, Arc<CronetEngine>) {
    fake::reset();
    let engine = Arc::new(CronetEngine::start(EngineConfig::default()).unwrap());
    let transport = CronetTransport::new(engine.clone());
    (transport, engine)
}

#[test]
fn get_streams_head_then_chunks() {
    let _guard = test_lock();
    let (transport, _engine) = transport();
    fake::configure(fake::FakeConfig {
        status: 200,
        status_text: "OK".into(),
        response_headers: vec![
            ("content-type".into(), "text/plain".into()),
            ("x-multi".into(), "a".into()),
            ("x-multi".into(), "b".into()),
        ],
        body: b"hello world, hello cronet".to_vec(),
        chunk_size: 5,
        ..Default::default()
    });

    let resp =
        block_on(transport.execute(Request::new(Method::Get, url()), CancelToken::new())).unwrap();

    assert_eq!(resp.status, 200);
    assert_eq!(resp.headers.get("content-type").unwrap(), "text/plain");
    assert_eq!(resp.headers.get("x-multi").unwrap(), "a, b");
    let body = block_on(resp.body.map(|c| c.unwrap()).collect::<Vec<Bytes>>());
    // chunk_size = 5 over 25 bytes
    let sizes: Vec<usize> = body.iter().map(Bytes::len).collect();
    assert_eq!(sizes, [5, 5, 5, 5, 5]);
    let joined: Vec<u8> = body.concat().to_vec();
    assert_eq!(joined, b"hello world, hello cronet".to_vec());

    let recorded = fake::last_request().unwrap();
    assert_eq!(recorded.url, "https://api.example.com/v1/things");
    assert_eq!(recorded.method, "GET");
    assert!(recorded.upload.is_none());
}

#[test]
fn empty_body_yields_no_chunks() {
    let _guard = test_lock();
    let (transport, _engine) = transport();
    let resp =
        block_on(transport.execute(Request::new(Method::Get, url()), CancelToken::new())).unwrap();
    let body = block_on(resp.body.map(|c| c.unwrap()).collect::<Vec<Bytes>>());
    assert!(body.is_empty());
}

#[test]
fn post_records_method_headers_and_upload() {
    let _guard = test_lock();
    let (transport, _engine) = transport();
    let req = Request::new(Method::Post, url())
        .with_header("content-type", "application/json")
        .unwrap()
        .with_body_bytes(br#"{"hello":"cronet"}"#);

    let resp = block_on(transport.execute(req, CancelToken::new())).unwrap();
    assert_eq!(resp.status, 200);

    let recorded = fake::last_request().unwrap();
    assert_eq!(recorded.method, "POST");
    assert_eq!(
        recorded
            .headers
            .iter()
            .find(|(n, _)| n == "content-type")
            .unwrap()
            .1,
        "application/json"
    );
    assert_eq!(recorded.upload.unwrap(), br#"{"hello":"cronet"}"#.to_vec());
}

#[test]
fn failure_maps_to_transport_error_with_cronet_code() {
    let _guard = test_lock();
    let (transport, _engine) = transport();
    fake::configure(fake::FakeConfig {
        fail: Some((11, "net::ERR_TIMED_OUT".into())),
        ..Default::default()
    });

    let err = block_on(transport.execute(Request::new(Method::Get, url()), CancelToken::new()))
        .unwrap_err();
    match err {
        Error::Transport { message, code } => {
            assert!(message.contains("ERR_TIMED_OUT"), "got: {message}");
            assert_eq!(code, Some(PlatformErrorCode::new("cronet", 11)));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn redirect_is_followed_by_default() {
    let _guard = test_lock();
    let (transport, _engine) = transport();
    fake::configure(fake::FakeConfig {
        status: 201,
        redirect_to: Some("https://api.example.com/v2/things".into()),
        ..Default::default()
    });

    let resp =
        block_on(transport.execute(Request::new(Method::Get, url()), CancelToken::new())).unwrap();
    // Followed: the final (configured) response arrives, not the 302.
    assert_eq!(resp.status, 201);
}

#[test]
fn pre_cancelled_token_fails_fast_without_touching_cronet() {
    let _guard = test_lock();
    let (transport, _engine) = transport();
    let cancel = CancelToken::new();
    cancel.cancel();

    let err = block_on(transport.execute(Request::new(Method::Get, url()), cancel)).unwrap_err();
    assert!(matches!(err, Error::Cancelled));
    assert!(fake::last_request().is_none());
}

#[test]
fn request_headers_reach_cronet() {
    let _guard = test_lock();
    let (transport, _engine) = transport();
    let req = Request::new(Method::Get, url())
        .with_header("Authorization", "Bearer token")
        .unwrap()
        .with_header("x-custom", "1")
        .unwrap();

    block_on(transport.execute(req, CancelToken::new())).unwrap();

    let recorded = fake::last_request().unwrap();
    let headers: HeaderMap = {
        let mut h = HeaderMap::new();
        for (n, v) in &recorded.headers {
            h.append(n, v.clone()).unwrap();
        }
        h
    };
    assert_eq!(headers.get("authorization").unwrap(), "Bearer token");
    assert_eq!(headers.get("x-custom").unwrap(), "1");
}
