//! Local mock provider fixtures. These are test doubles, not Discord or Cloudflare.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::Value;

/// One scripted mock response.
#[derive(Clone)]
pub(crate) struct Scripted {
    pub status: StatusCode,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
    pub delay: Duration,
}

impl Scripted {
    pub(crate) fn json(status: u16, body: Value) -> Self {
        Self {
            status: StatusCode::from_u16(status).unwrap(),
            headers: vec![("content-type", "application/json".to_owned())],
            body: body.to_string(),
            delay: Duration::ZERO,
        }
    }

    pub(crate) fn with_header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_owned()));
        self
    }

    pub(crate) fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

/// A request captured by a mock fixture.
#[derive(Clone, Debug)]
pub(crate) struct Captured {
    pub at: std::time::Instant,
    pub path: String,
    pub query: Option<String>,
    pub authorization: Option<String>,
    pub body: Value,
}

#[derive(Default)]
struct Shared {
    script: VecDeque<Scripted>,
    fallback: Option<Scripted>,
    requests: Vec<Captured>,
}

/// A mock HTTP provider on an ephemeral loopback port.
#[derive(Clone)]
pub(crate) struct MockProvider {
    pub address: SocketAddr,
    shared: Arc<Mutex<Shared>>,
}

impl MockProvider {
    /// Starts a mock that answers with `fallback` once `script` is exhausted.
    pub(crate) async fn start(script: Vec<Scripted>, fallback: Scripted) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            script: script.into(),
            fallback: Some(fallback),
            requests: Vec::new(),
        }));
        let app = Router::new()
            .fallback(post(handle))
            .with_state(Arc::clone(&shared));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { address, shared }
    }

    /// A mock Discord webhook that creates messages with increasing IDs.
    pub(crate) async fn discord(script: Vec<Scripted>) -> Self {
        Self::start(
            script,
            Scripted::json(
                200,
                serde_json::json!({"id": "200000000000000001", "type": 0}),
            ),
        )
        .await
    }

    pub(crate) fn discord_url(&self) -> String {
        format!(
            "http://{}/api/webhooks/123456789012345678/mock-token",
            self.address
        )
    }

    pub(crate) fn requests(&self) -> Vec<Captured> {
        self.shared.lock().unwrap().requests.clone()
    }

    /// Waits until at least `count` requests arrived.
    pub(crate) async fn wait_for(&self, count: usize) -> Vec<Captured> {
        for _ in 0..200 {
            let requests = self.requests();
            if requests.len() >= count {
                return requests;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "mock provider saw {} of {count} requests",
            self.requests().len()
        );
    }
}

async fn handle(
    State(shared): State<Arc<Mutex<Shared>>>,
    uri: axum::http::Uri,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let reply = {
        let mut shared = shared.lock().unwrap();
        shared.requests.push(Captured {
            at: std::time::Instant::now(),
            path: uri.path().to_owned(),
            query,
            authorization: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        });
        match shared.script.pop_front() {
            Some(reply) => reply,
            None => shared.fallback.clone().unwrap(),
        }
    };
    tokio::time::sleep(reply.delay).await;
    let mut response = (reply.status, reply.body).into_response();
    for (name, value) in reply.headers {
        response.headers_mut().insert(name, value.parse().unwrap());
    }
    response
}
