//! Health and metrics endpoint.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;

/// Produces the metrics page: called on every scrape, so the values are as fresh as the scrape.
pub type Render = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    let content_type = header::HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8");
    response.headers_mut().insert(header::CONTENT_TYPE, content_type);
    response
}

pub async fn respond<B>(render: &Render, request: &Request<B>) -> Response<Full<Bytes>> {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return text(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed\n");
    }
    match request.uri().path() {
        "/healthz" => text(StatusCode::OK, "OK\n"),
        "/metrics" => text(StatusCode::OK, render().await),
        _ => text(StatusCode::NOT_FOUND, "404 page not found\n"),
    }
}

/// Serves `/metrics` and `/healthz` until the process is stopped.
pub async fn serve(address: SocketAddr, render: Render) -> std::io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    tracing::info!(%address, "serving metrics");
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let render = render.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: Request<Incoming>| {
                let render = render.clone();
                async move { Ok::<_, Infallible>(respond(&render, &request).await) }
            });
            let connection = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(std::time::Duration::from_secs(30))
                .serve_connection(TokioIo::new(stream), service);
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "connection closed with error");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(method: Method, path: &str) -> (StatusCode, Bytes) {
        use http_body_util::BodyExt;
        let render: Render = Arc::new(|| Box::pin(async { "local_path_csi_test 1\n".to_string() }));
        let response = respond(&render, &Request::builder().method(method).uri(path).body(()).unwrap()).await;
        let status = response.status();
        (status, response.into_body().collect().await.unwrap().to_bytes())
    }

    #[tokio::test]
    async fn endpoints() {
        assert_eq!(get(Method::GET, "/healthz").await.0, StatusCode::OK);
        assert_eq!(get(Method::GET, "/metrics").await, (StatusCode::OK, Bytes::from("local_path_csi_test 1\n")));
        assert_eq!(get(Method::GET, "/other").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get(Method::POST, "/metrics").await.0, StatusCode::METHOD_NOT_ALLOWED);
    }
}
