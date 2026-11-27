//! Serves gRPC services on the Unix socket the CSI sidecars and the kubelet connect to.

use std::path::{Path, PathBuf};

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::server::Router;

/// Strips the `unix://` scheme sidecars put in front of the socket path.
pub fn socket_path(endpoint: &str) -> &str {
    endpoint.strip_prefix("unix://").unwrap_or(endpoint)
}

/// Why the services are not served.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("socket {}: {source}", .path.display())]
    Socket { path: PathBuf, source: std::io::Error },
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Serves `router` on the socket until SIGTERM or SIGINT. A stale socket file of a previous
/// process is removed first: bind fails on an existing path.
pub async fn serve(router: Router, endpoint: &str) -> Result<(), ServeError> {
    let path = Path::new(socket_path(endpoint));
    let socket = |source| ServeError::Socket { path: path.to_path_buf(), source };
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(socket(e)),
    }
    let listener = UnixListener::bind(path).map_err(socket)?;
    tracing::info!(socket = %path.display(), "serving CSI");
    let told = async {
        shutdown().await;
        tracing::info!("shutting down");
    };
    router.serve_with_incoming_shutdown(UnixListenerStream::new(listener), told).await?;
    Ok(())
}

/// Becomes `true` when the process is told to stop: what runs beside the server ends with it.
pub fn stopping() -> tokio::sync::watch::Receiver<bool> {
    let (stop, stopping) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown().await;
        stop.send_replace(true);
    });
    stopping
}

async fn shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}
