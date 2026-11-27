//! local-path-csi: `controller` runs next to the CSI sidecars, `agent` runs on every storage node.

#![forbid(unsafe_code)]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lexopt::prelude::*;
use local_path_csi::agent::{Agent, Transport};
use local_path_csi::config::Config;
use local_path_csi::controller::{ControllerService, DEFAULT_AGENT_TIMEOUT};
use local_path_csi::csi::controller_server::ControllerServer;
use local_path_csi::csi::identity_server::IdentityServer;
use local_path_csi::csi::node_server::NodeServer;
use local_path_csi::events::Events;
use local_path_csi::http::Render;
use local_path_csi::identity::IdentityService;
use local_path_csi::leader::{LeaseRef, Timings};
use local_path_csi::metrics::AgentCounters;
use local_path_csi::moving::Holder;
use local_path_csi::node::NodeService;
use local_path_csi::server::serve;
use local_path_csi::store::KubeStore;
use local_path_csi::tls::Identity;
use local_path_csi::webhook::Registration;
use tonic::transport::Server;

const USAGE: &str = "\
usage: local-path-csi <controller|agent|crds> [options]

controller, agent:
  --endpoint <unix://path>   CSI socket to listen on (default unix:///csi/csi.sock)
  --metrics-address <addr>   address of /metrics and /healthz (default 0.0.0.0:9753)
  --api-group <group>        API group of LocalPathVolume and LocalPathNode (default local-path-csi.domik.fi)
  --driver-name <name>       name reported to the kubelet and written into every PersistentVolume
                             (default local-path.csi.domik.fi)

controller:
  --agent-timeout <seconds>  a node whose agent has not reported for this long takes no new
                             volumes and no resizes (default 120)
  --webhook-address <addr>   serve the pod webhook there, over TLS; needs POD_NAMESPACE. Without
                             the option there is no webhook
  --webhook-service <name>   Service the API server calls the webhook through (default local-path-csi-webhook)
  --webhook-secret <name>    Secret that keeps the certificate of the webhook (default local-path-csi-webhook)
  --webhook-configuration <name>
                             MutatingWebhookConfiguration that is told the certificate (default local-path-csi)

agent:
  --node-name <name>         name of the node the agent runs on (or NODE_NAME)
  --config <file>            pools of the storage nodes, JSON (default /etc/local-path-csi/config.json)
  --pod-ip <address>         address the agents of other nodes reach this one at (or POD_IP);
                             without it the node neither gives out nor receives volumes
  --rsync <path>             the rsync binary (default rsync)

crds:
  --out <dir>                write the CustomResourceDefinitions of the driver there
  --api-group <group>        API group of the definitions (default local-path-csi.domik.fi)";

/// The Lease of the replica of the controller that leads, in the namespace of the release.
const LEASE: &str = "local-path-csi-controller";

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Options {
    command: String,
    endpoint: String,
    node_name: Option<String>,
    config: PathBuf,
    out: Option<PathBuf>,
    agent_timeout: Duration,
    metrics_address: SocketAddr,
    pod_ip: Option<String>,
    rsync: PathBuf,
    webhook_address: Option<SocketAddr>,
    webhook_service: String,
    webhook_secret: String,
    webhook_configuration: String,
    api_group: String,
    driver_name: String,
}

fn parse() -> Result<Options, Error> {
    let mut options = Options {
        command: String::new(),
        endpoint: "unix:///csi/csi.sock".into(),
        node_name: std::env::var("NODE_NAME").ok().filter(|n| !n.is_empty()),
        config: "/etc/local-path-csi/config.json".into(),
        out: None,
        agent_timeout: DEFAULT_AGENT_TIMEOUT,
        metrics_address: SocketAddr::from(([0, 0, 0, 0], 9753)),
        pod_ip: std::env::var("POD_IP").ok().filter(|ip| !ip.is_empty()),
        rsync: "rsync".into(),
        webhook_address: None,
        webhook_service: "local-path-csi-webhook".into(),
        webhook_secret: "local-path-csi-webhook".into(),
        webhook_configuration: "local-path-csi".into(),
        api_group: local_path_csi::DEFAULT_API_GROUP.into(),
        driver_name: local_path_csi::DEFAULT_DRIVER_NAME.into(),
    };
    let mut parser = lexopt::Parser::from_env();
    while let Some(argument) = parser.next()? {
        match argument {
            Value(value) if options.command.is_empty() => options.command = value.string()?,
            Long("endpoint") => options.endpoint = parser.value()?.string()?,
            Long("node-name") => options.node_name = Some(parser.value()?.string()?),
            Long("config") => options.config = parser.value()?.into(),
            Long("out") => options.out = Some(parser.value()?.into()),
            Long("metrics-address") => options.metrics_address = parser.value()?.parse()?,
            Long("agent-timeout") => options.agent_timeout = Duration::from_secs(parser.value()?.parse()?),
            Long("pod-ip") => options.pod_ip = Some(parser.value()?.string()?),
            Long("rsync") => options.rsync = parser.value()?.into(),
            Long("webhook-address") => options.webhook_address = Some(parser.value()?.parse()?),
            Long("webhook-service") => options.webhook_service = parser.value()?.string()?,
            Long("webhook-secret") => options.webhook_secret = parser.value()?.string()?,
            Long("webhook-configuration") => options.webhook_configuration = parser.value()?.string()?,
            Long("api-group") => options.api_group = parser.value()?.string()?,
            Long("driver-name") => options.driver_name = parser.value()?.string()?,
            Short('h') | Long("help") => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(other.unexpected().into()),
        }
    }
    Ok(options)
}

/// The webhook only helps the scheduler: when it cannot start, the driver works without it.
async fn serve_webhook(
    client: kube::Client,
    registration: Registration,
    address: SocketAddr,
    store: Arc<KubeStore>,
    agent_timeout: Duration,
) {
    let started = async {
        let serving = local_path_csi::webhook::certificate(&client, &registration).await?;
        let tls = serving.server_config()?;
        tokio::spawn(local_path_csi::webhook::keep_trusted(client, registration, serving.authority));
        local_path_csi::webhook::serve(address, tls, store, agent_timeout.as_secs()).await?;
        Ok::<(), local_path_csi::tls::Error>(())
    };
    if let Err(e) = started.await {
        tracing::error!(%address, error = %e, "cannot serve the pod webhook: pods are scheduled without its advice");
    }
}

/// A metrics endpoint that cannot start is reported and does not stop the driver.
async fn serve_metrics(address: SocketAddr, render: Render) {
    if let Err(e) = local_path_csi::http::serve(address, render).await {
        tracing::error!(%address, error = %e, "cannot serve metrics");
    }
}

fn main() -> Result<(), Error> {
    // The remote shell of rsync: its arguments are a command for the far side, not options.
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.first().is_some_and(|command| command == "rsh") {
        std::process::exit(local_path_csi::transfer::rsh(&arguments[1..]));
    }
    let options = parse()?;
    if options.command == "agent" {
        // The agent starts processes, and some of them outlive the one that started them: the
        // process that was started stays behind to collect those, and the agent goes on in
        // another. Before the runtime: the process that stays behind has one of its own.
        local_path_csi::init::supervise()
            .map_err(|e| format!("agent: cannot start the process that collects its children: {e}"))?;
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run(options))
}

async fn run(options: Options) -> Result<(), Error> {
    tracing_subscriber::fmt().with_ansi(false).init();
    match options.command.as_str() {
        "controller" => {
            local_path_csi::configure(&options.api_group, &options.driver_name)?;
            let client = kube::Client::try_default().await?;
            let instance = std::env::var("HOSTNAME").ok();
            let events = Arc::new(Events::new(client.clone(), "local-path-csi-controller", instance.clone()));
            let stopping = local_path_csi::server::stopping();
            // What decides about volumes on its own, with no call to answer, runs in one replica.
            let namespace = std::env::var("POD_NAMESPACE").ok().filter(|namespace| !namespace.is_empty());
            let (leads, leading) = tokio::sync::watch::channel(namespace.is_none());
            let election = namespace.clone().map(|namespace| {
                let identity = instance.unwrap_or_else(|| format!("local-path-csi-{}", std::process::id()));
                let lease = LeaseRef { name: LEASE.into(), namespace, identity };
                let mut stopping = stopping.clone();
                let stopped = async move {
                    let _ = stopping.wait_for(|stopping| *stopping).await;
                };
                tokio::spawn(local_path_csi::leader::run(client.clone(), lease, Timings::default(), leads, stopped))
            });
            if election.is_none() {
                tracing::warn!("no POD_NAMESPACE: this process takes itself for the only controller");
            }
            tokio::spawn(local_path_csi::restore::run(client.clone(), events.clone(), leading.clone()));
            let store = Arc::new(KubeStore::start(client.clone()).await);
            if let Some(address) = options.webhook_address {
                let registration = Registration {
                    namespace: namespace.ok_or("controller: the webhook needs POD_NAMESPACE")?,
                    secret: options.webhook_secret,
                    service: options.webhook_service,
                    configuration: options.webhook_configuration,
                };
                tokio::spawn(serve_webhook(client, registration, address, store.clone(), options.agent_timeout));
            }
            let service = ControllerService::new(store)
                .with_agent_timeout(options.agent_timeout)
                .with_events(events)
                .with_stopping(stopping.clone());
            let service = Arc::new(service);
            tokio::spawn(service.clone().relocate(leading));
            let render: Render = Arc::new({
                let service = service.clone();
                move || {
                    let service = service.clone();
                    Box::pin(async move { service.metrics().await })
                }
            });
            tokio::spawn(serve_metrics(options.metrics_address, render));
            let router = Server::builder()
                .add_service(IdentityServer::new(IdentityService))
                .add_service(ControllerServer::from_arc(service));
            let served = serve(router, &options.endpoint).await;
            // The leader gives the Lease up before the process ends: the other replica has it at
            // once then, and not a lease duration later.
            if let Some(election) = election {
                let _ = tokio::time::timeout(Duration::from_secs(5), election).await;
            }
            Ok(served?)
        }
        "agent" => {
            local_path_csi::configure(&options.api_group, &options.driver_name)?;
            let node_name = options.node_name.ok_or("agent: --node-name or NODE_NAME is required")?;
            let text = std::fs::read_to_string(&options.config)
                .map_err(|e| format!("read {}: {e}", options.config.display()))?;
            let config = Arc::new(Config::parse(&text).map_err(|e| format!("{}: {e}", options.config.display()))?);
            let counters = Arc::new(AgentCounters::default());
            let client = kube::Client::try_default().await?;
            // A key of its own every time the agent starts: the other agents learn the
            // certificate from the LocalPathNode of this node.
            let identity = Arc::new(Identity::generate().map_err(|e| format!("cannot make a key: {e}"))?);
            let address = match &options.pod_ip {
                Some(ip) => {
                    let ip: IpAddr = ip.parse().map_err(|e| format!("pod IP {ip:?}: {e}"))?;
                    let any = if ip.is_ipv6() {
                        IpAddr::V6(Ipv6Addr::UNSPECIFIED)
                    } else {
                        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                    };
                    let listener = tokio::net::TcpListener::bind(SocketAddr::new(any, config.transfer.port)).await?;
                    let known = local_path_csi::moving::known_agents(client.clone());
                    let tls = identity.server_config(known).map_err(|e| format!("TLS: {e}"))?;
                    let holder = Arc::new(Holder::new(client.clone(), node_name.clone(), config.clone()));
                    let zstd_level = config.transfer.zstd_level();
                    tokio::spawn(local_path_csi::transfer::serve(
                        listener,
                        tls,
                        holder,
                        options.rsync.clone(),
                        zstd_level,
                    ));
                    Some(SocketAddr::new(ip, config.transfer.port).to_string())
                }
                None => {
                    tracing::warn!("no pod IP given: this node neither gives out nor receives volumes");
                    None
                }
            };
            let transport = Arc::new(Transport { address, identity, rsync: options.rsync.clone() });
            let agent = Agent::new(client, node_name.clone(), config.clone(), counters.clone(), transport);
            tokio::spawn(agent.run());
            let render: Render = Arc::new({
                let (node_name, config) = (node_name.clone(), config.clone());
                move || {
                    let (node_name, config, counters) = (node_name.clone(), config.clone(), counters.clone());
                    Box::pin(async move {
                        // The values are read from the disk and the quota when the page is asked for.
                        let read = tokio::task::spawn_blocking(move || {
                            let (pools, volumes, trash) = local_path_csi::agent::samples(&config);
                            local_path_csi::metrics::agent(&node_name, &pools, &volumes, &trash, &counters)
                        });
                        read.await.unwrap_or_default()
                    })
                }
            });
            tokio::spawn(serve_metrics(options.metrics_address, render));
            let router = Server::builder()
                .add_service(IdentityServer::new(IdentityService))
                .add_service(NodeServer::new(NodeService::new(node_name, config)));
            Ok(serve(router, &options.endpoint).await?)
        }
        "crds" => {
            let out = options.out.ok_or("crds: --out is required")?;
            local_path_csi::configure(&options.api_group, &options.driver_name)?;
            std::fs::create_dir_all(&out)?;
            for (file, text) in local_path_csi::api::crds() {
                std::fs::write(out.join(file), text)?;
            }
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}
