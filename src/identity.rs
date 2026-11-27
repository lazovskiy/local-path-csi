//! CSI Identity service, shared by the controller and the agent.

use tonic::{Request, Response, Status};

use crate::csi::identity_server::Identity;
use crate::csi::plugin_capability::{Service, Type, VolumeExpansion, service, volume_expansion};
use crate::csi::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest, GetPluginInfoResponse,
    PluginCapability, ProbeRequest, ProbeResponse,
};
use crate::driver_name;

pub struct IdentityService;

fn service_capability(kind: service::Type) -> PluginCapability {
    PluginCapability { r#type: Some(Type::Service(Service { r#type: kind.into() })) }
}

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: driver_name().into(),
            vendor_version: env!("CARGO_PKG_VERSION").into(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![
                service_capability(service::Type::ControllerService),
                service_capability(service::Type::VolumeAccessibilityConstraints),
                // The limit of a mounted volume is raised in place.
                PluginCapability {
                    r#type: Some(Type::VolumeExpansion(VolumeExpansion {
                        r#type: volume_expansion::Type::Online.into(),
                    })),
                },
            ],
        }))
    }

    async fn probe(&self, _: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}
