//! TLS of the driver: the agents of two nodes prove who they are to each other, and the API
//! server reaches the webhook of the controller.

use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Name the agents put into their certificates and ask each other for. It identifies nobody:
/// an agent is recognised by its whole certificate.
pub const AGENT_NAME: &str = "local-path-csi-agent";

/// Tells whether a certificate (DER) is the one of an agent of the cluster. Asked in the
/// handshake: a client it does not know gets no further.
pub type Admits = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// The first certificate of a PEM text.
pub fn certificate(pem: &str) -> Option<CertificateDer<'static>> {
    CertificateDer::from_pem_slice(pem.as_bytes()).ok()
}

/// Key and self-signed certificate of an agent. Made anew every time the agent starts and
/// published in the LocalPathNode of its node: nothing is stored and nothing has to be renewed.
pub struct Identity {
    certificate_pem: String,
    certificate: CertificateDer<'static>,
    key: PrivatePkcs8KeyDer<'static>,
}

impl Identity {
    pub fn generate() -> Result<Self, Error> {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(vec![AGENT_NAME.to_string()])?;
        params.distinguished_name.push(DnType::CommonName, AGENT_NAME);
        let certificate = params.self_signed(&key)?;
        Ok(Self {
            certificate_pem: certificate.pem(),
            certificate: certificate.der().clone(),
            key: PrivatePkcs8KeyDer::from(key.serialize_der()),
        })
    }

    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(self.key.clone_key())
    }

    /// For the side that serves volumes. A client must present a certificate `admits` knows:
    /// the handshake ends there for anybody else. Which agent it is, the caller decides after
    /// the handshake, when the client has named its node.
    pub fn server_config(&self, admits: Admits) -> Result<Arc<ServerConfig>, Error> {
        let verifier = Arc::new(Pinned { expected: None, admits: Some(admits), provider: provider() });
        let config = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![self.certificate.clone()], self.key())?;
        Ok(Arc::new(config))
    }

    /// For the side that fetches a volume: the server must present exactly `peer_pem`, the
    /// certificate its LocalPathNode shows.
    pub fn client_config(&self, peer_pem: &str) -> Result<Arc<ClientConfig>, Error> {
        let expected = certificate(peer_pem).ok_or("the certificate of the peer is not a PEM certificate")?;
        let verifier = Arc::new(Pinned { expected: Some(expected), admits: None, provider: provider() });
        let config = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(vec![self.certificate.clone()], self.key())?;
        Ok(Arc::new(config))
    }
}

/// Accepts a peer by its whole certificate instead of a chain to an authority. The handshake
/// signature is verified as usual: the peer owns the key of the certificate it presents.
struct Pinned {
    /// The only certificate a server may present; `None` on the serving side.
    expected: Option<CertificateDer<'static>>,
    /// The certificates a client may present; `None` on the fetching side.
    admits: Option<Admits>,
    provider: Arc<CryptoProvider>,
}

impl std::fmt::Debug for Pinned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pinned").field("expected", &self.expected).finish_non_exhaustive()
    }
}

impl Pinned {
    fn tls12(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn tls13(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.expected.as_ref().is_some_and(|expected| expected == end_entity) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("the peer is not the agent its LocalPathNode describes".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes()
    }
}

impl ClientCertVerifier for Pinned {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        if self.admits.as_ref().is_some_and(|admits| admits(end_entity.as_ref())) {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(rustls::Error::General("the peer is not an agent of this cluster".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes()
    }
}

/// A serving certificate with the authority that signed it, all PEM.
pub struct Serving {
    /// What a client has to trust.
    pub authority: String,
    pub certificate: String,
    pub key: String,
}

impl Serving {
    /// Issues a certificate for `names`. The key of the authority is thrown away: nothing else
    /// can ever be signed by it.
    pub fn generate(names: &[String]) -> Result<Self, Error> {
        let authority_key = KeyPair::generate()?;
        let mut authority = CertificateParams::new(Vec::new())?;
        authority.distinguished_name.push(DnType::CommonName, "local-path-csi webhook authority");
        authority.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let authority_certificate = authority.self_signed(&authority_key)?;

        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(names.to_vec())?;
        params
            .distinguished_name
            .push(DnType::CommonName, names.first().map(String::as_str).unwrap_or("local-path-csi"));
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let certificate = params.signed_by(&key, &Issuer::from_params(&authority, &authority_key))?;
        Ok(Self { authority: authority_certificate.pem(), certificate: certificate.pem(), key: key.serialize_pem() })
    }

    /// TLS for a server any client may talk to.
    pub fn server_config(&self) -> Result<Arc<ServerConfig>, Error> {
        let certificate = certificate(&self.certificate).ok_or("the serving certificate is not a PEM certificate")?;
        let key = PrivateKeyDer::from_pem_slice(self.key.as_bytes())?;
        let config = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)?;
        Ok(Arc::new(config))
    }
}
