//! Generates the CSI gRPC bindings from the vendored spec. protox compiles the proto in pure
//! Rust, so the build needs no protoc binary.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/csi.proto");
    let descriptors = protox::compile(["proto/csi.proto"], ["proto"])?;
    tonic_prost_build::configure().build_client(false).compile_fds(descriptors)?;
    Ok(())
}
