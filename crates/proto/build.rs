fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile_protos(
            &[
                "proto/lance_service.proto",
                // Phase C: Control Plane protocol. Same package
                // (`lance.distributed`) so generated types share a
                // namespace with the existing service. See
                // docs/ARCHITECTURE_V2_PLAN.md §3 Phase C.
                "proto/cluster_control.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
