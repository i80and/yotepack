fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schemas")
        .file("schemas/block.capnp")
        .run()
        .expect("capnp compiler failed");

    capnpc::CompilerCommand::new()
        .src_prefix("schemas")
        .file("schemas/txnlog.capnp")
        .run()
        .expect("capnp compiler failed");
}
