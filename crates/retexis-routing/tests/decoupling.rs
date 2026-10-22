//! The decoupling claim, as a test.
//!
//! "Routing knows nothing about the medium" is easy to say and easy to break:
//! one `use retexis_link_udp::...` in a handler and the layering is gone, with
//! no compiler error. These tests read the manifests instead of the code,
//! because the dependency graph *is* the architecture.
//!
//! If one of these fails, the fix is not to relax the test — it is to move the
//! code to a crate that is allowed to know about media.

use std::path::PathBuf;

fn manifest(crate_name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .join(crate_name)
        .join("Cargo.toml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Everything between `[dependencies]` and the next section header.
fn production_dependencies(crate_name: &str) -> String {
    let text = manifest(crate_name);
    let mut out = String::new();
    let mut inside = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            inside = trimmed == "[dependencies]";
            continue;
        }
        if inside && !trimmed.is_empty() && !trimmed.starts_with('#') {
            out.push_str(trimmed);
            out.push('\n');
        }
    }
    out
}

#[test]
fn routing_depends_on_no_medium() {
    let deps = production_dependencies("retexis-routing");
    assert!(deps.contains("retexis-core"), "routing needs the contract");
    assert!(deps.contains("retexis-crypto"), "routing seals envelopes");
    for forbidden in [
        "retexis-link",
        "retexis-discovery",
        "retexis-app",
        "retexis-sim",
        "retexis-node",
        "tokio",
        "std::net",
    ] {
        assert!(
            !deps.contains(forbidden),
            "retexis-routing must not depend on {forbidden}:\n{deps}"
        );
    }
}

#[test]
fn the_contract_crate_depends_on_nothing() {
    let deps = production_dependencies("retexis-core");
    assert!(
        deps.trim().is_empty(),
        "retexis-core is the vocabulary every crate agrees on; it must stay \
         dependency-free, found:\n{deps}"
    );
}

#[test]
fn the_data_layer_depends_on_no_medium() {
    // The data layer defines frames and streams. It must not know whether the
    // bytes travel over TCP, UDP or a simulated switch — that is what makes a
    // stream work unchanged on a medium that does not exist yet.
    let deps = production_dependencies("retexis-data");
    assert!(deps.contains("retexis-core"), "frames are envelopes");
    assert!(deps.contains("retexis-routing"), "streams ride on the mesh");
    for forbidden in [
        "retexis-link",
        "retexis-discovery",
        "retexis-app",
        "retexis-sim",
        "retexis-node",
    ] {
        assert!(
            !deps.contains(forbidden),
            "retexis-data must not depend on {forbidden}:\n{deps}"
        );
    }
}

#[test]
fn only_the_application_layer_knows_which_media_exist() {
    // The CLI is the composition root: the one place allowed to construct a
    // medium. Everything below resolves media through the registry, by scheme.
    let app = production_dependencies("retexis-app");
    assert!(app.contains("retexis-link-tcp"), "the CLI wires media");
    assert!(app.contains("retexis-data"), "the CLI consumes frames");
    assert!(app.contains("retexis-routing"), "the CLI boots a router");

    // ...and the dependency only ever points inward.
    for krate in [
        "retexis-core",
        "retexis-crypto",
        "retexis-discovery",
        "retexis-link-tcp",
        "retexis-link-udp",
        "retexis-data",
        "retexis-routing",
    ] {
        assert!(
            !production_dependencies(krate).contains("retexis-app"),
            "{krate} must not depend back on the application layer"
        );
    }
}

#[test]
fn the_simulator_reuses_routing_instead_of_reimplementing_it() {
    // The simulator is allowed to know about media (it *is* one), but it must
    // not depend on a real socket medium — that would mean it is not testing
    // the production path but a different one.
    let deps = production_dependencies("retexis-sim");
    assert!(
        deps.contains("retexis-routing"),
        "the sim drives real routers"
    );
    assert!(
        !deps.contains("retexis-link-tcp") && !deps.contains("retexis-link-udp"),
        "the simulated medium must not borrow a real one:\n{deps}"
    );
}

#[test]
fn every_medium_crate_implements_only_the_contract() {
    // A medium may use `core` (the traits) and `discovery` (broadcast), and
    // nothing above it. If a medium ever depends on routing, the layering has
    // been inverted.
    for medium in ["retexis-link-tcp", "retexis-link-udp"] {
        let deps = production_dependencies(medium);
        assert!(deps.contains("retexis-core"), "{medium} needs the traits");
        for forbidden in ["retexis-routing", "retexis-app", "retexis-node"] {
            assert!(
                !deps.contains(forbidden),
                "{medium} must not depend on {forbidden}:\n{deps}"
            );
        }
    }
}

#[test]
fn no_crate_pulls_in_a_c_compiler() {
    // A transport that only builds on machines with a C toolchain is a
    // transport that does not build on a LoRa node.
    for krate in [
        "retexis-core",
        "retexis-crypto",
        "retexis-discovery",
        "retexis-link-tcp",
        "retexis-link-udp",
        "retexis-routing",
        "retexis-app",
    ] {
        // Dependency lines only: a comment explaining *why* we avoid `cc`
        // must not be mistaken for a dependency on it.
        let deps = production_dependencies(krate);
        for forbidden in ["cc ", "cc=", "bindgen", "-sys", "cmake"] {
            assert!(
                !deps.contains(forbidden),
                "{krate} must stay pure Rust, found '{forbidden}' in://n{deps}"
            );
        }
    }
}
