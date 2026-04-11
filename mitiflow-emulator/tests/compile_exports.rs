//! Compile-test: verify all public types are importable from mitiflow_emulator.

// This test verifies that all new modules and their public types are correctly
// exported from the crate. If this file compiles, the exports are correct.

use mitiflow_emulator::invariant_checker::{InvariantChecker, InvariantReport, InvariantVerdict};
use mitiflow_emulator::metrics::{EventManifestEntry, ManifestRole, ManifestWriter};
use mitiflow_emulator::network_fault::{
    ContainerNetworkFaultInjector, FaultGuard, NetworkFaultInjector,
};
use mitiflow_emulator::restart_channel::{RestartReceiver, RestartRequest, RestartSender};

#[test]
fn public_types_importable() {
    // If this compiles, all exports are correct.
    // Use the types to prevent unused import warnings.
    let _: Option<RestartRequest> = None;
    let _: Option<RestartSender> = None;
    let _: Option<RestartReceiver> = None;
    let _: Option<EventManifestEntry> = None;
    let _: Option<ManifestRole> = None;
    let _: Option<ManifestWriter> = None;
    let _: Option<Box<dyn NetworkFaultInjector>> = None;
    let _: Option<FaultGuard> = None;
    let _: Option<ContainerNetworkFaultInjector> = None;
    let _: Option<InvariantChecker> = None;
    let _: Option<InvariantReport> = None;
    let _: Option<InvariantVerdict> = None;
}
