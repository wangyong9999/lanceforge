// LanceForge v2 architecture — role composition layer.
//
// Each role (QN / PE / IDX / CP) is expressed as an async fn that
// takes a shared config + shutdown signal and returns when the role
// has finished draining. A binary composes roles by spawning one or
// more `run()` functions inside the same tokio runtime.
//
// In Phase A (this crate's initial shape) we preserve the existing
// two-process deployment: `qn_cp::run` wraps everything the coord
// binary used to do in-process, `pe_idx::run` wraps everything the
// worker binary did. Later phases (C–D) split the composite runners
// into four independent `qn::run`, `pe::run`, `idx::run`, `cp::run`
// functions. During the split the composite functions stay as thin
// orchestrators around the individual runners, so the monolith
// binary in Phase B can reuse them unchanged.
//
// The split-but-composable shape lets us reorganise code before
// introducing any new protocol; the goal of Phase A is zero
// behavioural change.

#![allow(clippy::too_many_arguments)]

pub mod common;
pub mod qn_cp;
pub mod pe_idx;

// Individual-role modules are introduced incrementally. In Phase A
// they re-export the composite's internals where possible; C/D
// phases migrate them to full standalone runners.
pub mod qn;
pub mod pe;
pub mod idx;
pub mod cp;
