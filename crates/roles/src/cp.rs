// Control Plane role — placeholder for Phase A.
//
// Phase A keeps CP-shaped background tasks (auth hot-reload, audit
// sink, orphan GC, health check loop, REST/metrics server) inside
// `qn_cp::run`. The standalone CP runner lands in Phase C alongside
// the ClusterControl gRPC service, at which point QN starts pulling
// routing / policy over the wire rather than sharing an in-process
// Arc.
