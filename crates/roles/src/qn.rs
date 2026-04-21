// Query Node role — placeholder for Phase A.
//
// In Phase A the composite `qn_cp::run` still owns the full coord
// logic. This module is reserved for the independent QN runner
// introduced in Phase C (once QN starts pulling routing from CP via
// gRPC subscribe instead of sharing an in-process Arc).
//
// Keeping the module empty with a doc comment ensures callers can
// already refer to `roles::qn` in code that's in flight.
