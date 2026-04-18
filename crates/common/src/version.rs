// Licensed under the Apache License, Version 2.0.
// Server version parsing + compatibility helpers.
//
// The server advertises its CARGO_PKG_VERSION in
// `HealthCheckResponse.server_version` (B3.1). Clients use the helpers
// here to decide whether to enable newer wire fields or fall back to
// pre-0.2 behaviour. Keeping the parser here (common crate) rather
// than on either end avoids re-implementing it in the Python SDK,
// admin CLI, and any future internal client.

/// Parsed `MAJOR.MINOR.PATCH[-pre]` view of a server version string.
/// See `docs/COMPAT_POLICY.md` for the full semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    /// Prerelease suffix without the leading dash (e.g. "alpha.1",
    /// "pre.2"). `None` for stable releases.
    pub pre: Option<String>,
}

impl ServerVersion {
    /// Parse a CARGO_PKG_VERSION-formatted string. Returns `None` for
    /// anything that doesn't match the `MAJOR.MINOR.PATCH[-pre]` shape —
    /// including empty strings, which the server sends when running a
    /// pre-B3.1 build that doesn't populate the field at all.
    pub fn parse(s: &str) -> Option<Self> {
        let (core, pre) = match s.split_once('-') {
            Some((c, p)) if !p.is_empty() => (c, Some(p.to_string())),
            Some((c, _)) => (c, None), // trailing hyphen with empty body
            None => (s, None),
        };
        let mut parts = core.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self { major, minor, patch, pre })
    }

    /// "Is the server advertising at least this major.minor?"
    ///
    /// Patch and prerelease suffix are intentionally ignored here —
    /// a prerelease of the target minor (`0.2.0-alpha.1`) is considered
    /// to advertise `0.2` capabilities. Clients that need stricter
    /// guarantees can compare the full tuple themselves.
    pub fn at_least(&self, major: u32, minor: u32) -> bool {
        (self.major, self.minor) >= (major, minor)
    }

    /// Pre-B3.1 servers send an empty string. Detect that case.
    pub fn is_legacy(s: &str) -> bool {
        s.is_empty() || Self::parse(s).is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stable_version() {
        let v = ServerVersion::parse("1.2.3").unwrap();
        assert_eq!(v, ServerVersion { major: 1, minor: 2, patch: 3, pre: None });
    }

    #[test]
    fn parse_prerelease_version() {
        let v = ServerVersion::parse("0.2.0-alpha.1").unwrap();
        assert_eq!(v.major, 0);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 0);
        assert_eq!(v.pre.as_deref(), Some("alpha.1"));
    }

    #[test]
    fn parse_pre_hyphen_in_suffix() {
        // Multi-part prerelease like "rc.1-hotfix" — we take everything
        // after the first '-' as the prerelease lump.
        let v = ServerVersion::parse("1.0.0-rc.1-hotfix").unwrap();
        assert_eq!(v.pre.as_deref(), Some("rc.1-hotfix"));
    }

    #[test]
    fn rejects_missing_patch() {
        assert!(ServerVersion::parse("1.2").is_none());
    }

    #[test]
    fn rejects_four_part_core() {
        assert!(ServerVersion::parse("1.2.3.4").is_none());
    }

    #[test]
    fn rejects_non_numeric() {
        assert!(ServerVersion::parse("1.foo.3").is_none());
        assert!(ServerVersion::parse("").is_none());
    }

    #[test]
    fn rejects_empty_pre() {
        // "1.0.0-" with a hyphen but no prerelease body doesn't parse as
        // a valid semver — we normalize to no-prerelease rather than
        // erroring here, but the is_legacy check still catches empty
        // strings from pre-B3.1 servers.
        let v = ServerVersion::parse("1.0.0-").unwrap();
        assert!(v.pre.is_none(), "trailing hyphen with nothing after is treated as stable");
    }

    #[test]
    fn at_least_ignores_patch_and_pre() {
        let v = ServerVersion::parse("0.2.0-alpha.1").unwrap();
        assert!(v.at_least(0, 2), "0.2.0-alpha.1 should advertise 0.2 capabilities");
        assert!(!v.at_least(0, 3));
        assert!(!v.at_least(1, 0));
    }

    #[test]
    fn at_least_higher_major_wins() {
        let v = ServerVersion::parse("2.0.0").unwrap();
        assert!(v.at_least(0, 99), "higher major beats any minor");
        assert!(v.at_least(1, 5));
        assert!(v.at_least(2, 0));
        assert!(!v.at_least(3, 0));
    }

    #[test]
    fn is_legacy_on_empty_and_malformed() {
        assert!(ServerVersion::is_legacy(""));
        assert!(ServerVersion::is_legacy("not-a-version"));
        assert!(!ServerVersion::is_legacy("0.1.0"));
    }

    #[test]
    fn round_trip_current_crate_version() {
        // This test ensures CARGO_PKG_VERSION stays parseable. If
        // someone bumps Cargo.toml to a format we can't parse, this
        // test catches it at unit-test time instead of in the
        // customer's client SDK.
        let v = env!("CARGO_PKG_VERSION");
        assert!(ServerVersion::parse(v).is_some(),
                "crate version {v:?} must be parseable by ServerVersion::parse");
    }
}
