// Licensed under the Apache License, Version 2.0.
// API key authentication + role-based authorization interceptor for gRPC.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Status};

/// Access role for an API key. Higher roles include lower roles' permissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Read-only: search, count, list, get_schema, get_status.
    Read,
    /// Write: Read + add/delete/upsert rows.
    Write,
    /// Admin: Write + create_table, drop_table, create_index, rebalance.
    Admin,
}

impl Role {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "read" => Some(Role::Read),
            "write" => Some(Role::Write),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }
}

/// Permission level required by an RPC method.
#[derive(Clone, Copy, Debug)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl Permission {
    fn satisfied_by(&self, role: Role) -> bool {
        match self {
            Permission::Read => role >= Role::Read,
            Permission::Write => role >= Role::Write,
            Permission::Admin => role >= Role::Admin,
        }
    }
}

/// API key validator + role lookup. Checks "authorization" metadata header.
#[derive(Clone)]
pub struct ApiKeyInterceptor {
    /// Map from key → role. Empty map = no auth required (all allowed as Admin).
    keys: Arc<HashMap<String, Role>>,
}

impl ApiKeyInterceptor {
    /// Legacy constructor: all keys granted Admin role (backward compatible).
    pub fn new(keys: Vec<String>) -> Self {
        let map: HashMap<String, Role> =
            keys.into_iter().map(|k| (k, Role::Admin)).collect();
        Self { keys: Arc::new(map) }
    }

    /// New constructor with explicit role assignment.
    pub fn with_roles(keys: HashMap<String, Role>) -> Self {
        Self { keys: Arc::new(keys) }
    }

    /// Returns true if authentication is required (non-empty key list).
    pub fn auth_required(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Validate key only (no permission check). Returns the role if valid.
    pub fn authenticate<T>(&self, req: &Request<T>) -> Result<Role, Status> {
        if self.keys.is_empty() {
            return Ok(Role::Admin); // No auth configured — allow everything
        }
        let key = req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s));
        match key {
            Some(k) => self.keys.get(k).copied()
                .ok_or_else(|| Status::unauthenticated("Invalid API key")),
            None => Err(Status::unauthenticated("Missing authorization header")),
        }
    }

    /// Legacy API: authenticate only, return request unchanged.
    pub fn check<T>(&self, req: Request<T>) -> Result<Request<T>, Status> {
        self.authenticate(&req)?;
        Ok(req)
    }

    /// Authenticate and authorize for a required permission level.
    pub fn authorize<T>(&self, req: &Request<T>, needed: Permission) -> Result<(), Status> {
        let role = self.authenticate(req)?;
        if needed.satisfied_by(role) {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "{:?} permission required (role={:?})", needed, role
            )))
        }
    }

    /// Extract a short, non-secret principal identifier for audit logs.
    /// Returns the prefix of the API key (first 8 chars) or "anonymous".
    pub fn principal<T>(&self, req: &Request<T>) -> String {
        if self.keys.is_empty() {
            return "anonymous".to_string();
        }
        req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s))
            .map(|k| {
                let take = k.len().min(8);
                format!("key:{}…", &k[..take])
            })
            .unwrap_or_else(|| "missing".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn make_request_with_key(key: &str) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {}", key)).unwrap(),
        );
        req
    }

    #[test]
    fn test_no_auth_configured() {
        let interceptor = ApiKeyInterceptor::new(vec![]);
        assert!(!interceptor.auth_required());
        let req = Request::new(());
        assert!(interceptor.check(req).is_ok());
    }

    #[test]
    fn test_valid_key() {
        let interceptor = ApiKeyInterceptor::new(vec!["secret-key-1".into()]);
        assert!(interceptor.auth_required());
        let req = make_request_with_key("secret-key-1");
        assert!(interceptor.check(req).is_ok());
    }

    #[test]
    fn test_invalid_key() {
        let interceptor = ApiKeyInterceptor::new(vec!["secret-key-1".into()]);
        let req = make_request_with_key("wrong-key");
        let err = interceptor.check(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("Invalid"));
    }

    #[test]
    fn test_missing_header() {
        let interceptor = ApiKeyInterceptor::new(vec!["secret-key-1".into()]);
        let req = Request::new(());
        let err = interceptor.check(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("Missing"));
    }

    #[test]
    fn test_multiple_valid_keys() {
        let interceptor = ApiKeyInterceptor::new(vec!["key-a".into(), "key-b".into()]);
        assert!(interceptor.check(make_request_with_key("key-a")).is_ok());
        assert!(interceptor.check(make_request_with_key("key-b")).is_ok());
        assert!(interceptor.check(make_request_with_key("key-c")).is_err());
    }

    #[test]
    fn test_key_without_bearer_prefix() {
        let interceptor = ApiKeyInterceptor::new(vec!["raw-key".into()]);
        let mut req = Request::new(());
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("raw-key").unwrap(),
        );
        assert!(interceptor.check(req).is_ok());
    }

    #[test]
    fn test_role_hierarchy() {
        assert!(Permission::Read.satisfied_by(Role::Read));
        assert!(Permission::Read.satisfied_by(Role::Write));
        assert!(Permission::Read.satisfied_by(Role::Admin));
        assert!(!Permission::Write.satisfied_by(Role::Read));
        assert!(Permission::Write.satisfied_by(Role::Write));
        assert!(Permission::Admin.satisfied_by(Role::Admin));
        assert!(!Permission::Admin.satisfied_by(Role::Write));
    }

    #[test]
    fn test_rbac_read_cannot_admin() {
        let mut m = HashMap::new();
        m.insert("reader".to_string(), Role::Read);
        m.insert("writer".to_string(), Role::Write);
        m.insert("boss".to_string(), Role::Admin);
        let ic = ApiKeyInterceptor::with_roles(m);

        let r = make_request_with_key("reader");
        assert!(ic.authorize(&r, Permission::Read).is_ok());
        let r = make_request_with_key("reader");
        assert!(ic.authorize(&r, Permission::Write).is_err());
        let r = make_request_with_key("reader");
        assert!(ic.authorize(&r, Permission::Admin).is_err());

        let r = make_request_with_key("writer");
        assert!(ic.authorize(&r, Permission::Write).is_ok());
        let r = make_request_with_key("writer");
        assert!(ic.authorize(&r, Permission::Admin).is_err());

        let r = make_request_with_key("boss");
        assert!(ic.authorize(&r, Permission::Admin).is_ok());
    }

    #[test]
    fn test_role_parse() {
        assert_eq!(Role::parse("read"), Some(Role::Read));
        assert_eq!(Role::parse("WRITE"), Some(Role::Write));
        assert_eq!(Role::parse("Admin"), Some(Role::Admin));
        assert_eq!(Role::parse("bogus"), None);
    }
}
