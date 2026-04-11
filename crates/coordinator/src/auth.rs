// Licensed under the Apache License, Version 2.0.
// API key authentication interceptor for gRPC.

use std::sync::Arc;

use tonic::{Request, Status};

/// API key validator — checks "authorization" metadata header.
#[derive(Clone)]
pub struct ApiKeyInterceptor {
    valid_keys: Arc<Vec<String>>,
}

impl ApiKeyInterceptor {
    /// Create a new interceptor. If keys is empty, all requests are allowed.
    pub fn new(keys: Vec<String>) -> Self {
        Self { valid_keys: Arc::new(keys) }
    }

    /// Returns true if authentication is required (non-empty key list).
    pub fn auth_required(&self) -> bool {
        !self.valid_keys.is_empty()
    }

    /// Validate a request's API key. Returns Ok(request) or Err(Status::unauthenticated).
    pub fn check<T>(&self, req: Request<T>) -> Result<Request<T>, Status> {
        if self.valid_keys.is_empty() {
            return Ok(req); // No auth configured
        }

        let key = req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s));

        match key {
            Some(k) if self.valid_keys.iter().any(|vk| vk == k) => Ok(req),
            Some(_) => Err(Status::unauthenticated("Invalid API key")),
            None => Err(Status::unauthenticated("Missing authorization header")),
        }
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
}
