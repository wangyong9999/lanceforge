// Licensed under the Apache License, Version 2.0.
// Coordinator HA — multiple active Coordinators for high availability.
//
// Strategy: Multi-Active (Milvus Proxy model)
// - All Coordinator instances are active simultaneously
// - Each independently connects to all Workers
// - Each maintains its own ShardState copy (from same config)
// - Client connects via load balancer (NLB/HAProxy/K8s Service)
// - Any Coordinator can handle any query — fully stateless
//
// This is simpler and more robust than Active-Standby:
// - No leader election needed
// - No failover delay (other instances already serving)
// - No state transfer between instances
// - Horizontal scaling: add more Coordinators = more query capacity
//
// The only shared state is the ShardMapping in config (or etcd).
// Each Coordinator reads it independently.

use log::info;

/// HA configuration for Coordinator deployment.
#[derive(Debug, Clone)]
pub struct HaConfig {
    /// Instance ID (for logging/metrics differentiation)
    pub instance_id: String,
    /// Total number of Coordinator instances (informational)
    pub total_instances: u32,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            instance_id: format!("coordinator-{}", std::process::id()),
            total_instances: 1,
        }
    }
}

impl HaConfig {
    pub fn log_status(&self) {
        info!(
            "Coordinator HA: instance={}, total_instances={}, mode=multi-active",
            self.instance_id, self.total_instances
        );
    }
}

/// Health status for this Coordinator instance (exposed via cluster status API).
pub struct CoordinatorHealth {
    pub instance_id: String,
    pub is_active: bool, // Always true in multi-active mode
    pub connected_workers: u32,
    pub uptime_secs: u64,
    start_time: std::time::Instant,
}

impl CoordinatorHealth {
    pub fn new(instance_id: String) -> Self {
        Self {
            instance_id,
            is_active: true,
            connected_workers: 0,
            uptime_secs: 0,
            start_time: std::time::Instant::now(),
        }
    }

    pub fn update(&mut self, connected_workers: u32) {
        self.connected_workers = connected_workers;
        self.uptime_secs = self.start_time.elapsed().as_secs();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ha_config_default() {
        let config = HaConfig::default();
        assert!(config.instance_id.starts_with("coordinator-"));
        assert_eq!(config.total_instances, 1);
    }

    #[test]
    fn test_coordinator_health() {
        let mut health = CoordinatorHealth::new("test-1".to_string());
        assert!(health.is_active);
        assert_eq!(health.connected_workers, 0);

        health.update(3);
        assert_eq!(health.connected_workers, 3);
        assert!(health.uptime_secs < 1); // just created
    }
}
