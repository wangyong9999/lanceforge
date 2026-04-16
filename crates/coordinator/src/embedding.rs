// Licensed under the Apache License, Version 2.0.
// Query-time text→vector embedding using lancedb EmbeddingFunction trait.
//
// Supports: OpenAI, SentenceTransformers (via lancedb features).
// When a query comes with text instead of vector, this module converts it.

use std::sync::Arc;

use arrow::array::{Array, Float32Array};
use datafusion::error::{DataFusionError, Result};
use log::debug;

use lance_distributed_common::config::EmbeddingConfig;

/// Embed a text query into a vector using the configured embedding model.
///
/// For MVP: delegates to lancedb's EmbeddingFunction implementations.
/// If no embedding config is set, returns an error (text queries not supported).
pub async fn embed_query(
    text: &str,
    config: &EmbeddingConfig,
) -> Result<Vec<f32>> {
    debug!("Embedding query text: '{}' with model {}/{}", text, config.provider, config.model);

    match config.provider.as_str() {
        #[cfg(feature = "openai")]
        "openai" => embed_openai(text, config).await,

        #[cfg(feature = "sentence-transformers")]
        "sentence-transformers" => embed_sentence_transformers(text, config).await,

        // Fallback: use a simple hash-based mock embedding for testing
        // This allows text queries to work without external API dependencies
        "mock" | "test" => {
            let dim = config.dimension.unwrap_or(32) as usize;
            Ok(mock_embedding(text, dim))
        }

        other => Err(DataFusionError::Plan(format!(
            "Unsupported embedding provider: '{}'. Available: mock{}{}",
            other,
            if cfg!(feature = "openai") { ", openai" } else { "" },
            if cfg!(feature = "sentence-transformers") { ", sentence-transformers" } else { "" },
        ))),
    }
}

/// Check if embedding is configured.
pub fn is_embedding_configured(config: &Option<EmbeddingConfig>) -> bool {
    config.is_some()
}

/// Mock embedding for testing — deterministic hash-based vector.
/// NOT for production use (no semantic meaning).
fn mock_embedding(text: &str, dim: usize) -> Vec<f32> {
    let mut vec = Vec::with_capacity(dim);
    let mut hash: u64 = 5381;
    for byte in text.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    for i in 0..dim {
        hash = hash.wrapping_mul(6364136223846793005).wrapping_add(i as u64);
        vec.push(((hash >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    vec
}

#[cfg(feature = "openai")]
async fn embed_openai(text: &str, config: &EmbeddingConfig) -> Result<Vec<f32>> {
    use lancedb::embeddings::openai::OpenAIEmbeddingFunction;
    use lancedb::embeddings::EmbeddingFunction;

    let api_key = config.api_key.clone()
        .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        .ok_or_else(|| DataFusionError::Plan(
            "OpenAI API key required: set in config or OPENAI_API_KEY env var".to_string()
        ))?;

    // Use lancedb's OpenAI embedding function
    let func = OpenAIEmbeddingFunction::new(&api_key, &config.model);
    let input = Arc::new(StringArray::from(vec![text])) as Arc<dyn Array>;
    let result = func.compute_query_embeddings(input)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Extract float values from the FixedSizeList result
    extract_vector_from_embedding(&result)
}

#[cfg(feature = "sentence-transformers")]
async fn embed_sentence_transformers(text: &str, config: &EmbeddingConfig) -> Result<Vec<f32>> {
    use lancedb::embeddings::sentence_transformers::SentenceTransformersEmbeddingFunction;
    use lancedb::embeddings::EmbeddingFunction;

    let func = SentenceTransformersEmbeddingFunction::new(&config.model)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let input = Arc::new(StringArray::from(vec![text])) as Arc<dyn Array>;
    let result = func.compute_query_embeddings(input)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    extract_vector_from_embedding(&result)
}

/// Extract a Vec<f32> from an embedding function result (FixedSizeList of Float32).
#[allow(dead_code)] // Used when openai/sentence-transformers features enabled
fn extract_vector_from_embedding(result: &Arc<dyn Array>) -> Result<Vec<f32>> {
    // The result is typically a FixedSizeListArray with one row
    if let Some(list) = result.as_any().downcast_ref::<arrow::array::FixedSizeListArray>()
        && list.len() > 0 {
            let values = list.value(0);
            if let Some(floats) = values.as_any().downcast_ref::<Float32Array>() {
                return Ok(floats.values().to_vec());
            }
        }
    // Fallback: try as flat Float32Array
    if let Some(floats) = result.as_any().downcast_ref::<Float32Array>() {
        return Ok(floats.values().to_vec());
    }
    Err(DataFusionError::Plan("Cannot extract vector from embedding result".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_embedding_deterministic() {
        let v1 = mock_embedding("hello", 8);
        let v2 = mock_embedding("hello", 8);
        assert_eq!(v1, v2, "Same input should produce same output");

        let v3 = mock_embedding("world", 8);
        assert_ne!(v1, v3, "Different input should produce different output");
    }

    #[test]
    fn test_mock_embedding_dimension() {
        let v = mock_embedding("test", 128);
        assert_eq!(v.len(), 128);

        let v = mock_embedding("test", 768);
        assert_eq!(v.len(), 768);
    }

    #[tokio::test]
    async fn test_embed_query_mock() {
        let config = EmbeddingConfig {
            provider: "mock".to_string(),
            model: "test".to_string(),
            api_key: None,
            dimension: Some(32),
        };
        let result = embed_query("hello world", &config).await.unwrap();
        assert_eq!(result.len(), 32);
    }

    #[tokio::test]
    async fn test_embed_query_unknown_provider() {
        let config = EmbeddingConfig {
            provider: "nonexistent".to_string(),
            model: "test".to_string(),
            api_key: None,
            dimension: None,
        };
        let result = embed_query("test", &config).await;
        assert!(result.is_err());
    }
}
