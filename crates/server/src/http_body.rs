//! Bounded reads of provider response bodies.

/// A response body could not be read within its bound.
#[derive(Debug)]
pub(crate) enum BodyError {
    TooLarge,
    Transport(reqwest::Error),
}

/// Reads at most `limit` bytes, failing instead of buffering an oversized body.
pub(crate) async fn read_bounded(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, BodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(BodyError::TooLarge);
    }
    let mut body = Vec::with_capacity(limit.min(16_384));
    while let Some(chunk) = response.chunk().await.map_err(BodyError::Transport)? {
        if body.len() + chunk.len() > limit {
            return Err(BodyError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
