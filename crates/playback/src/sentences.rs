//! Stateless segmentation for sentence-mode speech synthesis.
//!
//! The splitter recognizes terminal punctuation followed by standard Unicode
//! whitespace, then merges short fragments. It is a mechanical transport helper,
//! not a linguistic sentence parser or application-specific text normalizer.

/// Split trimmed text after `.`, `!` or `?` followed by whitespace, keeping punctuation.
///
/// Return no segments for empty text. Merge a short preceding segment with the
/// next one, then merge a short final segment back into its predecessor. Merges
/// insert one space; unmerged interior text is otherwise preserved.
///
/// `min_chars` counts Unicode scalar values, not UTF-8 bytes or grapheme clusters.
/// Zero/negative minima disable short-fragment merging. Whitespace follows Rust
/// standard Unicode rules; U+001C–U+001F are ordinary characters.
pub fn split_sentences(text: &str, min_chars: i64) -> Vec<String> {
    let text = text.trim();
    if text.is_empty() { return Vec::new(); }
    let mut raw = Vec::new();
    let mut start = 0;
    let mut punctuation = false;
    let mut boundary = false;
    for (i, c) in text.char_indices() {
        if c.is_whitespace() {
            if punctuation && !boundary {
                raw.push(text[start..i].trim().to_owned());
                boundary = true;
            }
        } else {
            if boundary { start = i; boundary = false; }
            punctuation = matches!(c,'.'|'!'|'?');
        }
    }
    if !boundary { raw.push(text[start..].to_owned()); }
    if raw.len() <= 1 { return vec![text.to_owned()]; }
    let mut merged: Vec<String> = Vec::new();
    for piece in raw {
        if let Some(last) = merged.last_mut() && (last.chars().count() as i64) < min_chars {
            last.push(' '); last.push_str(&piece);
        } else { merged.push(piece); }
    }
    if merged.len() >= 2 && (merged.last().unwrap().chars().count() as i64) < min_chars {
        let last = merged.pop().unwrap();
        merged.last_mut().unwrap().push(' '); merged.last_mut().unwrap().push_str(&last);
    }
    merged
}
