use serde::{Deserialize, Serialize};

const TRUNCATION_MARKER: &str = "\n\n... (output truncated) ...\n\n";

/// Machine-readable structural facts extracted from a complete process stream.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OutputStructure {
    pub line_count: usize,
    pub error_line_count: usize,
    pub warning_line_count: usize,
    pub http_status_count: usize,
    pub open_port_count: usize,
    pub vulnerable_banner_count: usize,
    pub match_count: usize,
    pub oversized_line_count: usize,
    pub omitted_finding_count: usize,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OutputArtifactRef {
    pub path: String,
    pub total_bytes: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OutputCursor {
    pub next_byte: usize,
    pub complete: bool,
}

/// Atomic model-facing process result. The preview and duplicated finding
/// detail may be shortened, while the complete stream remains addressable.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ToolResultEnvelope {
    pub status: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub structure: OutputStructure,
    pub findings: Vec<String>,
    pub preview: String,
    pub artifact: OutputArtifactRef,
    pub cursor: OutputCursor,
    pub truncated: bool,
}

impl ToolResultEnvelope {
    pub fn to_prompt_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            "{\"status\":\"serialization_error\",\"truncated\":true}".to_string()
        })
    }
}

/// A typed result embedded in a tool adapter's surrounding prompt text.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WrappedToolResultEnvelope {
    pub prefix: String,
    pub envelope: ToolResultEnvelope,
    pub suffix: String,
}

impl WrappedToolResultEnvelope {
    pub fn parse(input: &str) -> Option<Self> {
        for (start, _) in input.match_indices('{') {
            let mut stream = serde_json::Deserializer::from_str(&input[start..])
                .into_iter::<ToolResultEnvelope>();
            let Some(Ok(envelope)) = stream.next() else {
                continue;
            };
            let end = start.checked_add(stream.byte_offset())?;
            return Some(Self {
                prefix: input[..start].to_owned(),
                envelope,
                suffix: input[end..].to_owned(),
            });
        }
        None
    }

    /// Reserialize a valid envelope after bounding only its unstructured
    /// preview. Structural counts, status, artifact reference, and cursor are
    /// always retained.
    pub fn render(&self, preview_char_budget: usize, retain_finding_detail: bool) -> String {
        let mut envelope = self.envelope.clone();
        let (preview, preview_truncated) = truncate_middle(&envelope.preview, preview_char_budget);
        envelope.preview = preview;
        envelope.truncated |= preview_truncated;
        if !retain_finding_detail {
            let omitted = envelope
                .findings
                .len()
                .max(envelope.structure.findings.len());
            envelope.findings.clear();
            envelope.structure.findings.clear();
            envelope.structure.omitted_finding_count = envelope
                .structure
                .omitted_finding_count
                .saturating_add(omitted);
        }
        format!(
            "{}{}{}",
            self.prefix,
            envelope.to_prompt_json(),
            self.suffix
        )
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let end = value
        .char_indices()
        .nth(max_chars.saturating_sub(1))
        .map(|(index, _)| index)
        .unwrap_or(value.len());
    format!("{}…", &value[..end])
}

fn truncate_middle(value: &str, max_chars: usize) -> (String, bool) {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return (value.to_owned(), false);
    }
    if max_chars == 0 {
        return (String::new(), true);
    }

    let marker_chars = TRUNCATION_MARKER.chars().count();
    if max_chars <= marker_chars {
        return (truncate_chars(TRUNCATION_MARKER.trim(), max_chars), true);
    }

    let content_budget = max_chars - marker_chars;
    let front_chars = content_budget / 2;
    let back_chars = content_budget - front_chars;
    let front_end = value
        .char_indices()
        .nth(front_chars)
        .map(|(index, _)| index)
        .unwrap_or(value.len());
    let back_start = value
        .char_indices()
        .nth(char_count - back_chars)
        .map(|(index, _)| index)
        .unwrap_or(value.len());

    (
        format!(
            "{}{}{}",
            &value[..front_end],
            TRUNCATION_MARKER,
            &value[back_start..]
        ),
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapped_envelope_compaction_preserves_mandatory_fields() {
        let wrapped = WrappedToolResultEnvelope {
            prefix: "exit: 0\n".to_string(),
            envelope: ToolResultEnvelope {
                status: "completed".to_string(),
                exit_code: Some(0),
                signal: None,
                structure: OutputStructure {
                    line_count: 100,
                    findings: vec!["22/tcp open ssh".to_string()],
                    ..OutputStructure::default()
                },
                findings: vec!["22/tcp open ssh".to_string()],
                preview: "line\n".repeat(100),
                artifact: OutputArtifactRef {
                    path: "/tmp/output".to_string(),
                    total_bytes: 500,
                },
                cursor: OutputCursor {
                    next_byte: 500,
                    complete: true,
                },
                truncated: false,
            },
            suffix: String::new(),
        };

        let rendered = wrapped.render(12, false);
        let reparsed = WrappedToolResultEnvelope::parse(&rendered).expect("valid envelope");
        assert_eq!(reparsed.envelope.status, "completed");
        assert_eq!(reparsed.envelope.exit_code, Some(0));
        assert_eq!(reparsed.envelope.structure.line_count, 100);
        assert_eq!(reparsed.envelope.artifact.path, "/tmp/output");
        assert_eq!(reparsed.envelope.cursor.next_byte, 500);
        assert!(reparsed.envelope.findings.is_empty());
        assert!(reparsed.envelope.truncated);
    }
}
