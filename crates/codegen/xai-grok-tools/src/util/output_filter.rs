//! Streaming structural analysis for command output.
//!
//! The terminal keeps a bounded first/last preview in memory and writes the
//! complete stream to disk. This analyzer runs before the in-memory preview is
//! truncated, so high-value lines from the omitted middle can still be
//! represented in the model-visible result.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

const MAX_PENDING_LINE_BYTES: usize = 64 * 1024;
const MAX_FINDINGS: usize = 16;
const MAX_FINDING_CHARS: usize = 240;
const TRUNCATION_MARKER: &str = "\n\n... (output truncated) ...\n\n";

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct OutputAnalysis {
    pending_line: Vec<u8>,
    line_count: usize,
    error_line_count: usize,
    warning_line_count: usize,
    http_status_count: usize,
    open_port_count: usize,
    vulnerable_banner_count: usize,
    dropped_finding_count: usize,
    findings: Vec<String>,
    finding_keys: HashSet<String>,
    oversized_line_count: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FilteredOutput {
    pub text: String,
    pub truncated: bool,
    pub structure: OutputStructure,
}

/// Machine-readable structural facts extracted from the complete stream.
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

/// Atomic model-facing result. The preview may be shortened, while the
/// complete stream remains available through `artifact` and `cursor`.
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

/// A typed terminal result embedded in a tool adapter's surrounding prompt
/// text (for example, the `exit: 0\n` prefix emitted by the bash adapter).
///
/// Keeping the wrapper separate lets downstream context admission compact the
/// preview while reserializing the complete envelope atomically. Generic
/// middle truncation must never cut the JSON object itself.
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

    /// Serialize a valid envelope after bounding only its unstructured
    /// preview. Finding detail can be omitted after the preview reaches zero;
    /// all structural counts, status, artifact reference, and cursor remain.
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

impl OutputAnalysis {
    /// Ingest a raw stdout/stderr chunk. Lines may be split across chunks.
    pub fn push(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.pending_line);
                self.analyze_complete_line(&line);
                continue;
            }

            if self.pending_line.len() < MAX_PENDING_LINE_BYTES {
                self.pending_line.push(byte);
            } else if self.pending_line.len() == MAX_PENDING_LINE_BYTES {
                // Keep memory bounded for minified JSON or binary-ish output.
                // The first 64 KiB is still analyzed when the line terminates.
                self.oversized_line_count += 1;
                self.pending_line.push(byte);
            }
        }
    }

    /// Render a structural summary and bounded first/last preview.
    ///
    /// `char_budget` bounds the final model-visible string, including the
    /// summary and truncation marker. The terminal's complete output file is
    /// unaffected.
    pub fn render(
        &self,
        raw_preview: &str,
        raw_was_truncated: bool,
        char_budget: usize,
    ) -> FilteredOutput {
        if char_budget == 0 {
            return FilteredOutput {
                text: String::new(),
                truncated: !raw_preview.is_empty() || raw_was_truncated,
                structure: self.structure(),
            };
        }

        let mut finished = self.clone();
        finished.finish();
        let structure = finished.structure();
        let summary = finished.summary(raw_was_truncated);

        let mut body_budget = char_budget;
        if let Some(summary) = summary.as_deref() {
            body_budget = body_budget.saturating_sub(summary.chars().count() + 2);
        }

        let (body, body_truncated) = truncate_middle(raw_preview, body_budget);
        let mut text = match summary {
            Some(summary) if body.is_empty() => summary,
            Some(summary) => format!("{summary}\n\n{body}"),
            None => body,
        };

        let mut final_truncated = raw_was_truncated || body_truncated;
        if text.chars().count() > char_budget {
            let (bounded, was_truncated) = truncate_middle(&text, char_budget);
            text = bounded;
            final_truncated |= was_truncated;
        }

        FilteredOutput {
            text,
            truncated: final_truncated,
            structure,
        }
    }

    fn structure(&self) -> OutputStructure {
        OutputStructure {
            line_count: self.line_count,
            error_line_count: self.error_line_count,
            warning_line_count: self.warning_line_count,
            http_status_count: self.http_status_count,
            open_port_count: self.open_port_count,
            vulnerable_banner_count: self.vulnerable_banner_count,
            match_count: self
                .http_status_count
                .saturating_add(self.open_port_count)
                .saturating_add(self.vulnerable_banner_count),
            oversized_line_count: self.oversized_line_count,
            omitted_finding_count: self.dropped_finding_count,
            findings: self.findings.clone(),
        }
    }

    fn finish(&mut self) {
        if !self.pending_line.is_empty() {
            let line = std::mem::take(&mut self.pending_line);
            self.analyze_complete_line(&line);
        }
    }

    fn analyze_complete_line(&mut self, bytes: &[u8]) {
        self.line_count += 1;
        let line = String::from_utf8_lossy(bytes);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }

        let lowercase = trimmed.to_ascii_lowercase();
        let is_error = contains_any(
            &lowercase,
            &["error", "fatal", "failed", "panic", "traceback"],
        );
        let is_warning = contains_any(&lowercase, &["warning", "warn:"]);
        let is_http_status = has_http_status(trimmed);
        let is_open_port = has_open_port(trimmed, &lowercase);
        let is_vulnerable_banner = contains_any(
            &lowercase,
            &["vulnerable", "cve-", "server:", "banner:", "service banner"],
        );

        self.error_line_count += usize::from(is_error);
        self.warning_line_count += usize::from(is_warning);
        self.http_status_count += usize::from(is_http_status);
        self.open_port_count += usize::from(is_open_port);
        self.vulnerable_banner_count += usize::from(is_vulnerable_banner);

        if is_http_status || is_open_port || is_vulnerable_banner {
            self.add_finding(trimmed);
        }
    }

    fn add_finding(&mut self, line: &str) {
        let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
        if !self.finding_keys.insert(normalized.clone()) {
            return;
        }
        if self.findings.len() >= MAX_FINDINGS {
            self.dropped_finding_count += 1;
            return;
        }
        self.findings
            .push(truncate_chars(&normalized, MAX_FINDING_CHARS));
    }

    fn summary(&self, raw_was_truncated: bool) -> Option<String> {
        let match_count =
            self.http_status_count + self.open_port_count + self.vulnerable_banner_count;
        if !raw_was_truncated
            && match_count == 0
            && self.error_line_count == 0
            && self.warning_line_count == 0
        {
            return None;
        }

        let mut summary = format!(
            "[output structure: lines={}, errors={}, warnings={}, matches={}, \
             http_statuses={}, open_ports={}, vulnerable_banners={}",
            self.line_count,
            self.error_line_count,
            self.warning_line_count,
            match_count,
            self.http_status_count,
            self.open_port_count,
            self.vulnerable_banner_count,
        );
        if self.oversized_line_count > 0 {
            summary.push_str(&format!(", oversized_lines={}", self.oversized_line_count));
        }
        summary.push(']');

        if !self.findings.is_empty() {
            summary.push_str("\nKey findings retained from the full stream:");
            for finding in &self.findings {
                summary.push_str("\n- ");
                summary.push_str(finding);
            }
            if self.dropped_finding_count > 0 {
                summary.push_str(&format!(
                    "\n- ... {} additional unique findings omitted",
                    self.dropped_finding_count
                ));
            }
        }
        Some(summary)
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn has_http_status(line: &str) -> bool {
    let uppercase = line.to_ascii_uppercase();
    if !(uppercase.contains("HTTP/") || uppercase.contains("HTTP ")) {
        return false;
    }

    uppercase
        .split(|ch: char| !ch.is_ascii_digit())
        .any(|part| {
            part.len() == 3
                && part
                    .parse::<u16>()
                    .is_ok_and(|status| (200..400).contains(&status))
        })
}

fn has_open_port(line: &str, lowercase: &str) -> bool {
    if lowercase.contains("discovered open port") {
        return true;
    }

    let mut fields = line.split_whitespace();
    let Some(port_proto) = fields.next() else {
        return false;
    };
    let Some(state) = fields.next() else {
        return false;
    };
    let Some((port, protocol)) = port_proto.split_once('/') else {
        return false;
    };

    port.parse::<u16>().is_ok()
        && matches!(protocol.to_ascii_lowercase().as_str(), "tcp" | "udp")
        && state.to_ascii_lowercase().starts_with("open")
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
    fn retains_findings_split_across_stream_chunks() {
        let mut analysis = OutputAnalysis::default();
        analysis.push(b"noise\nHTTP/1.1 20");
        analysis.push(b"0 OK\n22/tcp open ssh OpenSSH_9.8\n");
        analysis.push(b"Server: nginx/1.24.0\n");

        let rendered = analysis.render("noise\ntail", true, 2_000);

        assert!(rendered.text.contains("lines=4"));
        assert!(rendered.text.contains("http_statuses=1"));
        assert!(rendered.text.contains("open_ports=1"));
        assert!(rendered.text.contains("HTTP/1.1 200 OK"));
        assert!(rendered.text.contains("22/tcp open ssh OpenSSH_9.8"));
        assert!(rendered.text.contains("Server: nginx/1.24.0"));
    }

    #[test]
    fn ordinary_short_output_is_unchanged() {
        let mut analysis = OutputAnalysis::default();
        analysis.push(b"hello\nworld\n");

        let rendered = analysis.render("hello\nworld\n", false, 2_000);

        assert_eq!(rendered.text, "hello\nworld\n");
        assert!(!rendered.truncated);
    }

    #[test]
    fn final_render_never_exceeds_budget() {
        let mut analysis = OutputAnalysis::default();
        analysis.push(b"HTTP/1.1 302 Found\n");
        let raw = "x".repeat(10_000);

        let rendered = analysis.render(&raw, true, 200);

        assert!(rendered.truncated);
        assert!(rendered.text.chars().count() <= 200);
    }

    #[test]
    fn errors_are_counted_without_being_promoted_to_findings() {
        let mut analysis = OutputAnalysis::default();
        analysis.push(b"warning: retrying\nfatal error: build failed\n");

        let rendered = analysis.render("tail", true, 500);

        assert!(rendered.text.contains("errors=1"));
        assert!(rendered.text.contains("warnings=1"));
        assert!(rendered.text.contains("matches=0"));
        assert!(!rendered.text.contains("Key findings"));
    }

    #[test]
    fn wrapped_envelope_compaction_never_splits_json() {
        let wrapped = WrappedToolResultEnvelope {
            prefix: "exit: 0\n".to_string(),
            envelope: ToolResultEnvelope {
                status: "completed".to_string(),
                exit_code: Some(0),
                signal: None,
                structure: OutputStructure {
                    line_count: 12,
                    findings: vec!["22/tcp open ssh".to_string()],
                    ..OutputStructure::default()
                },
                findings: vec!["22/tcp open ssh".to_string()],
                preview: "x".repeat(10_000),
                artifact: OutputArtifactRef {
                    path: "/tmp/output.jsonl".to_string(),
                    total_bytes: 10_000,
                },
                cursor: OutputCursor {
                    next_byte: 10_000,
                    complete: true,
                },
                truncated: false,
            },
            suffix: "\ntrailer".to_string(),
        };

        let rendered = wrapped.render(32, false);
        let reparsed = WrappedToolResultEnvelope::parse(&rendered).unwrap();
        assert!(reparsed.envelope.truncated);
        assert!(reparsed.envelope.preview.chars().count() <= 32);
        assert_eq!(reparsed.envelope.structure.line_count, 12);
        assert_eq!(reparsed.envelope.artifact.path, "/tmp/output.jsonl");
        assert_eq!(reparsed.envelope.cursor.next_byte, 10_000);
        assert_eq!(reparsed.suffix, "\ntrailer");
    }
}
