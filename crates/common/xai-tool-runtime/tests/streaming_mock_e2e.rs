//! Production-path streaming test driven by realistic mock command output.

use std::pin::Pin;

use futures::stream::{self, Stream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use xai_tool_runtime::{
    PartialResultPayload, StreamingSpec, Tool, ToolCallContext, ToolCapabilities, ToolId,
    ToolOutput, ToolProgress, ToolStream, ToolStreamItem, stream_chunk, with_progress,
};
use xai_tool_types::ToolDescription;

const FRAME_CAP: u32 = 31;
const MOCK_CHUNK_SIZES: &[usize] = &[7, 41, 3, 19, 64, 11, 5, 73, 2, 29, 47, 13];

#[derive(Debug, Deserialize, JsonSchema)]
struct MockScanArgs {
    target: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct MockScanResult {
    target: String,
    exit_code: i32,
    open_ports: Vec<u16>,
}

impl ToolOutput for MockScanResult {}

/// Minimal model of the execution supervisor's monotonically-counted,
/// bounded stdout tail. It deliberately exposes snapshots rather than
/// pre-splitting model frames; `stream_chunk` owns that policy.
struct MockStdoutTail {
    bytes: Vec<u8>,
    capacity: usize,
    total: u64,
}

impl MockStdoutTail {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: Vec::new(),
            capacity,
            total: 0,
        }
    }

    fn write(&mut self, chunk: &[u8]) {
        self.total = self
            .total
            .checked_add(chunk.len() as u64)
            .expect("mock output length fits u64");
        self.bytes.extend_from_slice(chunk);
        if self.bytes.len() > self.capacity {
            let dropped = self.bytes.len() - self.capacity;
            self.bytes.drain(..dropped);
        }
    }
}

struct MockNmapTool;

impl Tool for MockNmapTool {
    type Args = MockScanArgs;
    type Output = MockScanResult;

    fn id(&self) -> ToolId {
        ToolId::new("mock_nmap_scan").expect("static tool ID is valid")
    }

    fn description(&self, _ctx: &xai_tool_runtime::ListToolsContext) -> ToolDescription {
        ToolDescription::new(
            "mock_nmap_scan",
            "Stream a deterministic mock service-discovery scan",
        )
    }

    fn capabilities(&self) -> ToolCapabilities {
        ToolCapabilities {
            streaming: Some(StreamingSpec {
                subkind: "command_stdout_delta".to_owned(),
                max_delta_bytes: Some(FRAME_CAP),
            }),
            ..Default::default()
        }
    }

    async fn execute(&self, _ctx: ToolCallContext, args: Self::Args) -> ToolStream<Self::Output> {
        let output = mock_scan_output(&args.target);
        let spec = self
            .capabilities()
            .streaming
            .expect("mock tool declares streaming");
        let mut stdout = MockStdoutTail::new(2_048);
        let mut cursor = 0;
        let mut progress = Vec::new();
        let mut offset = 0;

        for chunk_size in MOCK_CHUNK_SIZES.iter().copied().cycle() {
            if offset == output.len() {
                break;
            }
            let end = (offset + chunk_size).min(output.len());
            stdout.write(&output.as_bytes()[offset..end]);
            offset = end;

            // A real event loop gets one opportunity to publish per wakeup.
            // Large writes therefore create a backlog that subsequent polls
            // and the final drain must recover without duplication or loss.
            if let Some(frame) =
                stream_chunk(&spec, &stdout.bytes, stdout.total, &mut cursor, false)
            {
                progress.push(frame);
            }
        }

        while cursor < stdout.total {
            let frame = stream_chunk(&spec, &stdout.bytes, stdout.total, &mut cursor, false)
                .expect("a retained stdout backlog must make progress");
            progress.push(frame);
        }

        let result = MockScanResult {
            target: args.target,
            exit_code: 0,
            open_ports: parse_open_tcp_ports(&output),
        };
        let progress: Pin<Box<dyn Stream<Item = ToolProgress> + Send>> =
            Box::pin(stream::iter(progress));
        with_progress(progress, async move { Ok(result) })
    }
}

#[tokio::test]
async fn mock_scan_survives_stdout_buffering_wire_encoding_and_terminal_summary() {
    let target = "10.10.4.23";
    let expected_output = mock_scan_output(target);
    let mut items: Vec<_> = MockNmapTool
        .execute(
            ToolCallContext::default(),
            MockScanArgs {
                target: target.to_owned(),
            },
        )
        .await
        .collect()
        .await;

    let terminal = items.pop().expect("mock tool emits a terminal result");
    let mut model_visible_output = String::new();
    let mut previous_total = 0;

    for item in items {
        let ToolStreamItem::Progress(progress) = item else {
            panic!("terminal item appeared before the end of the stream");
        };

        // Exercise the same serde boundary used by process/worker transport,
        // then decode the typed payload as the model-facing consumer does.
        let wire = serde_json::to_vec(&progress).expect("progress serializes to wire JSON");
        let decoded: ToolProgress =
            serde_json::from_slice(&wire).expect("wire JSON decodes as tool progress");
        let ToolProgress::Custom { subkind, payload } = decoded else {
            panic!("mock stdout must use a custom partial-result envelope");
        };
        assert_eq!(subkind, "command_stdout_delta");

        let payload: PartialResultPayload =
            serde_json::from_value(payload).expect("partial-result payload is valid");
        assert!(
            payload.total_bytes >= previous_total,
            "producer byte totals are monotonic"
        );
        assert!(
            payload.delta.len() <= FRAME_CAP as usize,
            "ordinary ASCII frames obey the configured cap"
        );
        assert!(!payload.gap, "the retained mock tail is large enough");
        assert!(
            !payload.truncated,
            "the mock command did not hit a hard cap"
        );

        previous_total = payload.total_bytes;
        model_visible_output.push_str(&payload.delta);
    }

    assert_eq!(
        model_visible_output, expected_output,
        "stdout reconstructed from model-facing deltas is byte-for-byte lossless"
    );
    assert_eq!(
        previous_total,
        expected_output.len() as u64,
        "the final progress envelope reports the complete producer byte count"
    );
    assert!(model_visible_output.contains("OpenSSH 9.6p1"));
    assert!(model_visible_output.contains("302 Found"));

    match terminal {
        ToolStreamItem::Terminal(Ok(result)) => {
            assert_eq!(
                result,
                MockScanResult {
                    target: target.to_owned(),
                    exit_code: 0,
                    open_ports: vec![22, 80, 443],
                }
            );
        }
        other => panic!("expected successful terminal scan result, got {other:?}"),
    }
}

fn mock_scan_output(target: &str) -> String {
    format!(
        "Starting Nmap 7.95 ( https://nmap.org )\n\
         Nmap scan report for {target}\n\
         Host is up (0.012s latency).\n\
         Not shown: 996 filtered tcp ports (no-response)\n\
         PORT     STATE  SERVICE   VERSION\n\
         22/tcp   open   ssh       OpenSSH 9.6p1 Ubuntu 3ubuntu13\n\
         80/tcp   open   http      nginx 1.24.0\n\
         |_http-title: 302 Found\n\
         443/tcp  open   ssl/http  nginx 1.24.0\n\
         3306/tcp closed mysql\n\
         Service detection performed.\n\
         Nmap done: 1 IP address (1 host up) scanned in 8.42 seconds\n"
    )
}

fn parse_open_tcp_ports(output: &str) -> Vec<u16> {
    output
        .lines()
        .filter_map(|line| {
            let (port, remainder) = line.split_once("/tcp")?;
            (remainder.split_whitespace().next() == Some("open"))
                .then(|| port.trim().parse().ok())
                .flatten()
        })
        .collect()
}
