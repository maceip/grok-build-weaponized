//! Scoped native Nmap driver.
//!
//! The driver accepts typed options only, invokes a resolved local `nmap`
//! executable without a shell, parses its XML incrementally, and returns a
//! typed report suitable for direct JSON serialization. NSE scripts, raw arguments,
//! OS detection, spoofing, and other active options are intentionally outside
//! this low-risk observability interface.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const MAX_PORTS: usize = 1_024;
const MAX_XML_BYTES: usize = 32 * 1024 * 1024;

#[derive(
    Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ScanProfile {
    /// ARP/ICMP/TCP host discovery only; no port scan.
    HostDiscovery,
    /// Unprivileged TCP connect scan without service probing.
    #[default]
    TcpConnect,
    /// TCP connect scan plus Nmap's lightweight version probes.
    ServiceDiscovery,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct NmapScanRequest {
    #[schemars(description = "A single IP address or CIDR within the configured scope.")]
    pub target: String,
    #[serde(default)]
    pub profile: ScanProfile,
    /// Optional explicit TCP ports. Empty uses Nmap's default port set.
    #[serde(default)]
    #[schemars(description = "Optional TCP ports (maximum 1024). Empty uses Nmap defaults.")]
    pub ports: Vec<u16>,
    /// Process deadline in seconds. Defaults to 30 minutes and is capped at
    /// two hours.
    #[schemars(description = "Optional deadline in seconds (1 to 7200).")]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScanScope {
    allowed: Vec<NetworkTarget>,
}

impl ScanScope {
    pub fn new(targets: impl IntoIterator<Item = String>) -> Result<Self, NmapError> {
        let allowed = targets
            .into_iter()
            .map(|target| NetworkTarget::parse(&target))
            .collect::<Result<Vec<_>, _>>()?;
        if allowed.is_empty() {
            return Err(NmapError::EmptyScope);
        }
        Ok(Self { allowed })
    }

    pub fn allows(&self, requested: &str) -> Result<bool, NmapError> {
        let requested = NetworkTarget::parse(requested)?;
        Ok(self.allowed.iter().any(|scope| scope.contains(&requested)))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum NetworkTarget {
    Address(IpAddr),
    Cidr { network: IpAddr, prefix: u8 },
}

impl NetworkTarget {
    fn parse(value: &str) -> Result<Self, NmapError> {
        let value = value.trim();
        if let Some((address, prefix)) = value.split_once('/') {
            let address = address
                .parse::<IpAddr>()
                .map_err(|_| NmapError::InvalidTarget(value.to_owned()))?;
            let prefix = prefix
                .parse::<u8>()
                .map_err(|_| NmapError::InvalidTarget(value.to_owned()))?;
            let max_prefix = if address.is_ipv4() { 32 } else { 128 };
            if prefix > max_prefix {
                return Err(NmapError::InvalidTarget(value.to_owned()));
            }
            Ok(Self::Cidr {
                network: mask_address(address, prefix),
                prefix,
            })
        } else {
            value
                .parse::<IpAddr>()
                .map(Self::Address)
                .map_err(|_| NmapError::InvalidTarget(value.to_owned()))
        }
    }

    fn contains(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Address(scope), Self::Address(requested)) => scope == requested,
            (
                Self::Cidr {
                    network: scope,
                    prefix,
                },
                Self::Address(requested),
            ) => mask_address(*requested, *prefix) == *scope,
            (
                Self::Cidr {
                    network: scope,
                    prefix: scope_prefix,
                },
                Self::Cidr {
                    network: requested,
                    prefix: requested_prefix,
                },
            ) => {
                requested_prefix >= scope_prefix
                    && mask_address(*requested, *scope_prefix) == *scope
            }
            (Self::Address(_), Self::Cidr { .. }) => false,
        }
    }
}

fn mask_address(address: IpAddr, prefix: u8) -> IpAddr {
    match address {
        IpAddr::V4(address) => {
            let bits = u32::from(address);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            IpAddr::V4(Ipv4Addr::from(bits & mask))
        }
        IpAddr::V6(address) => {
            let bits = u128::from(address);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            IpAddr::V6(Ipv6Addr::from(bits & mask))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NmapScanReport {
    pub target: String,
    pub profile: ScanProfile,
    pub scanner_version: Option<String>,
    pub started_at: Option<String>,
    pub elapsed_seconds: Option<String>,
    pub hosts_up: u32,
    pub hosts_down: u32,
    pub hosts_total: u32,
    pub hosts: Vec<NmapHost>,
}

/// Bounded progress stream emitted while the XML parser is still consuming the
/// running Nmap process. Consumers can persist/query these deltas without
/// waiting for the complete subnet scan.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum NmapScanEvent {
    Started {
        target: String,
        profile: ScanProfile,
    },
    HostDiscovered {
        host_index: usize,
        host: NmapHost,
    },
    Finished {
        hosts_up: u32,
        hosts_down: u32,
        hosts_total: u32,
    },
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct NmapHost {
    pub status: Option<String>,
    pub status_reason: Option<String>,
    pub addresses: Vec<NmapAddress>,
    pub hostnames: Vec<String>,
    pub ports: Vec<NmapPort>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NmapAddress {
    pub address: String,
    pub address_type: Option<String>,
    pub vendor: Option<String>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct NmapPort {
    pub protocol: String,
    pub port: u16,
    pub state: Option<String>,
    pub state_reason: Option<String>,
    pub service: Option<NmapService>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct NmapService {
    pub name: Option<String>,
    pub product: Option<String>,
    pub version: Option<String>,
    pub extra_info: Option<String>,
    pub tunnel: Option<String>,
    pub os_type: Option<String>,
    pub method: Option<String>,
    pub confidence: Option<u8>,
}

impl NmapService {
    /// Compact service banner for model context and report generation.
    pub fn banner(&self) -> Option<String> {
        let parts = [
            self.name.as_deref(),
            self.product.as_deref(),
            self.version.as_deref(),
            self.extra_info.as_deref(),
        ]
        .into_iter()
        .flatten()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
        (!parts.is_empty()).then(|| parts.join(" "))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NmapError {
    #[error("operational scope is empty; configure at least one explicit IP or CIDR")]
    EmptyScope,
    #[error("invalid IP or CIDR target `{0}`")]
    InvalidTarget(String),
    #[error("target `{0}` is outside the configured operational scope")]
    OutOfScope(String),
    #[error("at most {MAX_PORTS} explicit ports are allowed")]
    TooManyPorts,
    #[error("nmap timeout must be between 1 and {} seconds", MAX_TIMEOUT.as_secs())]
    InvalidTimeout,
    #[error("could not find the local nmap executable: {0}")]
    MissingBinary(String),
    #[error("failed to start nmap: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("failed while reading nmap output: {0}")]
    Read(#[source] std::io::Error),
    #[error("nmap exceeded its {0:?} deadline")]
    TimedOut(Duration),
    #[error("nmap exited unsuccessfully ({code:?}): {stderr}")]
    Unsuccessful { code: Option<i32>, stderr: String },
    #[error("nmap XML exceeded the {MAX_XML_BYTES} byte safety limit")]
    XmlTooLarge,
    #[error("invalid nmap XML: {0}")]
    InvalidXml(String),
}

#[derive(Debug, Clone)]
pub struct NativeNmapDriver {
    binary: PathBuf,
}

impl NativeNmapDriver {
    pub fn discover() -> Result<Self, NmapError> {
        which::which("nmap")
            .map(|binary| Self { binary })
            .map_err(|error| NmapError::MissingBinary(error.to_string()))
    }

    pub fn with_binary(binary: PathBuf) -> Result<Self, NmapError> {
        if !binary.is_absolute() {
            return Err(NmapError::MissingBinary(
                "nmap path must be absolute".to_owned(),
            ));
        }
        Ok(Self { binary })
    }

    pub fn binary(&self) -> &Path {
        &self.binary
    }

    pub async fn scan(
        &self,
        scope: &ScanScope,
        request: NmapScanRequest,
    ) -> Result<NmapScanReport, NmapError> {
        self.scan_with_events(scope, request, None).await
    }

    pub async fn scan_with_events(
        &self,
        scope: &ScanScope,
        request: NmapScanRequest,
        events: Option<mpsc::Sender<NmapScanEvent>>,
    ) -> Result<NmapScanReport, NmapError> {
        if !scope.allows(&request.target)? {
            return Err(NmapError::OutOfScope(request.target.clone()));
        }
        if request.ports.len() > MAX_PORTS {
            return Err(NmapError::TooManyPorts);
        }
        let timeout = request
            .timeout_secs
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_TIMEOUT);
        if timeout.is_zero() || timeout > MAX_TIMEOUT {
            return Err(NmapError::InvalidTimeout);
        }

        let mut command = std::process::Command::new(&self.binary);
        command
            .arg("--noninteractive")
            .arg("-n")
            .arg("-oX")
            .arg("-")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match request.profile {
            ScanProfile::HostDiscovery => {
                command.arg("-sn");
            }
            ScanProfile::TcpConnect => {
                command.arg("-sT");
            }
            ScanProfile::ServiceDiscovery => {
                command.args(["-sT", "-sV", "--version-light"]);
            }
        }
        if !request.ports.is_empty() {
            let ports = request
                .ports
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|port| port.to_string())
                .collect::<Vec<_>>()
                .join(",");
            command.args(["-p", &ports]);
        }
        command.arg(&request.target);

        // Construct with std::process::Command (no shell), then hand ownership
        // to Tokio so long scans do not block the interactive runtime.
        let scan_target = request.target.clone();
        let scan_profile = request.profile;
        let scan =
            async move {
                let mut command = tokio::process::Command::from(command);
                command.kill_on_drop(true);
                let mut child = command.spawn().map_err(NmapError::Spawn)?;
                let stdout = child.stdout.take().ok_or_else(|| {
                    NmapError::Spawn(std::io::Error::other("stdout pipe missing"))
                })?;
                let stderr = child.stderr.take().ok_or_else(|| {
                    NmapError::Spawn(std::io::Error::other("stderr pipe missing"))
                })?;
                let stderr_task = tokio::spawn(read_bounded_stderr(stderr));
                if let Some(events) = events.as_ref() {
                    let _ = events
                        .send(NmapScanEvent::Started {
                            target: scan_target.clone(),
                            profile: scan_profile,
                        })
                        .await;
                }
                let report = parse_nmap_xml_async(
                    &scan_target,
                    scan_profile,
                    BufReader::new(stdout),
                    events.as_ref(),
                )
                .await?;
                let status = child.wait().await.map_err(NmapError::Read)?;
                let stderr = stderr_task
                    .await
                    .map_err(|error| {
                        NmapError::Read(std::io::Error::other(format!(
                            "stderr reader task failed: {error}"
                        )))
                    })?
                    .map_err(NmapError::Read)?;
                if !status.success() {
                    return Err(NmapError::Unsuccessful {
                        code: status.code(),
                        stderr,
                    });
                }
                if let Some(events) = events.as_ref() {
                    let _ = events
                        .send(NmapScanEvent::Finished {
                            hosts_up: report.hosts_up,
                            hosts_down: report.hosts_down,
                            hosts_total: report.hosts_total,
                        })
                        .await;
                }
                Ok(report)
            };
        tokio::time::timeout(timeout, scan)
            .await
            .map_err(|_| NmapError::TimedOut(timeout))?
    }
}

async fn read_bounded_stderr(mut stderr: impl AsyncRead + Unpin) -> std::io::Result<String> {
    const STDERR_PREVIEW_BYTES: usize = 8 * 1024;
    let mut preview = Vec::with_capacity(STDERR_PREVIEW_BYTES);
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        let read = stderr.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let remaining = STDERR_PREVIEW_BYTES.saturating_sub(preview.len());
        preview.extend_from_slice(&chunk[..read.min(remaining)]);
    }
    Ok(String::from_utf8_lossy(&preview).into_owned())
}

pub fn parse_nmap_xml(
    target: &str,
    profile: ScanProfile,
    xml: &[u8],
) -> Result<NmapScanReport, NmapError> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut report = empty_report(target, profile);
    let mut host: Option<NmapHost> = None;
    let mut port: Option<NmapPort> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) => {
                handle_start(&start, &mut report, &mut host, &mut port)?;
            }
            Ok(Event::Empty(start)) => {
                handle_start(&start, &mut report, &mut host, &mut port)?;
            }
            Ok(Event::End(end)) if end.name().as_ref() == b"port" => {
                if let (Some(host), Some(port)) = (host.as_mut(), port.take()) {
                    host.ports.push(port);
                }
            }
            Ok(Event::End(end)) if end.name().as_ref() == b"host" => {
                if let Some(host) = host.take() {
                    report.hosts.push(host);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(NmapError::InvalidXml(error.to_string())),
        }
    }
    Ok(report)
}

async fn parse_nmap_xml_async<R>(
    target: &str,
    profile: ScanProfile,
    source: R,
    events: Option<&mpsc::Sender<NmapScanEvent>>,
) -> Result<NmapScanReport, NmapError>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut reader = Reader::from_reader(source);
    reader.config_mut().trim_text(true);
    let mut report = empty_report(target, profile);
    let mut host: Option<NmapHost> = None;
    let mut port: Option<NmapPort> = None;
    let mut buffer = Vec::with_capacity(16 * 1024);

    loop {
        let mut progress = None;
        match reader.read_event_into_async(&mut buffer).await {
            Ok(Event::Start(start)) => {
                handle_start(&start, &mut report, &mut host, &mut port)?;
            }
            Ok(Event::Empty(start)) => {
                handle_start(&start, &mut report, &mut host, &mut port)?;
            }
            Ok(Event::End(end)) if end.name().as_ref() == b"port" => {
                if let (Some(host), Some(port)) = (host.as_mut(), port.take()) {
                    host.ports.push(port);
                }
            }
            Ok(Event::End(end)) if end.name().as_ref() == b"host" => {
                if let Some(host) = host.take() {
                    let host_index = report.hosts.len();
                    progress = Some(NmapScanEvent::HostDiscovered {
                        host_index,
                        host: host.clone(),
                    });
                    report.hosts.push(host);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(NmapError::InvalidXml(error.to_string())),
        }
        if reader.buffer_position() > MAX_XML_BYTES as u64 {
            return Err(NmapError::XmlTooLarge);
        }
        buffer.clear();
        if let (Some(events), Some(progress)) = (events, progress) {
            let _ = events.send(progress).await;
        }
    }
    Ok(report)
}

fn empty_report(target: &str, profile: ScanProfile) -> NmapScanReport {
    NmapScanReport {
        target: target.to_owned(),
        profile,
        scanner_version: None,
        started_at: None,
        elapsed_seconds: None,
        hosts_up: 0,
        hosts_down: 0,
        hosts_total: 0,
        hosts: Vec::new(),
    }
}

fn handle_start(
    start: &BytesStart<'_>,
    report: &mut NmapScanReport,
    host: &mut Option<NmapHost>,
    port: &mut Option<NmapPort>,
) -> Result<(), NmapError> {
    match start.name().as_ref() {
        b"nmaprun" => {
            report.scanner_version = attr(start, b"version")?;
            report.started_at = attr(start, b"startstr")?;
        }
        b"host" => *host = Some(NmapHost::default()),
        b"status" => {
            if let Some(host) = host.as_mut() {
                host.status = attr(start, b"state")?;
                host.status_reason = attr(start, b"reason")?;
            }
        }
        b"address" => {
            if let (Some(host), Some(address)) = (host.as_mut(), attr(start, b"addr")?) {
                host.addresses.push(NmapAddress {
                    address,
                    address_type: attr(start, b"addrtype")?,
                    vendor: attr(start, b"vendor")?,
                });
            }
        }
        b"hostname" => {
            if let (Some(host), Some(name)) = (host.as_mut(), attr(start, b"name")?) {
                host.hostnames.push(name);
            }
        }
        b"port" => {
            let protocol = attr(start, b"protocol")?.unwrap_or_default();
            let port_id = attr(start, b"portid")?
                .ok_or_else(|| NmapError::InvalidXml("port missing portid".to_owned()))?
                .parse::<u16>()
                .map_err(|error| NmapError::InvalidXml(error.to_string()))?;
            *port = Some(NmapPort {
                protocol,
                port: port_id,
                ..Default::default()
            });
        }
        b"state" => {
            if let Some(port) = port.as_mut() {
                port.state = attr(start, b"state")?;
                port.state_reason = attr(start, b"reason")?;
            }
        }
        b"service" => {
            if let Some(port) = port.as_mut() {
                port.service = Some(NmapService {
                    name: attr(start, b"name")?,
                    product: attr(start, b"product")?,
                    version: attr(start, b"version")?,
                    extra_info: attr(start, b"extrainfo")?,
                    tunnel: attr(start, b"tunnel")?,
                    os_type: attr(start, b"ostype")?,
                    method: attr(start, b"method")?,
                    confidence: attr(start, b"conf")?.and_then(|value| value.parse::<u8>().ok()),
                });
            }
        }
        b"finished" => report.elapsed_seconds = attr(start, b"elapsed")?,
        b"hosts" => {
            report.hosts_up = attr_u32(start, b"up")?;
            report.hosts_down = attr_u32(start, b"down")?;
            report.hosts_total = attr_u32(start, b"total")?;
        }
        _ => {}
    }
    Ok(())
}

fn attr(start: &BytesStart<'_>, name: &[u8]) -> Result<Option<String>, NmapError> {
    for attribute in start.attributes() {
        let attribute = attribute.map_err(|error| NmapError::InvalidXml(error.to_string()))?;
        if attribute.key.as_ref() == name {
            return attribute
                .unescape_value()
                .map(|value| Some(value.into_owned()))
                .map_err(|error| NmapError::InvalidXml(error.to_string()));
        }
    }
    Ok(None)
}

fn attr_u32(start: &BytesStart<'_>, name: &[u8]) -> Result<u32, NmapError> {
    attr(start, name)?
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|error| NmapError::InvalidXml(error.to_string()))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &[u8] = br#"<?xml version="1.0"?>
<nmaprun scanner="nmap" startstr="Wed Jul 29" version="7.99">
  <host>
    <status state="up" reason="syn-ack"/>
    <address addr="10.10.4.12" addrtype="ipv4"/>
    <hostnames><hostname name="fileserver.local" type="PTR"/></hostnames>
    <ports>
      <port protocol="tcp" portid="22">
        <state state="open" reason="syn-ack"/>
        <service name="ssh" product="OpenSSH" version="9.8" extrainfo="protocol 2.0" method="probed" conf="10"/>
      </port>
      <port protocol="tcp" portid="443">
        <state state="open" reason="syn-ack"/>
        <service name="https" tunnel="ssl" product="nginx" version="1.24.0"/>
      </port>
    </ports>
  </host>
  <runstats>
    <finished elapsed="2.14"/>
    <hosts up="1" down="0" total="1"/>
  </runstats>
</nmaprun>"#;

    #[test]
    fn scope_contains_only_equal_or_narrower_targets() {
        let scope = ScanScope::new(["10.10.4.0/24".to_owned()]).unwrap();
        assert!(scope.allows("10.10.4.12").unwrap());
        assert!(scope.allows("10.10.4.128/25").unwrap());
        assert!(!scope.allows("10.10.5.1").unwrap());
        assert!(!scope.allows("10.10.0.0/16").unwrap());
    }

    #[test]
    fn parses_typed_hosts_ports_and_banners() {
        let report = parse_nmap_xml("10.10.4.0/24", ScanProfile::ServiceDiscovery, SAMPLE).unwrap();

        assert_eq!(report.scanner_version.as_deref(), Some("7.99"));
        assert_eq!(report.hosts_up, 1);
        assert_eq!(report.hosts.len(), 1);
        assert_eq!(report.hosts[0].hostnames, vec!["fileserver.local"]);
        assert_eq!(report.hosts[0].ports[0].port, 22);
        assert_eq!(
            report.hosts[0].ports[0]
                .service
                .as_ref()
                .and_then(NmapService::banner)
                .as_deref(),
            Some("ssh OpenSSH 9.8 protocol 2.0")
        );
    }

    #[tokio::test]
    async fn incremental_parser_emits_completed_hosts_before_final_report() {
        let (events_tx, mut events_rx) = mpsc::channel(8);
        let report = parse_nmap_xml_async(
            "10.10.4.0/24",
            ScanProfile::ServiceDiscovery,
            BufReader::new(SAMPLE),
            Some(&events_tx),
        )
        .await
        .unwrap();
        let event = events_rx.recv().await.expect("host progress event");
        let NmapScanEvent::HostDiscovered { host_index, host } = event else {
            panic!("expected host progress event");
        };
        assert_eq!(host_index, 0);
        assert_eq!(host.ports.len(), 2);
        assert_eq!(report.hosts, vec![host]);
    }

    #[test]
    fn driver_resolves_installed_nmap() {
        let driver = NativeNmapDriver::discover().unwrap();
        assert!(driver.binary().is_absolute());
    }

    #[tokio::test]
    #[ignore = "requires the local nmap binary"]
    async fn live_loopback_scan_stays_inside_scope_and_parses_xml() {
        let driver = NativeNmapDriver::discover().unwrap();
        let scope = ScanScope::new(["127.0.0.1".to_owned()]).unwrap();
        let report = driver
            .scan(
                &scope,
                NmapScanRequest {
                    target: "127.0.0.1".to_owned(),
                    profile: ScanProfile::TcpConnect,
                    ports: vec![1],
                    timeout_secs: Some(15),
                },
            )
            .await
            .unwrap();

        assert_eq!(report.target, "127.0.0.1");
        assert_eq!(report.hosts_total, 1);
    }
}
