use std::path::Path;

use quick_xml::Reader;
use quick_xml::events::Event;
use serde::{Deserialize, Serialize};

use crate::supervisor::NativeExecutionError;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanProfile {
    HostDiscovery,
    TcpConnect,
    #[default]
    ServiceDiscovery,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NmapRequest {
    pub target: String,
    pub allowed_targets: Vec<String>,
    #[serde(default)]
    pub profile: ScanProfile,
    #[serde(default)]
    pub ports: Vec<u16>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    20 * 60 * 1_000
}

impl NmapRequest {
    pub(crate) fn validate(&self) -> Result<(), NativeExecutionError> {
        if self.target.trim().is_empty() || self.allowed_targets.is_empty() {
            return Err(NativeExecutionError::InvalidRequest(
                "Nmap requires a target and non-empty allowed_targets".to_owned(),
            ));
        }
        if !self
            .allowed_targets
            .iter()
            .any(|allowed| target_is_allowed(&self.target, allowed))
        {
            return Err(NativeExecutionError::OutOfScope(self.target.clone()));
        }
        if self.ports.len() > 1024 {
            return Err(NativeExecutionError::InvalidRequest(
                "Nmap accepts at most 1024 explicit ports".to_owned(),
            ));
        }
        if self.timeout_ms == 0 || self.timeout_ms > 24 * 60 * 60 * 1_000 {
            return Err(NativeExecutionError::InvalidRequest(
                "Nmap timeout must be between 1 ms and 24 hours".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn arguments(&self, xml_path: &Path) -> Vec<String> {
        let mut args = vec!["-oX".to_owned(), xml_path.to_string_lossy().into_owned()];
        match self.profile {
            ScanProfile::HostDiscovery => args.push("-sn".to_owned()),
            ScanProfile::TcpConnect => args.push("-sT".to_owned()),
            ScanProfile::ServiceDiscovery => {
                args.extend(["-sT", "-sV", "--version-light"].map(str::to_owned));
            }
        }
        if !self.ports.is_empty() {
            args.push("-p".to_owned());
            args.push(
                self.ports
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        args.push(self.target.clone());
        args
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NmapFinding {
    pub address: Option<String>,
    pub port: u16,
    pub protocol: String,
    pub state: String,
    pub service: Option<String>,
    pub banner: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NmapHost {
    pub status: Option<String>,
    pub status_reason: Option<String>,
    #[serde(default)]
    pub addresses: Vec<NmapAddress>,
    #[serde(default)]
    pub hostnames: Vec<String>,
    #[serde(default)]
    pub ports: Vec<NmapPort>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NmapAddress {
    pub address: String,
    pub address_type: Option<String>,
    pub vendor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NmapPort {
    pub protocol: String,
    pub port: u16,
    pub state: Option<String>,
    pub state_reason: Option<String>,
    pub service: Option<NmapService>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NmapResult {
    pub target: String,
    #[serde(default)]
    pub profile: ScanProfile,
    #[serde(default)]
    pub scanner_version: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub elapsed_seconds: Option<String>,
    pub hosts_up: u32,
    pub hosts_down: u32,
    #[serde(default)]
    pub hosts_total: u32,
    #[serde(default)]
    pub hosts: Vec<NmapHost>,
    #[serde(default)]
    pub findings: Vec<NmapFinding>,
}

pub(crate) async fn parse_result(
    target: &str,
    profile: ScanProfile,
    xml_path: &Path,
) -> Result<NmapResult, NativeExecutionError> {
    let bytes = tokio::fs::read(xml_path).await?;
    if bytes.len() > 256 * 1024 * 1024 {
        return Err(NativeExecutionError::InvalidOutput(
            "Nmap XML exceeds 256 MiB".to_owned(),
        ));
    }
    parse_xml(target, profile, &bytes, false)
}

pub(crate) async fn parse_progress_result(
    target: &str,
    profile: ScanProfile,
    xml_path: &Path,
) -> Result<NmapResult, NativeExecutionError> {
    let bytes = tokio::fs::read(xml_path).await?;
    if bytes.len() > 256 * 1024 * 1024 {
        return Err(NativeExecutionError::InvalidOutput(
            "Nmap XML exceeds 256 MiB".to_owned(),
        ));
    }
    parse_xml(target, profile, &bytes, true)
}

fn parse_xml(
    target: &str,
    profile: ScanProfile,
    bytes: &[u8],
    allow_truncated_tail: bool,
) -> Result<NmapResult, NativeExecutionError> {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut scanner_version = None;
    let mut started_at = None;
    let mut elapsed_seconds = None;
    let mut hosts_up = 0_u32;
    let mut hosts_down = 0_u32;
    let mut hosts_total = 0_u32;
    let mut hosts = Vec::new();
    let mut current_host: Option<NmapHost> = None;
    let mut current_port: Option<NmapPort> = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) | Ok(Event::Empty(element)) => {
                match element.name().as_ref() {
                    b"nmaprun" => {
                        scanner_version = attribute(&element, b"version")?;
                        started_at = attribute(&element, b"startstr")?;
                    }
                    b"host" => {
                        current_host = Some(NmapHost::default());
                        current_port = None;
                    }
                    b"status" => {
                        if let Some(host) = current_host.as_mut() {
                            host.status = attribute(&element, b"state")?;
                            host.status_reason = attribute(&element, b"reason")?;
                        }
                    }
                    b"address" => {
                        if let (Some(host), Some(address)) =
                            (current_host.as_mut(), attribute(&element, b"addr")?)
                        {
                            host.addresses.push(NmapAddress {
                                address,
                                address_type: attribute(&element, b"addrtype")?,
                                vendor: attribute(&element, b"vendor")?,
                            });
                        }
                    }
                    b"hostname" => {
                        if let (Some(host), Some(name)) =
                            (current_host.as_mut(), attribute(&element, b"name")?)
                        {
                            host.hostnames.push(name);
                        }
                    }
                    b"port" => {
                        let port = attribute(&element, b"portid")?
                            .ok_or_else(|| {
                                NativeExecutionError::InvalidOutput(
                                    "Nmap port is missing portid".to_owned(),
                                )
                            })?
                            .parse::<u16>()
                            .map_err(|error| {
                                NativeExecutionError::InvalidOutput(error.to_string())
                            })?;
                        current_port = Some(NmapPort {
                            port,
                            protocol: attribute(&element, b"protocol")?
                                .unwrap_or_else(|| "tcp".to_owned()),
                            state: None,
                            state_reason: None,
                            service: None,
                        });
                    }
                    b"state" => {
                        if let Some(port) = &mut current_port {
                            port.state = attribute(&element, b"state")?;
                            port.state_reason = attribute(&element, b"reason")?;
                        }
                    }
                    b"service" => {
                        if let Some(port) = &mut current_port {
                            port.service = Some(NmapService {
                                name: attribute(&element, b"name")?,
                                product: attribute(&element, b"product")?,
                                version: attribute(&element, b"version")?,
                                extra_info: attribute(&element, b"extrainfo")?,
                                tunnel: attribute(&element, b"tunnel")?,
                                os_type: attribute(&element, b"ostype")?,
                                method: attribute(&element, b"method")?,
                                confidence: attribute(&element, b"conf")?
                                    .and_then(|value| value.parse::<u8>().ok()),
                            });
                        }
                    }
                    b"finished" => elapsed_seconds = attribute(&element, b"elapsed")?,
                    b"hosts" => {
                        hosts_up = attribute_u32(&element, b"up")?;
                        hosts_down = attribute_u32(&element, b"down")?;
                        hosts_total = attribute_u32(&element, b"total")?;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(element)) if element.name().as_ref() == b"port" => {
                if let (Some(host), Some(port)) = (current_host.as_mut(), current_port.take())
                {
                    host.ports.push(port);
                }
            }
            Ok(Event::End(element)) if element.name().as_ref() == b"host" => {
                if let Some(host) = current_host.take() {
                    hosts.push(host);
                }
                current_port = None;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) if allow_truncated_tail => break,
            Err(error) => return Err(NativeExecutionError::InvalidOutput(error.to_string())),
        }
    }
    if hosts_total == 0 && !hosts.is_empty() {
        hosts_total = u32::try_from(hosts.len()).unwrap_or(u32::MAX);
    }
    if hosts_up == 0 && hosts_down == 0 {
        for host in &hosts {
            match host.status.as_deref() {
                Some("up") => hosts_up = hosts_up.saturating_add(1),
                Some("down") => hosts_down = hosts_down.saturating_add(1),
                _ => {}
            }
        }
    }
    let findings = hosts
        .iter()
        .flat_map(|host| {
            let address = host.addresses.first().map(|address| address.address.clone());
            host.ports.iter().filter_map(move |port| {
                (port.state.as_deref() == Some("open")).then(|| {
                    let service = port.service.as_ref();
                    let banner = service
                        .into_iter()
                        .flat_map(|service| {
                            [
                                service.product.as_deref(),
                                service.version.as_deref(),
                                service.extra_info.as_deref(),
                            ]
                        })
                        .flatten()
                        .collect::<Vec<_>>()
                        .join(" ");
                    NmapFinding {
                        address: address.clone(),
                        port: port.port,
                        protocol: port.protocol.clone(),
                        state: port.state.clone().unwrap_or_else(|| "unknown".to_owned()),
                        service: service.and_then(|service| service.name.clone()),
                        banner: (!banner.is_empty()).then_some(banner),
                    }
                })
            })
        })
        .collect();
    Ok(NmapResult {
        target: target.to_owned(),
        profile,
        scanner_version,
        started_at,
        elapsed_seconds,
        hosts_up,
        hosts_down,
        hosts_total,
        hosts,
        findings,
    })
}

fn attribute_u32(
    element: &quick_xml::events::BytesStart<'_>,
    name: &[u8],
) -> Result<u32, NativeExecutionError> {
    attribute(element, name)?
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|error| NativeExecutionError::InvalidOutput(error.to_string()))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn attribute(
    element: &quick_xml::events::BytesStart<'_>,
    name: &[u8],
) -> Result<Option<String>, NativeExecutionError> {
    for attribute in element.attributes() {
        let attribute =
            attribute.map_err(|error| NativeExecutionError::InvalidOutput(error.to_string()))?;
        if attribute.key.as_ref() == name {
            return Ok(Some(
                attribute
                    .unescape_value()
                    .map_err(|error| NativeExecutionError::InvalidOutput(error.to_string()))?
                    .into_owned(),
            ));
        }
    }
    Ok(None)
}

fn target_is_allowed(target: &str, allowed: &str) -> bool {
    if target == allowed {
        return true;
    }
    let Ok(address) = target.parse::<std::net::IpAddr>() else {
        return false;
    };
    network_contains(allowed, address)
}

fn network_contains(network: &str, address: std::net::IpAddr) -> bool {
    let Some((base, prefix)) = network.split_once('/') else {
        return false;
    };
    let Ok(base) = base.parse::<std::net::IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.parse::<u8>() else {
        return false;
    };
    match (base, address) {
        (std::net::IpAddr::V4(base), std::net::IpAddr::V4(address)) if prefix <= 32 => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            u32::from(base) & mask == u32::from(address) & mask
        }
        (std::net::IpAddr::V6(base), std::net::IpAddr::V6(address)) if prefix <= 128 => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            u128::from(base) & mask == u128::from(address) & mask
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_open_ports_and_normalizes_banner() {
        let xml = br#"<nmaprun><hosthint><status state="up"/><address addr="10.10.4.8" addrtype="ipv4"/></hosthint><host><status state="up"/><address addr="10.10.4.8" addrtype="ipv4"/><ports><port protocol="tcp" portid="443"><state state="open"/><service name="https" product="nginx" version="1.25"/></port><port protocol="tcp" portid="22"><state state="closed"/></port></ports></host></nmaprun>"#;
        let result = parse_xml("10.10.4.8", ScanProfile::ServiceDiscovery, xml, false).unwrap();
        assert_eq!(result.hosts_up, 1);
        assert_eq!(result.hosts_total, 1);
        assert_eq!(result.hosts.len(), 1);
        assert_eq!(result.hosts[0].addresses[0].address, "10.10.4.8");
        assert_eq!(result.hosts[0].ports.len(), 2);
        assert_eq!(result.findings.len(), 1);
        assert_eq!(result.findings[0].port, 443);
        assert_eq!(result.findings[0].banner.as_deref(), Some("nginx 1.25"));
    }

    #[test]
    fn partial_parser_returns_only_complete_hosts() {
        let xml = br#"<nmaprun version="7.99"><host><status state="up"/><address addr="10.10.4.8" addrtype="ipv4"/></host><host><status state="up"/><address addr="#;
        let result = parse_xml("10.10.4.0/24", ScanProfile::HostDiscovery, xml, true).unwrap();
        assert_eq!(result.scanner_version.as_deref(), Some("7.99"));
        assert_eq!(result.hosts.len(), 1);
        assert_eq!(result.hosts_up, 1);
        assert_eq!(result.hosts[0].addresses[0].address, "10.10.4.8");
    }

    #[test]
    fn address_must_be_inside_declared_scope() {
        assert!(target_is_allowed("10.10.4.8", "10.10.4.0/24"));
        assert!(!target_is_allowed("10.10.5.8", "10.10.4.0/24"));
    }
}
