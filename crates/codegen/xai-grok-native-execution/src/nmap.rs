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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NmapResult {
    pub target: String,
    pub hosts_up: u32,
    pub hosts_down: u32,
    pub findings: Vec<NmapFinding>,
}

pub(crate) async fn parse_result(
    target: &str,
    xml_path: &Path,
) -> Result<NmapResult, NativeExecutionError> {
    let bytes = tokio::fs::read(xml_path).await?;
    if bytes.len() > 256 * 1024 * 1024 {
        return Err(NativeExecutionError::InvalidOutput(
            "Nmap XML exceeds 256 MiB".to_owned(),
        ));
    }
    parse_xml(target, &bytes)
}

fn parse_xml(target: &str, bytes: &[u8]) -> Result<NmapResult, NativeExecutionError> {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut current_address = None;
    let mut current_port: Option<NmapFinding> = None;
    let mut findings = Vec::new();
    let mut hosts_up = 0_u32;
    let mut hosts_down = 0_u32;
    let mut inside_host = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) | Ok(Event::Empty(element)) => {
                match element.name().as_ref() {
                    b"host" => {
                        inside_host = true;
                        current_address = None;
                        current_port = None;
                    }
                    b"status" if inside_host => match attribute(&element, b"state")?.as_deref() {
                        Some("up") => hosts_up = hosts_up.saturating_add(1),
                        Some("down") => hosts_down = hosts_down.saturating_add(1),
                        _ => {}
                    },
                    b"address" if inside_host => {
                        if matches!(
                            attribute(&element, b"addrtype")?.as_deref(),
                            Some("ipv4" | "ipv6")
                        ) {
                            current_address = attribute(&element, b"addr")?;
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
                        current_port = Some(NmapFinding {
                            address: current_address.clone(),
                            port,
                            protocol: attribute(&element, b"protocol")?
                                .unwrap_or_else(|| "tcp".to_owned()),
                            state: "unknown".to_owned(),
                            service: None,
                            banner: None,
                        });
                    }
                    b"state" => {
                        if let Some(port) = &mut current_port {
                            port.state = attribute(&element, b"state")?
                                .unwrap_or_else(|| "unknown".to_owned());
                        }
                    }
                    b"service" => {
                        if let Some(port) = &mut current_port {
                            port.service = attribute(&element, b"name")?;
                            let banner = ["product", "version", "extrainfo"]
                                .into_iter()
                                .filter_map(|name| attribute(&element, name.as_bytes()).transpose())
                                .collect::<Result<Vec<_>, _>>()?
                                .join(" ");
                            port.banner = (!banner.is_empty()).then_some(banner);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(element)) if element.name().as_ref() == b"port" => {
                if let Some(port) = current_port.take()
                    && port.state == "open"
                {
                    findings.push(port);
                }
            }
            Ok(Event::End(element)) if element.name().as_ref() == b"host" => {
                inside_host = false;
                current_address = None;
                current_port = None;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(NativeExecutionError::InvalidOutput(error.to_string())),
        }
    }
    Ok(NmapResult {
        target: target.to_owned(),
        hosts_up,
        hosts_down,
        findings,
    })
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
        let result = parse_xml("10.10.4.8", xml).unwrap();
        assert_eq!(result.hosts_up, 1);
        assert_eq!(result.findings.len(), 1);
        assert_eq!(result.findings[0].port, 443);
        assert_eq!(result.findings[0].banner.as_deref(), Some("nginx 1.25"));
    }

    #[test]
    fn address_must_be_inside_declared_scope() {
        assert!(target_is_allowed("10.10.4.8", "10.10.4.0/24"));
        assert!(!target_is_allowed("10.10.5.8", "10.10.4.0/24"));
    }
}
