use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                Self(format!("{}_{}", $prefix, uuid::Uuid::new_v4().simple()))
            }

            pub fn from_string(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }
    };
}

string_id!(ArtifactId, "art");
string_id!(ClientId, "client");
string_id!(CommandId, "cmd");
string_id!(EvidenceId, "evidence");
string_id!(EngagementId, "eng");
string_id!(ExerciseId, "exercise");
string_id!(EventId, "evt");
string_id!(FindingId, "finding");
string_id!(OperationId, "op");
string_id!(OperationRunId, "run");
string_id!(OperatorSessionId, "session");
string_id!(PlaybookId, "playbook");
string_id!(ProfileId, "profile");
string_id!(ProviderId, "provider");
string_id!(RequestId, "req");
string_id!(ServiceId, "service");
string_id!(TaskId, "task");
string_id!(TeamId, "team");
string_id!(TargetId, "target");
string_id!(WorkspaceId, "workspace");

/// Canonical name for one model/tool work item. The wire protocol retains the
/// historical `EngagementId` spelling until its next incompatible migration.
pub type TurnId = EngagementId;
