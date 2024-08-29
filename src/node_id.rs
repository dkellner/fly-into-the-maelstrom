use core::fmt;
use std::str::FromStr;

use anyhow::bail;
use serde_with::{DeserializeFromStr, SerializeDisplay};

// We want `NodeId` to be `Copy`. I opted for storing 8 ASCII bytes, but this
// is an implementation detail. Because this is a private field, changing this
// will not affect other parts of the code.
#[derive(PartialEq, Eq, Clone, Copy, Debug, SerializeDisplay, DeserializeFromStr)]
pub struct NodeId([u8; 8]);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for c in self.0.iter().copied().take_while(|&c| c != 0) {
            write!(f, "{}", char::from(c))?;
        }
        Ok(())
    }
}

impl FromStr for NodeId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut result = [0; 8];
        for (idx, c) in s.chars().enumerate() {
            if !c.is_ascii() {
                bail!("node id contains non-ascii chars");
            }
            let ascii = c.try_into()?;
            if ascii == 0 {
                bail!("encountered null byte");
            }
            if idx >= result.len() {
                bail!("node id is longer than 8 chars");
            }
            result[idx] = ascii;
        }
        Ok(Self(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_node_id() {
        for valid_node_id in ["n1", "12345678"] {
            let id: NodeId = serde_json::from_str(&format!("\"{valid_node_id}\"")).unwrap();
            assert_eq!(id.to_string(), valid_node_id);
        }
    }

    #[test]
    fn deserialize_node_id_failures() {
        for invalid_node_id in ["toolong!!", "noäscii"] {
            let id: Result<NodeId, _> = serde_json::from_str(&format!("\"{invalid_node_id}\""));
            assert!(
                id.is_err(),
                "deserialization of \"{}\" should fail",
                invalid_node_id
            );
        }
    }
}
