use std::{fmt, str::FromStr};

use derive_more::derive::Display;
use serde_with::{DeserializeFromStr, SerializeDisplay};

const NODE_ID_LENGTH: usize = 8;

// We want `NodeId` to be `Copy`. I opted for storing 8 ASCII bytes, but this
// is an implementation detail. Because this is a private field, changing this
// will not affect other parts of the code.
#[derive(
    PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Hash, SerializeDisplay, DeserializeFromStr,
)]
pub struct NodeId([u8; NODE_ID_LENGTH]);

#[derive(PartialEq, Eq, Clone, Copy, Debug, Display)]
pub enum ParseNodeIdError {
    #[display("node id is longer than 8 characters")]
    TooLong,
    #[display("node id contains invalid byte: {_0:x}")]
    InvalidByte(u8),
}

impl NodeId {
    /// Parse a string `s` to return a [NodeId].
    ///
    /// This is a const version of [FromStr::from_str].
    pub const fn from_str(s: &str) -> Result<Self, ParseNodeIdError> {
        let bytes = s.as_bytes();
        if bytes.len() > NODE_ID_LENGTH {
            return Err(ParseNodeIdError::TooLong);
        }

        let mut result = [0; NODE_ID_LENGTH];
        let mut idx = 0;
        while idx < bytes.len() {
            let b = bytes[idx];
            if b.is_ascii_alphabetic() || b.is_ascii_digit() || b.is_ascii_punctuation() {
                result[idx] = b;
            } else {
                return Err(ParseNodeIdError::InvalidByte(b));
            }
            idx += 1;
        }

        Ok(Self(result))
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NodeId(\"{}\")", self)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for c in self.0.iter().copied().take_while(|&c| c != 0) {
            write!(f, "{}", char::from(c))?;
        }
        Ok(())
    }
}

impl FromStr for NodeId {
    type Err = ParseNodeIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NodeId::from_str(s)
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
