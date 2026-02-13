use serde::de::Error;
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum U64Input {
        String(String),
        Number(u64),
    }

    match U64Input::deserialize(deserializer)? {
        U64Input::String(raw) => raw.parse::<u64>().map_err(D::Error::custom),
        U64Input::Number(value) => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Wrapper {
        #[serde(with = "super")]
        seed: u64,
    }

    #[test]
    fn deserialize_accepts_string() {
        let parsed: Wrapper = serde_json::from_str(r#"{"seed":"1337"}"#).expect("string seed");
        assert_eq!(parsed.seed, 1337);
    }

    #[test]
    fn deserialize_accepts_number() {
        let parsed: Wrapper = serde_json::from_str(r#"{"seed":1337}"#).expect("numeric seed");
        assert_eq!(parsed.seed, 1337);
    }
}
