//! Strict metadata JSON validation for anchor eligibility.

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;

const MAX_METADATA_BYTES: usize = 64 * 1024;
const ARBITRARY_PRECISION_NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// Returns canonical JSON if `raw` is a valid metadata payload; otherwise `None`.
pub fn validated_metadata(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_METADATA_BYTES
        || !matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'['))
    {
        return None;
    }
    let mut deserializer = serde_json::Deserializer::from_str(trimmed);
    let value = StrictValue::deserialize(&mut deserializer).ok()?.0;
    deserializer.end().ok()?;
    let canonical = serde_json::to_string(&value).ok()?;
    (canonical != "{}").then_some(canonical)
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictVisitor)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .map(StrictValue)
            .ok_or_else(|| de::Error::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<StrictValue>()? {
            values.push(value.0);
        }
        Ok(StrictValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut output = Map::new();
        let Some(first_key) = map.next_key::<String>()? else {
            return Ok(StrictValue(Value::Object(output)));
        };
        if first_key == ARBITRARY_PRECISION_NUMBER_TOKEN {
            let raw = map.next_value::<String>()?;
            if map.next_key::<String>()?.is_some() {
                return Err(de::Error::custom(
                    "invalid arbitrary-precision JSON number representation",
                ));
            }
            return raw
                .parse::<Number>()
                .map(Value::Number)
                .map(StrictValue)
                .map_err(de::Error::custom);
        }
        output.insert(first_key, map.next_value::<StrictValue>()?.0);
        while let Some(key) = map.next_key::<String>()? {
            if output.contains_key(&key) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key `{key}`"
                )));
            }
            output.insert(key, map.next_value::<StrictValue>()?.0);
        }
        Ok(StrictValue(Value::Object(output)))
    }
}

#[cfg(test)]
mod tests {
    use super::validated_metadata;

    #[test]
    fn rejects_empty_object_and_duplicate_keys() {
        assert!(validated_metadata(" {} ").is_none());
        assert!(validated_metadata(r#"{"a":1,"a":2}"#).is_none());
        assert_eq!(
            validated_metadata(r#"{"name":"ok"}"#).as_deref(),
            Some(r#"{"name":"ok"}"#)
        );
    }

    #[test]
    fn preserves_large_integers_and_precise_decimals() {
        let raw = r#"{"amount":18446744073709551617,"ratio":0.12345678901234567890123456789}"#;
        assert_eq!(validated_metadata(raw).as_deref(), Some(raw));
        assert_ne!(
            validated_metadata(r#"{"amount":18446744073709551616}"#),
            validated_metadata(r#"{"amount":18446744073709551617}"#)
        );
    }
}
