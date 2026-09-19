use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::collections::BTreeSet;
use std::fmt;
use unicode_normalization::UnicodeNormalization;

// serde_json's arbitrary_precision deserializer exposes non-native numbers
// through this reserved map key. It is part of serde_json's serialization
// protocol for Number when that feature is enabled.
const ARBITRARY_PRECISION_NUMBER_TOKEN: &str = "$serde_json::private::Number";

pub fn canonicalize_json(raw: &str) -> Option<String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw.trim());
    let value = StrictValue::deserialize(&mut deserializer).ok()?.0;
    deserializer.end().ok()?;
    let normalized = normalize_value(value)?;
    serde_json::to_string(&normalized).ok()
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
        let mut keys = BTreeSet::new();
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

        keys.insert(first_key.clone());
        output.insert(first_key, map.next_value::<StrictValue>()?.0);
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key `{key}`"
                )));
            }
            let value = map.next_value::<StrictValue>()?;
            output.insert(key, value.0);
        }
        Ok(StrictValue(Value::Object(output)))
    }
}

fn normalize_value(value: Value) -> Option<Value> {
    match value {
        Value::String(value) => Some(Value::String(normalize_string(&value))),
        Value::Number(number) => Some(Value::Number(normalize_number(number))),
        Value::Array(items) => Some(Value::Array(
            items
                .into_iter()
                .map(normalize_value)
                .collect::<Option<Vec<_>>>()?,
        )),
        Value::Object(map) => {
            let mut entries = Vec::with_capacity(map.len());
            let mut normalized_keys = BTreeSet::new();
            for (key, value) in map {
                let key = normalize_string(&key);
                if !normalized_keys.insert(key.clone()) {
                    return None;
                }
                entries.push((key, normalize_value(value)?));
            }
            if let Some((_, Value::Array(attributes))) =
                entries.iter_mut().find(|(key, _)| key == "attributes")
            {
                sort_attributes(attributes);
            }
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Some(Value::Object(entries.into_iter().collect()))
        }
        other => Some(other),
    }
}

fn normalize_number(number: Number) -> Number {
    if let Some(value) = number.as_i64() {
        return Number::from(value);
    }
    if let Some(value) = number.as_u64() {
        return Number::from(value);
    }
    let raw = number.to_string();
    canonical_decimal(&raw)
        .and_then(|value| value.parse::<Number>().ok())
        .unwrap_or(number)
}

fn canonical_decimal(raw: &str) -> Option<String> {
    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, raw),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => (
            &unsigned[..index],
            unsigned[index + 1..].parse::<i128>().ok()?,
        ),
        None => (unsigned, 0),
    };
    let (integer, fraction) = match mantissa.split_once('.') {
        Some(parts) => parts,
        None => (mantissa, ""),
    };
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(integer);
    digits.push_str(fraction);

    let first_nonzero = digits.find(|ch| ch != '0').unwrap_or(digits.len());
    if first_nonzero == digits.len() {
        return Some("0".to_owned());
    }
    digits.drain(..first_nonzero);
    let trailing_zeros = digits.len() - digits.trim_end_matches('0').len();
    digits.truncate(digits.len() - trailing_zeros);

    let power = exponent
        .checked_sub(i128::try_from(fraction.len()).ok()?)?
        .checked_add(i128::try_from(trailing_zeros).ok()?)?;
    let mut output = String::new();
    if negative {
        output.push('-');
    }

    if (0..=64).contains(&power) {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n('0', usize::try_from(power).ok()?));
        return Some(output);
    }

    let decimal_position = i128::try_from(digits.len()).ok()?.checked_add(power)?;
    if decimal_position > 0 && decimal_position < i128::try_from(digits.len()).ok()? {
        let split = usize::try_from(decimal_position).ok()?;
        output.push_str(&digits[..split]);
        output.push('.');
        output.push_str(&digits[split..]);
        return Some(output);
    }
    if (-64..=0).contains(&decimal_position) {
        output.push_str("0.");
        output.extend(std::iter::repeat_n(
            '0',
            usize::try_from(-decimal_position).ok()?,
        ));
        output.push_str(&digits);
        return Some(output);
    }

    output.push(digits.as_bytes()[0] as char);
    if digits.len() > 1 {
        output.push('.');
        output.push_str(&digits[1..]);
    }
    let scientific_exponent = power.checked_add(i128::try_from(digits.len() - 1).ok()?)?;
    if scientific_exponent != 0 {
        output.push('e');
        output.push_str(&scientific_exponent.to_string());
    }
    Some(output)
}

fn attr_key(value: &Value) -> (String, String) {
    match value {
        Value::Object(map) => (
            map.get("trait_type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            map.get("value").map(Value::to_string).unwrap_or_default(),
        ),
        _ => (String::new(), value.to_string()),
    }
}

fn sort_attributes(attributes: &mut Vec<Value>) {
    if attributes.len() < 2 {
        return;
    }
    let mut keyed = std::mem::take(attributes)
        .into_iter()
        .map(|value| (attr_key(&value), value))
        .collect::<Vec<_>>();
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    attributes.extend(keyed.into_iter().map(|(_, value)| value));
}

fn normalize_string(input: &str) -> String {
    // Keep `str::to_lowercase` as a whole-string operation: unlike
    // `char::to_lowercase`, it implements context-sensitive mappings such as
    // Greek final sigma.
    let normalized = input.nfkc().collect::<String>().to_lowercase();
    let mut output = String::with_capacity(input.len());
    let mut pending_space = false;
    for character in normalized.chars() {
        if character.is_whitespace() {
            pending_space = !output.is_empty();
        } else {
            if pending_space {
                output.push(' ');
                pending_space = false;
            }
            output.push(character);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{canonicalize_json, normalize_string};
    use unicode_normalization::UnicodeNormalization;

    fn reference_normalize_string(input: &str) -> String {
        input
            .nfkc()
            .collect::<String>()
            .to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn rejects_duplicate_raw_keys() {
        assert!(canonicalize_json(r#"{"name":"a","name":"b"}"#).is_none());
    }

    #[test]
    fn rejects_post_normalization_key_conflicts() {
        assert!(canonicalize_json(r#"{"Name":"a","name":"b"}"#).is_none());
    }

    #[test]
    fn normalizes_integer_float_representation() {
        assert_eq!(
            canonicalize_json(r#"{"value":1.0}"#).unwrap(),
            r#"{"value":1}"#
        );
    }

    #[test]
    fn preserves_distinct_integers_beyond_f64_precision() {
        let first = canonicalize_json(r#"{"value":9007199254740992.0}"#).unwrap();
        let second = canonicalize_json(r#"{"value":9007199254740993.0}"#).unwrap();

        assert_eq!(first, r#"{"value":9007199254740992}"#);
        assert_eq!(second, r#"{"value":9007199254740993}"#);
        assert_ne!(first, second);
    }

    #[test]
    fn canonicalizes_equivalent_arbitrary_precision_decimals() {
        let plain = canonicalize_json(r#"{"value":12345678901234567890.5000}"#).unwrap();
        let exponent = canonicalize_json(r#"{"value":1.23456789012345678905e19}"#).unwrap();

        assert_eq!(plain, exponent);
        assert_eq!(plain, r#"{"value":12345678901234567890.5}"#);
    }

    #[test]
    fn fused_string_normalization_matches_reference_pipeline() {
        for input in [
            "",
            "  Hello\tWORLD  ",
            "Ｆｕｌｌ　Ｗｉｄｔｈ",
            "Straße\nCAFÉ",
            "A\u{2003}\u{2009}B",
            "İ  Σ  K",
            "ΟΣ ΟΣΑ",
        ] {
            assert_eq!(normalize_string(input), reference_normalize_string(input));
        }
    }

    #[test]
    fn attribute_sort_keys_are_computed_without_changing_stable_order() {
        let canonical = canonicalize_json(
            r#"{"attributes":[{"trait_type":"b","value":2},{"trait_type":"a","value":1},{"trait_type":"a","value":1,"extra":"kept"}]}"#,
        )
        .unwrap();
        assert_eq!(
            canonical,
            r#"{"attributes":[{"trait_type":"a","value":1},{"extra":"kept","trait_type":"a","value":1},{"trait_type":"b","value":2}]}"#
        );
    }
}
