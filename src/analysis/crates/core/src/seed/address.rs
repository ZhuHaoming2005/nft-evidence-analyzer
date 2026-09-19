//! Chain-specific contract address validation.

pub fn normalize_address(chain: &str, address: &str) -> Option<String> {
    let chain = chain.trim().to_ascii_lowercase();
    let address = address.trim();
    if is_evm_chain(&chain) {
        if valid_evm_address(address) {
            return Some(address.to_ascii_lowercase());
        }
        return None;
    }
    if chain == "solana" {
        if valid_solana_address(address) {
            return Some(address.to_owned());
        }
        return None;
    }
    None
}

pub fn is_evm_chain(chain: &str) -> bool {
    matches!(
        chain.trim().to_ascii_lowercase().as_str(),
        "ethereum" | "base" | "polygon" | "matic"
    )
}

pub fn valid_evm_address(value: &str) -> bool {
    let value = value.trim();
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .is_some_and(|hex| hex.len() == 40 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub fn valid_solana_address(value: &str) -> bool {
    base58_decoded_len(value.trim()) == Some(32)
}

fn base58_decoded_len(value: &str) -> Option<usize> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.is_empty() {
        return None;
    }
    let leading_zeroes = value.bytes().take_while(|b| *b == b'1').count();
    let mut decoded = vec![0_u8];
    for byte in value.bytes() {
        let digit = ALPHABET.iter().position(|c| *c == byte)? as u16;
        let mut carry = digit;
        for part in &mut decoded {
            let value = u16::from(*part) * 58 + carry;
            *part = value as u8;
            carry = value >> 8;
        }
        while carry > 0 {
            decoded.push(carry as u8);
            carry >>= 8;
        }
    }
    while decoded.last() == Some(&0) && decoded.len() > 1 {
        decoded.pop();
    }
    let body_len = if decoded == [0] { 0 } else { decoded.len() };
    Some(leading_zeroes + body_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_evm_and_solana_addresses() {
        assert!(valid_evm_address(
            "0x1111111111111111111111111111111111111111"
        ));
        assert!(!valid_evm_address("0x1"));
        assert!(valid_solana_address(
            "So11111111111111111111111111111111111111112"
        ));
        assert!(!valid_solana_address("not-base58-0"));
    }
}
