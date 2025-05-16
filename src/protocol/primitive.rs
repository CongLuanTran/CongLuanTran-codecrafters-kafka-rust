#[derive(Debug)]
pub struct UnsignedVarint(pub u32);

impl UnsignedVarint {
    pub fn encode(&self) -> Vec<u8> {
        let mut result = Vec::new();
        let mut value = self.0;
        while value >= 0x80 {
            result.push((value as u8 & 0x7F) | 0x80);
            value >>= 7;
        }
        result.push(value as u8);
        result
    }
    pub fn decode(bytes: &[u8]) -> (Self, usize) {
        let mut result: u32 = 0;
        let mut shift = 0;
        for (i, byte) in bytes.iter().enumerate() {
            let value = (byte & 0x7F) as u32;
            result |= value << shift;
            if byte & 0x80 == 0 {
                return (UnsignedVarint(result), i + 1);
            }
            shift += 7;
        }
        (UnsignedVarint(result), 0)
    }
}
