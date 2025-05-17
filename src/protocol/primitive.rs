pub trait Serializable {
    fn serialize(&self) -> Vec<u8>;
}

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

#[derive(Debug)]
pub struct TagField {
    pub tag: u32,
    pub data: Vec<u8>,
}

impl Serializable for TagField {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let length = self.data.len() as u32;
        buf.extend(UnsignedVarint(self.tag).encode());
        buf.extend(UnsignedVarint(length).encode());
        buf.extend(&self.data);
        buf
    }
}

#[derive(Debug)]
pub struct TagSection(pub Option<Vec<TagField>>);

impl TagSection {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match &self.0 {
            None => buf.extend(UnsignedVarint(0).encode()),
            Some(fields) => {
                buf.extend(UnsignedVarint(fields.len() as u32 + 1).encode());
                for field in fields {
                    buf.extend(field.serialize());
                }
            }
        }
        buf
    }
}
