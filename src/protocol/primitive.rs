use anyhow::{Ok, Result};
use derive_more::Deref;

pub trait Serializable: Sized {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])>;
}

#[derive(Debug, Deref)]
pub struct UnsignedVarint(pub u32);

impl Serializable for UnsignedVarint {
    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        let mut value = self.0;
        while value >= 0x80 {
            result.push((value as u8 & 0x7F) | 0x80);
            value >>= 7;
        }
        result.push(value as u8);
        result
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let mut result = 0;
        let mut shift = 0;
        for (i, byte) in bytes.iter().enumerate() {
            let value = (byte & 0x7F) as u32;
            result |= value << shift;
            if byte & 0x80 == 0 {
                return Ok((UnsignedVarint(result), &bytes[i + 1..]));
            }
            shift += 7;
        }
        anyhow::bail!("Failed to deserialize")
    }
}

#[derive(Debug)]
pub struct Varint(pub i32);

impl Serializable for Varint {
    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        let mut value = ((self.0 << 1) ^ (self.0 >> 31)) as u32;
        while value >= 0x80 {
            result.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        result.push(value as u8);
        result
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let mut result = 0;
        let mut shift = 0;
        for (i, byte) in bytes.iter().enumerate() {
            let value = (byte & 0x7F) as i32;
            result |= value << shift;
            if byte & 0x80 == 0 {
                let decode = (result >> 1) ^ -(result & 1);
                return Ok((Varint(decode), &bytes[i + 1..]));
            }
            shift += 7;
        }
        anyhow::bail!("Failed to deserialize")
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
        buf.extend(UnsignedVarint(self.tag).serialize());
        buf.extend(UnsignedVarint(length).serialize());
        buf.extend(&self.data);
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (tag, bytes) = UnsignedVarint::deserialize(bytes)?;
        let tag = tag.0;
        let (length, bytes) = UnsignedVarint::deserialize(bytes)?;
        let length = length.0;
        let (data, bytes) = bytes.split_at(length as usize);
        let data = data.to_vec();
        Ok((TagField { tag, data }, bytes))
    }
}

#[derive(Debug, Default)]
pub struct TagSection(pub Option<Vec<TagField>>);

impl TagSection {
    pub fn new() -> Self {
        TagSection(Some(vec![]))
    }
}

impl Serializable for TagSection {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match &self.0 {
            None => buf.extend(UnsignedVarint(0).serialize()),
            Some(fields) => {
                buf.extend(UnsignedVarint(fields.len() as u32).serialize());
                for field in fields {
                    buf.extend(field.serialize());
                }
            }
        }
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (length, bytes) = UnsignedVarint::deserialize(bytes)?;
        if length.0 == 0 {
            return Ok((TagSection(None), bytes));
        }

        let mut tags: Vec<TagField> = vec![];
        let mut bytes = bytes;
        for _ in 0..length.0 {
            let (field, rest) = TagField::deserialize(bytes)?;
            bytes = rest;
            tags.push(field);
        }
        Ok((TagSection(Some(tags)), bytes))
    }
}

#[derive(Debug, Deref, Default)]
pub struct CompactString(pub Option<String>);

impl Serializable for CompactString {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match &self.0 {
            None => buf.extend(UnsignedVarint(0).serialize()),
            Some(str) => {
                buf.extend(UnsignedVarint(str.len() as u32 + 1).serialize());
                buf.extend(str.as_bytes());
            }
        }
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (length, bytes) = UnsignedVarint::deserialize(bytes)?;
        let length = length.0;
        if length == 0 {
            return Ok((CompactString(None), bytes));
        }

        let (str, bytes) = bytes.split_at(length as usize - 1);
        let str = String::from_utf8_lossy(str).into_owned();
        Ok((CompactString(Some(str)), bytes))
    }
}

#[derive(Debug, Default, Deref)]
pub struct CompactArray<T: Serializable>(pub Option<Vec<T>>);

impl<T: Serializable> Serializable for CompactArray<T> {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match &self.0 {
            None => buf.extend(UnsignedVarint(0).serialize()),
            Some(array) => {
                buf.extend(UnsignedVarint(array.len() as u32 + 1).serialize());
                for element in array {
                    buf.extend(element.serialize());
                }
            }
        }
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (length, bytes) = UnsignedVarint::deserialize(bytes)?;
        let length = length.0;
        if length == 0 {
            return Ok((CompactArray(None), bytes));
        }

        let mut bytes = bytes;
        let mut array = Vec::with_capacity(length as usize - 1);
        for _ in 0..(length - 1) {
            let (item, rest) = T::deserialize(bytes)?;
            array.push(item);
            bytes = rest;
        }
        Ok((CompactArray(Some(array)), bytes))
    }
}
