use crate::deserialize::Meta;
use azure_sdk_core::errors::AzureError;
use chrono::{DateTime, Utc};
use http::header;
use http::HeaderMap;
use serde::{de::DeserializeOwned, ser::SerializeStruct, Deserialize, Serialize, Serializer};
use serde_json::{
    self,
    ser::{Formatter, Serializer as JSONSerializer},
};
use std::io;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NoData {}

#[derive(Debug, Clone)]
pub struct TableEntity<T> {
    pub row_key: String,
    pub partition_key: String,
    pub etag: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    pub payload: T,
}

impl<T> std::convert::TryFrom<(&HeaderMap, &[u8])> for TableEntity<T>
where
    T: DeserializeOwned,
{
    type Error = AzureError;

    fn try_from(value: (&HeaderMap, &[u8])) -> Result<Self, Self::Error> {
        let headers = value.0;
        let body = value.1;
        log::trace!("headers == {:?}", headers);
        log::trace!("body == {:?}", std::str::from_utf8(body));

        let mut entity = Self::from_payload(body)?;

        if let Some(etag) = headers.get(header::ETAG) {
            entity.etag = Some(etag.to_str()?.to_owned());
        }

        Ok(entity)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ContinuationCursor {
    pub(crate) partition_key: String,
    pub(crate) row_key: String,
}

#[derive(Debug, Clone)]
pub struct Continuation {
    pub(crate) fused: bool,
    pub(crate) next: Option<ContinuationCursor>,
}

impl Continuation {
    pub fn start() -> Self {
        Continuation {
            fused: false,
            next: None,
        }
    }
}

impl std::convert::TryFrom<&HeaderMap> for Continuation {
    type Error = AzureError;

    fn try_from(headers: &HeaderMap) -> Result<Self, Self::Error> {
        const HEADER_NEXTPARTITIONKEY: &str = "x-ms-continuation-NextPartitionKey";
        const HEADER_NEXTROWKEY: &str = "x-ms-continuation-NextRowKey";

        if headers.contains_key(HEADER_NEXTPARTITIONKEY) && headers.contains_key(HEADER_NEXTROWKEY)
        {
            Ok(Continuation {
                fused: false,
                next: Some(ContinuationCursor {
                    partition_key: headers[HEADER_NEXTPARTITIONKEY].to_str()?.to_string(),
                    row_key: headers[HEADER_NEXTROWKEY].to_str()?.to_string(),
                }),
            })
        } else {
            Ok(Continuation {
                fused: true,
                next: None,
            })
        }
    }
}

// Formatter that flattens a nested structure
struct FlattenFormatter(usize);

impl Formatter for FlattenFormatter {
    #[inline]
    fn begin_array<W: ?Sized>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Array serialization is not supported",
        ))
    }

    #[inline]
    fn begin_object<W: ?Sized>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        self.0 += 1;
        if self.0 == 1 {
            //meta start
            writer.write_all(b"{")
        } else if self.0 == 2 {
            // payload start
            writer.write_all(b",")
        } else {
            // nesting in payload
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Nested structures are not supported, try to use #[serde(flatten)]",
            ))
        }
    }

    #[inline]
    fn end_object<W: ?Sized>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        if self.0 == 2 {
            // payload end
            writer.write_all(b"}")
        } else {
            Ok(())
        }
    }
}

/// Serialize TableEntity for message payloads
impl<T> TableEntity<T>
where
    T: Serialize,
{
    pub fn to_payload(&self) -> Result<String, AzureError> {
        let writer = Vec::with_capacity(128);
        let mut ser = JSONSerializer::with_formatter(writer, FlattenFormatter(0));
        {
            let mut s = ser.serialize_struct("Meta", 2)?;
            s.serialize_field("PartitionKey", &self.partition_key)?;
            s.serialize_field("RowKey", &self.row_key)?;
            s.end()?;
        }
        self.payload.serialize(&mut ser)?;
        let string = String::from_utf8(ser.into_inner())?;
        Ok(string)
    }
}

#[derive(Deserialize)]
struct MetaValues {
    values: Vec<Meta>,
}

#[derive(Deserialize)]
struct PayloadValues<'de, T>
where
    T: Deserialize<'de>,
{
    values: Vec<T>,
}

impl<T> TableEntity<T>
where
    T: DeserializeOwned,
{
    pub fn from_payload(data: &[u8]) -> Result<Self, AzureError> {
        let meta: Meta = serde_json::from_slice(data)?;
        let payload: T = serde_json::from_slice(data)?;
        Ok(Self {
            partition_key: meta.partition_key,
            row_key: meta.row_key,
            etag: meta.etag,
            timestamp: meta.timestamp,
            payload: payload,
        })
    }

    pub fn from_payload_set(data: &[u8]) -> Result<Vec<Self>, AzureError> {
        let meta: MetaValues = serde_json::from_slice(data)?;
        let meta = meta.values;
        let payload: PayloadValues<Self> = serde_json::from_slice(data)?;
        let payload = payload.values;
        let mut v = Vec::with_capacity(meta.len());
        for (m, p) in meta.into_iter().zip(payload.into_iter()) {
            v.push(Self {
                partition_key: m.partition_key,
                row_key: m.row_key,
                etag: m.etag,
                timestamp: m.timestamp,
                payload: p,
            });
        }

        Ok(v)
    }
}
