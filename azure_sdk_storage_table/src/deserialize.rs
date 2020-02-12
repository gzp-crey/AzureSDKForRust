use chrono::{DateTime, Utc};
use serde::{
    de::{DeserializeOwned, Error, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{fmt, io};

/// Visitor to deserialize Timestamp
pub struct TimestampVisitor;

pub fn deserialize<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_str(TimestampVisitor)
}

impl<'de> Visitor<'de> for TimestampVisitor {
    type Value = DateTime<Utc>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a timestamp string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match value.parse::<DateTime<Utc>>() {
            Ok(date) => Ok(date),
            Err(e) => Err(E::custom(format!("Parse error {} for {}", e, value))),
        }
    }
}

/// Visitor to deserialize Option<Timestamp>
struct OptionalTimestampVisitor;

impl<'de> Visitor<'de> for OptionalTimestampVisitor {
    type Value = Option<DateTime<Utc>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "null or a timestamp string")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, d: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Some(d.deserialize_str(TimestampVisitor)?))
    }
}

pub fn optional_timestamp<'de, D>(d: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_option(OptionalTimestampVisitor)
}

#[derive(Deserialize)]
pub struct Meta {
    #[serde(rename = "RowKey")]
    pub row_key: String,
    #[serde(rename = "PartitionKey")]
    pub partition_key: String,
    #[serde(rename = "odata.etag")]
    pub etag: Option<String>,
    #[serde(rename = "Timestamp")]
    #[serde(deserialize_with = "optional_timestamp")]
    pub timestamp: Option<DateTime<Utc>>,
}
