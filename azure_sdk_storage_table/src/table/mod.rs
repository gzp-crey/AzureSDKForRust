mod batch;
use self::batch::generate_batch_payload;
pub use self::batch::BatchItem;
use crate::TableEntry;
use azure_sdk_core::errors::{
    check_status_extract_body, check_status_extract_headers_and_body, AzureError,
};
use azure_sdk_storage_core::client::Client;
use azure_sdk_storage_core::{
    get_default_json_mime, get_json_mime_fullmetadata, get_json_mime_nometadata, ServiceType,
};
use futures::stream::Stream;
use http::HeaderMap;
use hyper::client::ResponseFuture;
use hyper::header::{self, HeaderValue};
use hyper::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use std::convert::TryFrom;

const TABLE_TABLES: &str = "TABLES";

#[derive(Clone)]
pub struct TableService {
    client: Client,
}

impl TableService {
    pub fn new(client: Client) -> Self {
        TableService { client }
    }

    pub async fn list_tables(&self) -> Result<Vec<String>, AzureError> {
        let entities = self.query_entries(TABLE_TABLES, None).await?;
        let e: Vec<String> = entities
            .into_iter()
            .map(|x: TableEntry<TableEntity>| x.payload.TableName)
            .collect();
        Ok(e)
    }

    // Create table if not exists.
    pub async fn create_table<T: Into<String>>(&self, table_name: T) -> Result<(), AzureError> {
        let body = &serde_json::to_string(&TableEntity {
            TableName: table_name.into(),
        })
        .unwrap();
        debug!("body == {}", body);
        let future_response = self.request_with_default_header(
            TABLE_TABLES,
            &Method::POST,
            Some(body),
            false,
            |_| {},
        )?;

        check_status_extract_body(future_response, StatusCode::CREATED).await?;
        Ok(())
    }

    pub async fn get_entry<T: DeserializeOwned>(
        &self,
        table_name: &str,
        partition_key: &str,
        row_key: &str,
    ) -> Result<TableEntry<T>, AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        let path = &entry_path(table_name, partition_key, row_key);
        let future_response =
            self.request_with_default_header(path, &Method::GET, None, false, |_| {})?;
        let (headers, body) =
            check_status_extract_headers_and_body(future_response, StatusCode::OK).await?;

        TableEntry::try_from((&headers, &body as &[u8]))
    }

    pub async fn query_entries<T>(
        &self,
        table_name: &str,
        query: Option<&str>,
    ) -> Result<Vec<TableEntry<T>>, AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        let mut path = table_name.to_owned();
        if let Some(clause) = query {
            path.push_str("?");
            path.push_str(clause);
        }

        let future_response =
            self.request_with_default_header(path.as_str(), &Method::GET, None, false, |_| {})?;
        let body = check_status_extract_body(future_response, StatusCode::OK).await?;
        let ec = serde_json::from_str::<EntryCollection<T>>(&body)?;
        Ok(ec.value)
    }

    async fn query_entry_collection<T>(
        &self,
        table_name: &str,
        query: Option<&str>,
        continuation: Option<&Continuation>,
        fullmetadata: bool,
    ) -> Result<EntryCollection<T>, AzureError>
    where
        T: DeserializeOwned + Serialize,
    {
        debug!("query_entry_collection(table_name == {}, query == {:?}, continuation == {:?}, fullmetadata == {:?}) called", table_name, query, continuation, fullmetadata);
        let mut path = table_name.to_owned();
        path.push_str("?");
        if let Some(clause) = query {
            path.push_str(clause);
        }
        if let Some(cont) = continuation {
            path.push_str("&NextPartitionKey=");
            path.push_str(&cont.next_partition_key);
            path.push_str("&NextRowKey=");
            path.push_str(&cont.next_row_key);
        }

        let future_response = self.request_with_default_header(
            path.as_str(),
            &Method::GET,
            None,
            fullmetadata,
            |_| {},
        )?;

        let (headers, body) =
            check_status_extract_headers_and_body(future_response, StatusCode::OK).await?;

        Ok(
            serde_json::from_slice::<EntryCollection<T>>(&body).map(|mut ec| {
                ec.continuation = continuation_from_headers(&headers);
                ec
            })?,
        )
    }

    fn stream_query_entries_metadata<'a, T>(
        &'a self,
        table_name: &'a str,
        query: Option<&'a str>,
        fullmetadata: bool,
    ) -> impl Stream<Item = Result<Vec<TableEntry<T>>, AzureError>> + 'a
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        futures::stream::unfold(ContinuationState::Start, move |cont_state| {
            async move {
                let cont = match cont_state {
                    ContinuationState::Start => None,
                    ContinuationState::Next(Some(cont)) => Some(cont),
                    ContinuationState::Next(None) => return None,
                };

                debug!("cont == {:?}", cont);

                let mut path = table_name.to_owned();
                if let Some(clause) = query {
                    path.push_str("?");
                    path.push_str(clause);
                }

                let ec = self
                    .query_entry_collection(table_name, query, cont.as_ref(), fullmetadata)
                    .await;

                let ec = match ec {
                    Ok(ec) => ec,
                    Err(err) => return Some((Err(err), ContinuationState::Next(None))),
                };

                Some((Ok(ec.value), ContinuationState::Next(ec.continuation)))
            }
        })
    }

    pub fn stream_query_entries<'a, T>(
        &'a self,
        table_name: &'a str,
        query: Option<&'a str>,
    ) -> impl Stream<Item = Result<Vec<TableEntry<T>>, AzureError>> + 'a
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        self.stream_query_entries_metadata(table_name, query, false)
    }

    pub fn stream_query_entries_fullmetadata<'a, T>(
        &'a self,
        table_name: &'a str,
        query: Option<&'a str>,
    ) -> impl Stream<Item = Result<Vec<TableEntry<T>>, AzureError>> + 'a
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        self.stream_query_entries_metadata(table_name, query, true)
    }

    pub async fn insert_entry<'a, T>(
        &self,
        table_name: &str,
        entry: &'a TableEntry<T>,
    ) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        let obj_ser = serde_json::to_string(&entry)?.to_owned();

        let future_response = self.request_with_default_header(
            table_name,
            &Method::POST,
            Some(&obj_ser),
            false,
            |_| {},
        )?;

        check_status_extract_body(future_response, StatusCode::CREATED).await?;
        Ok(())
    }

    pub async fn update_entry<'a, T>(
        &self,
        table_name: &str,
        entry: &'a TableEntry<T>,
    ) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        let obj_ser = serde_json::to_string(&entry)?.to_owned();
        let path = &entry_path(table_name, &entry.partition_key, &entry.row_key);

        // IsMatched is mandatory, we pass * if the caller
        // does not care for it.
        let etag = match entry.etag {
            Some(ref etag) => etag.as_ref(),
            None => "*",
        };

        let future_response = self.request_with_default_header(
            path,
            &Method::PUT,
            Some(&obj_ser),
            false,
            |headers| {
                headers.append(header::IF_MATCH, etag.parse().unwrap());
            },
        )?;
        let (headers, body) =
            check_status_extract_headers_and_body(future_response, StatusCode::NO_CONTENT).await?;

        debug!("response headers == {:?}", headers);
        debug!("response body == {:?}", body);

        Ok(())
    }

    pub async fn delete_entry<'a, T>(
        &self,
        table_name: &str,
        entry: &'a TableEntry<T>,
    ) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        let path = &entry_path(table_name, &entry.partition_key, &entry.row_key);

        let future_response = self.request(path, &Method::DELETE, None, |ref mut request| {
            request.header(
                header::ACCEPT,
                HeaderValue::from_static(get_json_mime_nometadata()),
            );
            request.header(header::IF_MATCH, header::HeaderValue::from_static("*"));
        })?;
        check_status_extract_body(future_response, StatusCode::NO_CONTENT).await?;
        Ok(())
    }

    pub async fn batch<T>(
        &self,
        table_name: &str,
        partition_key: &str,
        batch_items: &[BatchItem<T>],
    ) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        let payload = &generate_batch_payload(
            self.client.get_uri_prefix(ServiceType::Table).as_str(),
            table_name,
            partition_key,
            batch_items,
        );

        let future_response =
            self.request("$batch", &Method::POST, Some(payload), |ref mut request| {
                request.header(
                    header::CONTENT_TYPE,
                    header::HeaderValue::from_static(get_batch_mime()),
                );
            })?;
        check_status_extract_body(future_response, StatusCode::ACCEPTED).await?;
        // TODO deal with body response, handle batch failure.
        // let ref body = get_response_body(&mut response)?;
        // info!("{}", body);
        Ok(())
    }

    fn request_with_default_header<H>(
        &self,
        segment: &str,
        method: &Method,
        request_str: Option<&str>,
        fullmetadata: bool,
        add_extra_headers: H,
    ) -> Result<ResponseFuture, AzureError>
    where
        H: FnOnce(&mut HeaderMap),
    {
        self.request(segment, method, request_str, |ref mut request| {
            if fullmetadata {
                request.header(
                    header::ACCEPT,
                    HeaderValue::from_static(get_json_mime_fullmetadata()),
                );
            } else {
                request.header(
                    header::ACCEPT,
                    HeaderValue::from_static(get_json_mime_nometadata()),
                );
            }
            request.header(
                header::ACCEPT,
                HeaderValue::from_static(get_json_mime_nometadata()),
            );
            if request_str.is_some() {
                request.header(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(get_default_json_mime()),
                );
            }

            // since we have already added some headers
            // this unwrap should be safe
            add_extra_headers(request.headers_mut().unwrap());
        })
    }

    fn request<F>(
        &self,
        segment: &str,
        method: &Method,
        request_str: Option<&str>,
        headers_func: F,
    ) -> Result<ResponseFuture, AzureError>
    where
        F: FnOnce(&mut ::http::request::Builder),
    {
        trace!("{:?} {}", method, segment);
        if let Some(body) = request_str {
            trace!("Request: {}", body);
        }

        let request_vec: Option<&[u8]> = match request_str {
            Some(s) => Some(s.as_bytes()),
            None => None,
        };

        self.client
            .perform_table_request(segment, method, headers_func, request_vec)
    }
}

#[derive(Clone)]
pub struct TableStorage {
    service: TableService,
    table_name: String,
}

impl TableStorage {
    pub fn new<S: Into<String>>(service: TableService, table_name: S) -> Self {
        TableStorage {
            service,
            table_name: table_name.into(),
        }
    }

    pub async fn create_table(&self) -> Result<(), AzureError> {
        self.service.create_table(self.table_name.clone()).await
    }

    pub async fn get_entry<T: DeserializeOwned>(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<TableEntry<T>, AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        self.service
            .get_entry(&self.table_name, partition_key, row_key)
            .await
    }

    pub async fn query_entries<T>(
        &self,
        query: Option<&str>,
    ) -> Result<Vec<TableEntry<T>>, AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        self.service.query_entries(&self.table_name, query).await
    }

    pub fn stream_query_entries<'a, T>(
        &'a self,
        query: Option<&'a str>,
    ) -> impl Stream<Item = Result<Vec<TableEntry<T>>, AzureError>> + 'a
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        self.service.stream_query_entries(&self.table_name, query)
    }

    pub fn stream_query_entries_fullmetadata<'a, T>(
        &'a self,
        query: Option<&'a str>,
    ) -> impl Stream<Item = Result<Vec<TableEntry<T>>, AzureError>> + 'a
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        self.service
            .stream_query_entries_fullmetadata(&self.table_name, query)
    }

    pub async fn insert_entry<'a, T>(&self, entry: &'a TableEntry<T>) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned + 'a,
    {
        self.service
            .insert_entry::<T>(&self.table_name, entry)
            .await
    }

    pub async fn update_entry<T>(&self, entry: &TableEntry<T>) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        self.service.update_entry(&self.table_name, entry).await
    }

    pub async fn delete_entry<'a, T>(&self, entry: &'a TableEntry<T>) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        self.service.delete_entry(&self.table_name, entry).await
    }

    pub async fn batch<T>(
        &self,
        partition_key: &str,
        batch_items: &[BatchItem<T>],
    ) -> Result<(), AzureError>
    where
        T: Serialize + DeserializeOwned,
    {
        self.service
            .batch(&self.table_name, partition_key, batch_items)
            .await
    }
}

const HEADER_NEXTPARTITIONKEY: &str = "x-ms-continuation-NextPartitionKey";
const HEADER_NEXTROWKEY: &str = "x-ms-continuation-NextRowKey";

fn continuation_from_headers(headers: &HeaderMap) -> Option<Continuation> {
    if headers.contains_key(HEADER_NEXTPARTITIONKEY) && headers.contains_key(HEADER_NEXTROWKEY) {
        Some(Continuation {
            next_partition_key: headers[HEADER_NEXTPARTITIONKEY]
                .to_str()
                .unwrap()
                .to_string(),
            next_row_key: headers[HEADER_NEXTROWKEY].to_str().unwrap().to_string(),
        })
    } else {
        None
    }
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize)]
struct TableEntity {
    TableName: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EntryCollection<T> {
    value: Vec<TableEntry<T>>,
    #[serde(skip)]
    continuation: Option<Continuation>,
}

#[derive(Debug, Clone)]
struct Continuation {
    next_partition_key: String,
    next_row_key: String,
}

#[derive(Debug, Clone)]
enum ContinuationState {
    Start,
    Next(Option<Continuation>),
}

#[inline]
fn entry_path(table_name: &str, partition_key: &str, row_key: &str) -> String {
    table_name.to_owned() + "(PartitionKey='" + partition_key + "',RowKey='" + row_key + "')"
}

#[inline]
pub fn get_batch_mime() -> &'static str {
    "multipart/mixed; boundary=batch_a1e9d677-b28b-435e-a89e-87e6a768a431"
}
