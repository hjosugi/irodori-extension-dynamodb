use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use aws_config::sts::AssumeRoleProvider;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndexDescription, KeySchemaElement,
    LocalSecondaryIndexDescription, Projection,
};
use aws_sdk_dynamodb::Client;
use aws_types::region::Region;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::{json, Map, Number, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, DynamoConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct DynamoConnection {
    client: Client,
    config: DynamoConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DynamoConfig {
    region: String,
    endpoint: Option<String>,
    profile: Option<String>,
    credentials: Option<DynamoCredentials>,
    role_arn: Option<String>,
    role_session_name: Option<String>,
    external_id: Option<String>,
    session_duration_secs: Option<u64>,
    redaction_values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DynamoCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, DynamoConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match DynamoConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection =
        match runtime().and_then(|runtime| runtime.block_on(DynamoConnection::new(config))) {
            Ok(connection) => connection,
            Err(err) => return abi::error("connector.connectFailed", err),
        };
    let table_count = match runtime()
        .and_then(|runtime| runtime.block_on(probe_connection(&connection)))
    {
        Ok(table_count) => table_count,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };

    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = connection.connect_response(&connection_id, table_count);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(statement) = abi::string_field(request, "statement")
        .or_else(|| abi::string_field(request, "sql"))
        .or_else(|| abi::string_field(request, "query"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string statement, sql, or query field.",
        );
    };
    let parameters = match request.get("parameters").or_else(|| request.get("params")) {
        Some(Value::Array(values)) => match values
            .iter()
            .map(json_to_attribute_value)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(values) => values,
            Err(err) => return abi::error("connector.invalidRequest", err),
        },
        Some(_) => {
            return abi::error(
                "connector.invalidRequest",
                "query parameters must be a JSON array.",
            )
        }
        None => Vec::new(),
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| {
        runtime.block_on(run_statement(
            &connection,
            statement,
            parameters,
            abi::max_rows(request),
            bool_option(request, &["consistentRead"]).unwrap_or(false),
        ))
    }) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl DynamoConnection {
    async fn new(config: DynamoConfig) -> Result<Self, String> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()));
        if let Some(profile) = config.profile.as_deref() {
            loader = loader.profile_name(profile);
        }
        if let Some(credentials) = config.credentials.as_ref() {
            loader = loader.credentials_provider(Credentials::new(
                credentials.access_key_id.clone(),
                credentials.secret_access_key.clone(),
                credentials.session_token.clone(),
                None,
                "irodori-dynamodb",
            ));
        }
        let shared_config = loader.load().await;
        // `aws_config::defaults` above has already resolved the standard chain
        // — environment, named profile, SSO, web identity, ECS/IMDS. What it
        // cannot do is take a role ARN from the connection rather than from a
        // `role_arn`/`source_profile` profile on disk, so wrap the resolved
        // base credentials when the profile asks for a role.
        let shared_config = if let Some(role_arn) = config.role_arn.as_deref() {
            let mut provider = AssumeRoleProvider::builder(role_arn).configure(&shared_config);
            if let Some(name) = config.role_session_name.as_deref() {
                provider = provider.session_name(name);
            }
            if let Some(external_id) = config.external_id.as_deref() {
                provider = provider.external_id(external_id);
            }
            if let Some(secs) = config.session_duration_secs {
                provider = provider.session_length(std::time::Duration::from_secs(secs));
            }
            shared_config
                .into_builder()
                .credentials_provider(SharedCredentialsProvider::new(provider.build().await))
                .build()
        } else {
            shared_config
        };
        let mut builder = aws_sdk_dynamodb::config::Builder::from(&shared_config);
        if let Some(endpoint) = config.endpoint.as_deref() {
            builder = builder.endpoint_url(endpoint);
        }
        let client = Client::from_conf(builder.build());
        Ok(Self { client, config })
    }

    fn connect_response(&self, connection_id: &str, table_count: usize) -> Map<String, Value> {
        let mut response = Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            (
                "connectionId".to_string(),
                Value::String(connection_id.to_string()),
            ),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "region".to_string(),
                Value::String(self.config.region.clone()),
            ),
            ("tableCount".to_string(), json!(table_count)),
        ]);
        if let Some(endpoint) = self.config.endpoint.as_deref() {
            response.insert("endpoint".to_string(), Value::String(endpoint.to_string()));
        }
        if let Some(profile) = self.config.profile.as_deref() {
            response.insert("profile".to_string(), Value::String(profile.to_string()));
        }
        response
    }
}

impl DynamoConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let region = option_string(request, &["region", "awsRegion"])
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .or_else(|| profile_region(request))
            .ok_or_else(|| "DynamoDB requires an AWS region.".to_string())?;
        let endpoint = option_string(
            request,
            &[
                "endpoint",
                "endpointUrl",
                "endpointURL",
                "url",
                "connectionString",
                "dsn",
            ],
        )
        .map(|value| normalize_endpoint(&value, &region));
        let profile = option_string(request, &["profile", "awsProfile"])
            .or_else(|| form_profile_name(request))
            .or_else(|| std::env::var("AWS_PROFILE").ok());
        let credentials = credentials_from_request(request).or_else(env_credentials);
        let role_arn = option_string(request, &["roleArn", "awsRoleArn", "assumeRoleArn"]);
        let role_session_name = option_string(
            request,
            &["roleSessionName", "awsRoleSessionName", "sessionName"],
        );
        let external_id = option_string(request, &["externalId", "awsExternalId"]);
        let session_duration_secs = option_string(
            request,
            &["sessionDurationSeconds", "assumeRoleDurationSeconds"],
        )
        .and_then(|value| value.trim().parse::<u64>().ok());
        let mut redaction_values = Vec::new();
        if let Some(endpoint) = endpoint.as_deref() {
            collect_url_auth(endpoint, &mut redaction_values);
        }
        if let Some(credentials) = credentials.as_ref() {
            push_sensitive(&mut redaction_values, Some(&credentials.access_key_id));
            push_sensitive(&mut redaction_values, Some(&credentials.secret_access_key));
            push_sensitive(&mut redaction_values, credentials.session_token.as_deref());
        }
        Ok(Self {
            region,
            endpoint,
            profile,
            credentials,
            role_arn,
            role_session_name,
            external_id,
            session_duration_secs,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        let endpoint = self.endpoint.as_deref().unwrap_or_default();
        self.redaction_values.iter().fold(
            message.replace(endpoint, "<dynamodb-endpoint>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

async fn probe_connection(connection: &DynamoConnection) -> Result<usize, String> {
    let response = connection
        .client
        .list_tables()
        .limit(1)
        .send()
        .await
        .map_err(|err| format!("DynamoDB ListTables failed: {err}"))?;
    Ok(response.table_names().len())
}

async fn run_statement(
    connection: &DynamoConnection,
    statement: &str,
    parameters: Vec<AttributeValue>,
    cap: usize,
    consistent_read: bool,
) -> Result<QueryOutput, String> {
    let mut rows_json = Vec::new();
    let mut next_token = None;
    loop {
        let mut builder = connection
            .client
            .execute_statement()
            .statement(statement)
            .consistent_read(consistent_read);
        if !parameters.is_empty() {
            builder = builder.set_parameters(Some(parameters.clone()));
        }
        if let Some(token) = next_token.take() {
            builder = builder.next_token(token);
        }
        let remaining = cap.saturating_sub(rows_json.len()).max(1);
        builder = builder.limit(remaining.min(1000) as i32);
        let response = builder
            .send()
            .await
            .map_err(|err| format!("DynamoDB ExecuteStatement failed: {err}"))?;
        rows_json.extend(response.items().iter().map(item_to_json));
        next_token = response.next_token().map(str::to_owned);
        if rows_json.len() >= cap || next_token.is_none() {
            break;
        }
    }
    let truncated = rows_json.len() > cap || next_token.is_some();
    rows_json.truncate(cap);
    Ok(rows_to_output(rows_json, truncated))
}

async fn load_metadata(connection: &DynamoConnection) -> Result<Value, String> {
    let mut table_names = Vec::new();
    let mut exclusive_start_table_name = None;
    loop {
        let mut builder = connection.client.list_tables();
        if let Some(name) = exclusive_start_table_name.take() {
            builder = builder.exclusive_start_table_name(name);
        }
        let response = builder
            .send()
            .await
            .map_err(|err| format!("DynamoDB ListTables failed: {err}"))?;
        table_names.extend(response.table_names().iter().map(ToOwned::to_owned));
        exclusive_start_table_name = response.last_evaluated_table_name().map(str::to_owned);
        if exclusive_start_table_name.is_none() {
            break;
        }
    }

    let mut objects = Vec::new();
    for table_name in table_names {
        let response = connection
            .client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
            .map_err(|err| format!("DynamoDB DescribeTable {table_name} failed: {err}"))?;
        if let Some(table) = response.table() {
            objects.push(table_to_metadata(table));
        }
    }
    Ok(json!({
        "schemas": [{
            "name": connection.config.region,
            "objects": objects
        }]
    }))
}

fn table_to_metadata(table: &aws_sdk_dynamodb::types::TableDescription) -> Value {
    let attributes = table
        .attribute_definitions()
        .iter()
        .map(attribute_to_column)
        .collect::<Vec<_>>();
    let primary_key = key_schema_names(table.key_schema());
    let indexes = table
        .global_secondary_indexes()
        .iter()
        .map(global_index_to_json)
        .chain(
            table
                .local_secondary_indexes()
                .iter()
                .map(local_index_to_json),
        )
        .collect::<Vec<_>>();
    json!({
        "schema": "default",
        "name": table.table_name().unwrap_or(""),
        "kind": "table",
        "columns": attributes,
        "indexes": indexes,
        "primaryKey": primary_key,
        "foreignKeys": [],
        "itemCount": table.item_count(),
        "sizeBytes": table.table_size_bytes(),
        "status": table.table_status().map(|status| status.as_str()).unwrap_or("unknown")
    })
}

fn attribute_to_column(attribute: &AttributeDefinition) -> Value {
    json!({
        "name": attribute.attribute_name(),
        "dataType": attribute.attribute_type().as_str(),
        "nullable": true
    })
}

fn global_index_to_json(index: &GlobalSecondaryIndexDescription) -> Value {
    json!({
        "name": index.index_name().unwrap_or(""),
        "kind": "globalSecondaryIndex",
        "keySchema": key_schema_names(index.key_schema()),
        "projection": projection_to_json(index.projection()),
        "itemCount": index.item_count(),
        "sizeBytes": index.index_size_bytes(),
        "status": index.index_status().map(|status| status.as_str()).unwrap_or("unknown")
    })
}

fn local_index_to_json(index: &LocalSecondaryIndexDescription) -> Value {
    json!({
        "name": index.index_name().unwrap_or(""),
        "kind": "localSecondaryIndex",
        "keySchema": key_schema_names(index.key_schema()),
        "projection": projection_to_json(index.projection()),
        "itemCount": index.item_count(),
        "sizeBytes": index.index_size_bytes()
    })
}

fn projection_to_json(projection: Option<&Projection>) -> Value {
    match projection {
        Some(projection) => json!({
            "type": projection
                .projection_type()
                .map(|projection_type| projection_type.as_str())
                .unwrap_or("unknown"),
            "nonKeyAttributes": projection.non_key_attributes()
        }),
        None => Value::Null,
    }
}

fn key_schema_names(schema: &[KeySchemaElement]) -> Vec<Value> {
    schema
        .iter()
        .map(|key| {
            json!({
                "name": key.attribute_name(),
                "keyType": key.key_type().as_str()
            })
        })
        .collect()
}

fn rows_to_output(rows_json: Vec<Value>, truncated: bool) -> QueryOutput {
    let mut columns = Vec::new();
    for row in &rows_json {
        if let Some(object) = row.as_object() {
            for key in object.keys() {
                if !columns.iter().any(|column| column == key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    let rows = rows_json
        .iter()
        .map(|row| {
            if let Some(object) = row.as_object() {
                columns
                    .iter()
                    .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
                vec![row.clone()]
            }
        })
        .collect::<Vec<_>>();
    if columns.is_empty() && !rows_json.is_empty() {
        (vec!["value".to_string()], rows, truncated)
    } else {
        (columns, rows, truncated)
    }
}

fn item_to_json(item: &HashMap<String, AttributeValue>) -> Value {
    let object = item
        .iter()
        .map(|(key, value)| (key.clone(), attribute_value_to_json(value)))
        .collect::<Map<_, _>>();
    Value::Object(object)
}

fn attribute_value_to_json(value: &AttributeValue) -> Value {
    match value {
        AttributeValue::S(value) => Value::String(value.clone()),
        AttributeValue::N(value) => number_string_to_json(value),
        AttributeValue::B(value) => Value::String(BASE64.encode(value.as_ref())),
        AttributeValue::Bool(value) => Value::Bool(*value),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::M(value) => Value::Object(
            value
                .iter()
                .map(|(key, value)| (key.clone(), attribute_value_to_json(value)))
                .collect(),
        ),
        AttributeValue::L(value) => {
            Value::Array(value.iter().map(attribute_value_to_json).collect())
        }
        AttributeValue::Ss(value) => {
            Value::Array(value.iter().cloned().map(Value::String).collect())
        }
        AttributeValue::Ns(value) => Value::Array(
            value
                .iter()
                .map(|value| number_string_to_json(value))
                .collect(),
        ),
        AttributeValue::Bs(value) => Value::Array(
            value
                .iter()
                .map(|value| Value::String(BASE64.encode(value.as_ref())))
                .collect(),
        ),
        _ => Value::Null,
    }
}

fn number_string_to_json(value: &str) -> Value {
    serde_json::from_str::<Number>(value)
        .map(Value::Number)
        .unwrap_or_else(|_| Value::String(value.to_string()))
}

fn json_to_attribute_value(value: &Value) -> Result<AttributeValue, String> {
    match value {
        Value::Null => Ok(AttributeValue::Null(true)),
        Value::Bool(value) => Ok(AttributeValue::Bool(*value)),
        Value::Number(value) => Ok(AttributeValue::N(value.to_string())),
        Value::String(value) => Ok(AttributeValue::S(value.clone())),
        Value::Array(values) => Ok(AttributeValue::L(
            values
                .iter()
                .map(json_to_attribute_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(values) => Ok(AttributeValue::M(
            values
                .iter()
                .map(|(key, value)| {
                    json_to_attribute_value(value).map(|value| (key.clone(), value))
                })
                .collect::<Result<HashMap<_, _>, _>>()?,
        )),
    }
}

fn connection(connection_id: &str) -> Result<DynamoConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
            })
        })
}

fn bool_option(request: &Value, fields: &[&str]) -> Option<bool> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| container.get(*field).and_then(Value::as_bool))
        })
}

/// The desktop connection form gives this engine two credential boxes labelled
/// "AWS profile / access key" and "Secret / session token", so a profile filled
/// in through the UI arrives with `user`/`password` rather than the explicit
/// option names. `password` is unambiguous — it is the secret access key.
/// `user` is not, so disambiguate it by shape: an access key id is 20 uppercase
/// alphanumerics beginning with `A` (`AKIA…` long-term, `ASIA…` temporary).
/// Anything else is a profile name.
fn looks_like_access_key_id(value: &str) -> bool {
    value.len() == 20
        && value.starts_with('A')
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

fn form_access_key_id(request: &Value) -> Option<String> {
    option_string(request, &["user", "username"]).filter(|value| looks_like_access_key_id(value))
}

fn form_profile_name(request: &Value) -> Option<String> {
    option_string(request, &["user", "username"]).filter(|value| !looks_like_access_key_id(value))
}

fn credentials_from_request(request: &Value) -> Option<DynamoCredentials> {
    let access_key_id = option_string(
        request,
        &["accessKeyId", "accessKey", "awsAccessKeyId", "awsAccessKey"],
    )
    .or_else(|| form_access_key_id(request))?;
    let secret_access_key = option_string(
        request,
        &[
            "secretAccessKey",
            "secretKey",
            "awsSecretAccessKey",
            "awsSecretKey",
        ],
    )
    .or_else(|| option_string(request, &["password"]))?;
    let session_token = option_string(
        request,
        &["sessionToken", "token", "awsSessionToken", "securityToken"],
    );
    Some(DynamoCredentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

fn env_credentials() -> Option<DynamoCredentials> {
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
    Some(DynamoCredentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

fn profile_region(request: &Value) -> Option<String> {
    let profile = option_string(request, &["profile", "awsProfile"])
        .or_else(|| form_profile_name(request))
        .or_else(|| std::env::var("AWS_PROFILE").ok())
        .unwrap_or_else(|| "default".to_string());
    let config = std::env::var("AWS_CONFIG_FILE").ok().or_else(|| {
        std::env::var("HOME")
            .ok()
            .map(|home| format!("{home}/.aws/config"))
    })?;
    read_aws_ini_value(&config, &profile_section_name(&profile), "region")
        .or_else(|| read_aws_ini_value(&config, "default", "region"))
}

fn read_aws_ini_value(path: &str, section: &str, key: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut current_section = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(section_name) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            current_section = section_name.trim().to_string();
            continue;
        }
        if current_section == section {
            let Some((name, value)) = line.split_once('=') else {
                continue;
            };
            if name.trim() == key {
                let value = value.trim();
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

fn profile_section_name(profile: &str) -> String {
    if profile == "default" {
        "default".to_string()
    } else {
        format!("profile {profile}")
    }
}

fn normalize_endpoint(value: &str, region: &str) -> String {
    let value = value.trim();
    if value.contains("://") {
        value.trim_end_matches('/').to_string()
    } else if value.is_empty() {
        format!("https://dynamodb.{region}.amazonaws.com")
    } else {
        format!("https://{}", value.trim_end_matches('/'))
    }
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

fn collect_url_auth(url: &str, values: &mut Vec<String>) {
    let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) else {
        return;
    };
    let Some(auth) = after_scheme
        .split('/')
        .next()
        .and_then(|host| host.split('@').next())
    else {
        return;
    };
    if auth.contains(':') {
        for part in auth.split(':') {
            push_sensitive(values, Some(part));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_attribute_values_to_json_rows() {
        let item = HashMap::from([
            ("id".to_string(), AttributeValue::S("a".to_string())),
            ("count".to_string(), AttributeValue::N("42".to_string())),
            ("active".to_string(), AttributeValue::Bool(true)),
        ]);
        let row = item_to_json(&item);
        assert_eq!(row["id"], "a");
        assert_eq!(row["count"], 42);
        assert_eq!(row["active"], true);
    }

    #[test]
    fn parses_request_config_from_profile_and_secrets() {
        let request = json!({
            "profile": {
                "id": "local",
                "region": "us-west-2",
                "endpoint": "http://localhost:8000",
                "secrets": {
                    "accessKeyId": "key",
                    "secretAccessKey": "secret"
                }
            }
        });
        let config = DynamoConfig::from_request(&request).unwrap();
        assert_eq!(config.region, "us-west-2");
        assert_eq!(config.endpoint.as_deref(), Some("http://localhost:8000"));
        assert_eq!(
            config
                .credentials
                .as_ref()
                .map(|creds| creds.access_key_id.as_str()),
            Some("key")
        );
    }

    #[test]
    fn converts_json_parameters_to_attribute_values() {
        let value = json!({
            "id": "a",
            "count": 3,
            "tags": ["x", "y"]
        });
        let AttributeValue::M(map) = json_to_attribute_value(&value).unwrap() else {
            panic!("expected map");
        };
        assert!(matches!(map.get("id"), Some(AttributeValue::S(value)) if value == "a"));
        assert!(matches!(map.get("count"), Some(AttributeValue::N(value)) if value == "3"));
        assert!(matches!(map.get("tags"), Some(AttributeValue::L(values)) if values.len() == 2));
    }

    #[test]
    fn takes_the_secret_access_key_from_the_password_field() {
        // The connection form labels `user`/`password` "AWS profile / access
        // key" and "Secret / session token", so this is the shape a profile
        // filled in through the UI arrives as.
        let credentials = credentials_from_request(&json!({
            "profile": {
                "user": "AKIAIOSFODNN7EXAMPLE",
                "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            }
        }))
        .expect("form credentials should resolve");
        assert_eq!(credentials.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(
            credentials.secret_access_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
    }

    #[test]
    fn a_user_that_is_not_an_access_key_id_is_a_profile_name_not_a_credential() {
        assert!(credentials_from_request(&json!({
            "profile": { "user": "staging", "password": "secret" }
        }))
        .is_none());
        assert_eq!(
            form_profile_name(&json!({ "profile": { "user": "staging" } })).as_deref(),
            Some("staging")
        );
        assert_eq!(
            form_profile_name(&json!({ "profile": { "user": "AKIAIOSFODNN7EXAMPLE" } })),
            None
        );
    }

    #[test]
    fn explicit_credential_options_win_over_the_form_fields() {
        let credentials = credentials_from_request(&json!({
            "profile": {
                "user": "AKIAIOSFODNN7EXAMPLE",
                "password": "from-the-form",
                "options": {
                    "accessKeyId": "AKIAEXPLICITEXPLICIT",
                    "secretAccessKey": "explicit-secret"
                }
            }
        }))
        .expect("explicit credentials should resolve");
        assert_eq!(credentials.access_key_id, "AKIAEXPLICITEXPLICIT");
        assert_eq!(credentials.secret_access_key, "explicit-secret");
    }

    #[test]
    fn recognizes_the_access_key_id_shape() {
        assert!(looks_like_access_key_id("AKIAIOSFODNN7EXAMPLE"));
        assert!(looks_like_access_key_id("ASIAIOSFODNN7EXAMPLE"));
        assert!(!looks_like_access_key_id("default"));
        assert!(!looks_like_access_key_id("akiaiosfodnn7example"));
        assert!(!looks_like_access_key_id("AKIASHORT"));
    }

    #[test]
    fn reads_the_assume_role_options() {
        let config = DynamoConfig::from_request(&json!({
            "profile": {
                "region": "ap-northeast-1",
                "options": {
                    "roleArn": "arn:aws:iam::123456789012:role/analytics",
                    "roleSessionName": "irodori",
                    "externalId": "shared-secret",
                    "sessionDurationSeconds": "3600"
                }
            }
        }))
        .unwrap();
        assert_eq!(
            config.role_arn.as_deref(),
            Some("arn:aws:iam::123456789012:role/analytics")
        );
        assert_eq!(config.role_session_name.as_deref(), Some("irodori"));
        assert_eq!(config.external_id.as_deref(), Some("shared-secret"));
        assert_eq!(config.session_duration_secs, Some(3600));
    }

    #[test]
    fn a_profile_without_a_role_arn_assumes_nothing() {
        let config = DynamoConfig::from_request(&json!({
            "profile": { "region": "ap-northeast-1" }
        }))
        .unwrap();
        assert_eq!(config.role_arn, None);
        assert_eq!(config.session_duration_secs, None);
    }

    #[test]
    fn a_non_numeric_session_duration_is_ignored_rather_than_fatal() {
        // The SDK's own default (1 hour) is a better outcome than refusing to
        // connect over a malformed optional field.
        let config = DynamoConfig::from_request(&json!({
            "profile": {
                "region": "ap-northeast-1",
                "options": { "roleArn": "arn:aws:iam::1:role/r", "sessionDurationSeconds": "an hour" }
            }
        }))
        .unwrap();
        assert_eq!(config.role_arn.as_deref(), Some("arn:aws:iam::1:role/r"));
        assert_eq!(config.session_duration_secs, None);
    }
}
