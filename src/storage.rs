use crate::{
    config::{Config, Fingerprint, Output, S3Config},
    error::{Error, Result},
    model::Input,
    recipe::digest,
};
use futures_util::StreamExt;
use object_store::{
    Attribute, Attributes, ObjectStore, ObjectStoreExt, PutOptions,
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey},
    path::Path as ObjectPath,
    signer::Signer,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
pub struct Storage {
    pub(crate) config: Arc<Config>,
    stores: BTreeMap<String, Arc<AmazonS3>>,
}
impl Storage {
    pub async fn new(config: Arc<Config>) -> Result<Self> {
        let mut stores = BTreeMap::new();
        for (name, c) in &config.stores {
            stores.insert(name.clone(), Arc::new(build_s3(c)?));
        }
        if let Output::Local { root } = &config.output {
            tokio::fs::create_dir_all(root).await?;
        }
        Ok(Self { config, stores })
    }
    pub async fn validate_input(&self, input: &Input) -> Result<Input> {
        match input {
            Input::Local { path } => {
                let canonical = tokio::fs::canonicalize(path)
                    .await
                    .map_err(|_| Error::Invalid("local input does not exist".into()))?;
                let mut allowed = false;
                for root in &self.config.inputs.local_roots {
                    if let Ok(root) = tokio::fs::canonicalize(root).await
                        && canonical.starts_with(root)
                    {
                        allowed = true;
                        break;
                    }
                }
                if !allowed || !tokio::fs::metadata(&canonical).await?.is_file() {
                    return Err(Error::Invalid(
                        "input must be a file within an allowed local root".into(),
                    ));
                }
                Ok(Input::Local { path: canonical })
            }
            Input::Http { url } => {
                self.http_client(url).await?;
                Ok(input.clone())
            }
            Input::S3 { store, key } => {
                if !self.stores.contains_key(store) {
                    return Err(Error::Invalid("unknown input store".into()));
                }
                object_path(key)?;
                Ok(input.clone())
            }
        }
    }
    async fn http_client(&self, raw: &str) -> Result<(reqwest::Client, url::Url)> {
        let url = url::Url::parse(raw).map_err(|_| Error::Invalid("invalid input URL".into()))?;
        let Some(host) = url.host_str() else {
            return Err(Error::Invalid("URL needs a host".into()));
        };
        if !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https"
                || (url.scheme() == "http" && self.config.inputs.allow_http))
        {
            return Err(Error::Invalid(
                "URL scheme or credentials are not permitted".into(),
            ));
        }
        if !self
            .config
            .inputs
            .http_hosts
            .iter()
            .any(|h| h.eq_ignore_ascii_case(host))
        {
            return Err(Error::Invalid(
                "URL host is not in inputs.http_hosts".into(),
            ));
        }
        let port = url
            .port_or_known_default()
            .ok_or_else(|| Error::Invalid("URL has no port".into()))?;
        let host_unbracketed = host.trim_start_matches('[').trim_end_matches(']');
        let addresses: Vec<SocketAddr> = tokio::time::timeout(
            Duration::from_secs(self.config.execution.io_timeout_seconds),
            tokio::net::lookup_host((host_unbracketed, port)),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Operation("DNS lookup failed".into()))?
        .collect();
        if addresses.is_empty()
            || (!self.config.inputs.allow_private_ips
                && addresses.iter().any(|a| !public_ip(a.ip())))
        {
            return Err(Error::Invalid(
                "URL resolves to a disallowed address".into(),
            ));
        }
        // Pin the checked DNS results and disable redirects/proxies to preserve the access policy.
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(
                self.config.execution.io_timeout_seconds,
            ))
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| Error::Operation("HTTP client initialization failed".into()))?;
        Ok((client, url))
    }
    pub async fn stage(&self, input: &Input, destination: &Path) -> Result<String> {
        let input = self.validate_input(input).await?;
        let max = self.config.execution.max_input_bytes;
        let mut output = tokio::fs::File::create(destination).await?;
        let mut hash = Sha256::new();
        let mut size = 0u64;
        let metadata_fingerprint = match &input {
            Input::Local { path } => {
                let mut f = tokio::fs::File::open(path).await?;
                let before = f.metadata().await?;
                if before.len() > max {
                    return Err(Error::Invalid("input exceeds max_input_bytes".into()));
                }
                let mut buffer = vec![0; 65536];
                loop {
                    let n = f.read(&mut buffer).await?;
                    if n == 0 {
                        break;
                    }
                    write_chunk(&mut output, &mut hash, &mut size, &buffer[..n], max).await?;
                }
                let after = f.metadata().await?;
                if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
                    return Err(Error::Operation(
                        "input changed while it was being read".into(),
                    ));
                }
                Some(format!(
                    "local:{}:{}:{:?}",
                    path.display(),
                    before.len(),
                    before.modified().ok()
                ))
            }
            Input::Http { url } => {
                let (client, url) = self.http_client(url).await?;
                let response = client
                    .get(url.clone())
                    .send()
                    .await
                    .map_err(|error| http_input_error(error, "HTTP input download failed"))?;
                if !response.status().is_success() {
                    return Err(Error::Operation(format!(
                        "HTTP input returned status {}",
                        response.status().as_u16()
                    )));
                }
                if response.content_length().is_some_and(|n| n > max) {
                    return Err(Error::Invalid("input exceeds max_input_bytes".into()));
                }
                let validator = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| !v.starts_with("W/"))
                    .map(|v| format!("http:{}:{v}", url.as_str()));
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk
                        .map_err(|error| http_input_error(error, "HTTP input stream failed"))?;
                    write_chunk(&mut output, &mut hash, &mut size, &chunk, max).await?;
                }
                validator
            }
            Input::S3 { store, key } => {
                let result = self.stores[store]
                    .get(&object_path(key)?)
                    .await
                    .map_err(|_| Error::Operation("S3 input download failed".into()))?;
                if result.meta.size > max {
                    return Err(Error::Invalid("input exceeds max_input_bytes".into()));
                }
                let validator = result
                    .meta
                    .version
                    .as_ref()
                    .or(result.meta.e_tag.as_ref())
                    .map(|v| {
                        let source = &self.config.stores[store];
                        format!(
                            "s3:{:?}:{}:{}:{key}:{v}:{}",
                            source.endpoint, source.region, source.bucket, result.meta.size
                        )
                    });
                let mut stream = result.into_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk =
                        chunk.map_err(|_| Error::Operation("S3 input stream failed".into()))?;
                    write_chunk(&mut output, &mut hash, &mut size, &chunk, max).await?;
                }
                validator
            }
        };
        output.flush().await?;
        if size == 0 {
            return Err(Error::Invalid("input is empty".into()));
        }
        Ok(if self.config.cache.fingerprint == Fingerprint::Metadata {
            metadata_fingerprint
                .map(digest)
                .unwrap_or_else(|| format!("{:x}", hash.finalize()))
        } else {
            format!("{:x}", hash.finalize())
        })
    }
    pub async fn cached(&self, key: &str) -> Result<Option<u64>> {
        validate_artifact_key(key)?;
        let (size, modified) = match &self.config.output {
            Output::Local { root } => match tokio::fs::metadata(root.join(key)).await {
                Ok(m) => (m.len(), m.modified().unwrap_or(SystemTime::UNIX_EPOCH)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            },
            Output::S3 { store, prefix, .. } => {
                match self.stores[store].head(&output_key(prefix, key)?).await {
                    Ok(m) => (m.size, SystemTime::from(m.last_modified)),
                    Err(object_store::Error::NotFound { .. }) => return Ok(None),
                    Err(_) => return Err(Error::Operation("artifact lookup failed".into())),
                }
            }
        };
        if size == 0
            || size > self.config.execution.max_output_bytes
            || self.config.cache.ttl_seconds.is_some_and(|ttl| {
                modified.elapsed().unwrap_or_default() > Duration::from_secs(ttl)
            })
        {
            return Ok(None);
        }
        Ok(Some(size))
    }
    pub async fn publish(&self, key: &str, path: &Path) -> Result<u64> {
        validate_artifact_key(key)?;
        let size = tokio::fs::metadata(path).await?.len();
        if size == 0 || size > self.config.execution.max_output_bytes {
            return Err(Error::Operation(
                "output size is outside configured limits".into(),
            ));
        }
        match &self.config.output {
            Output::Local { root } => {
                // Same-directory tempfile makes rename atomic, even when staging uses another disk.
                let tmp = tempfile::NamedTempFile::new_in(root)?;
                tokio::fs::copy(path, tmp.path()).await?;
                let destination = root.join(key);
                tokio::task::spawn_blocking(move || -> Result<()> {
                    tmp.as_file().sync_all()?;
                    tmp.persist(destination).map_err(|e| Error::Io(e.error))?;
                    Ok(())
                })
                .await
                .map_err(|_| Error::Operation("artifact publication worker stopped".into()))??;
            }
            Output::S3 { store, prefix, .. } => {
                let mut attributes = Attributes::new();
                attributes.insert(Attribute::ContentType, "image/gif".into());
                attributes.insert(
                    Attribute::CacheControl,
                    self.config.cache.cache_control.clone().into(),
                );
                let bytes = tokio::fs::read(path).await?;
                self.stores[store]
                    .put_opts(
                        &output_key(prefix, key)?,
                        bytes.into(),
                        PutOptions {
                            attributes,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|_| Error::Operation("artifact upload failed".into()))?;
            }
        }
        Ok(size)
    }
    pub async fn location(&self, key: &str) -> Result<ArtifactLocation> {
        validate_artifact_key(key)?;
        match &self.config.output {
            Output::Local { root } => {
                let path = root.join(key);
                if !tokio::fs::try_exists(&path).await? {
                    return Err(Error::NotFound);
                }
                Ok(ArtifactLocation::Local(path))
            }
            Output::S3 {
                store,
                prefix,
                signed_url_seconds,
            } => {
                let url = self.stores[store]
                    .signed_url(
                        http::Method::GET,
                        &output_key(prefix, key)?,
                        Duration::from_secs(*signed_url_seconds),
                    )
                    .await
                    .map_err(|_| Error::Operation("artifact signing failed".into()))?;
                Ok(ArtifactLocation::Redirect(url.to_string()))
            }
        }
    }
}
pub enum ArtifactLocation {
    Local(PathBuf),
    Redirect(String),
}
fn http_input_error(error: reqwest::Error, message: &'static str) -> Error {
    if error.is_timeout() {
        Error::Timeout
    } else {
        // Keep source URLs and signed query parameters out of public errors.
        Error::Operation(message.into())
    }
}
fn build_s3(c: &S3Config) -> Result<AmazonS3> {
    s3_builder(c, |key| std::env::var(key).ok())?
        .build()
        .map_err(|_| Error::Config("cannot initialize S3 store".into()))
}
fn s3_builder(c: &S3Config, env: impl Fn(&str) -> Option<String>) -> Result<AmazonS3Builder> {
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(&c.bucket)
        .with_region(&c.region)
        .with_virtual_hosted_style_request(c.virtual_hosted_style)
        .with_allow_http(c.allow_http);
    if let Some(endpoint) = &c.endpoint {
        b = b.with_endpoint(endpoint);
    }
    // Import identity sources only. AWS endpoint/bucket/region/HTTP variables
    // must not override the deployment's explicit storage/access configuration.
    for (name, key) in [
        (
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            AmazonS3ConfigKey::WebIdentityTokenFile,
        ),
        ("AWS_ROLE_ARN", AmazonS3ConfigKey::RoleArn),
        ("AWS_ROLE_SESSION_NAME", AmazonS3ConfigKey::RoleSessionName),
        ("AWS_ENDPOINT_URL_STS", AmazonS3ConfigKey::StsEndpoint),
        (
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
        ),
        (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            AmazonS3ConfigKey::ContainerCredentialsFullUri,
        ),
        (
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
        ),
    ] {
        if let Some(value) = env(name) {
            b = b.with_config(key, value);
        }
    }
    let access = env(&c.access_key_env);
    let secret = env(&c.secret_key_env);
    match (access, secret) {
        (Some(a), Some(s)) if !a.is_empty() && !s.is_empty() => {
            b = b.with_access_key_id(a).with_secret_access_key(s);
        }
        (None, None) => {}
        _ => {
            return Err(Error::Config(
                "S3 access and secret environment variables must both be set or both absent".into(),
            ));
        }
    }
    if let Some(token) = env(&c.session_token_env) {
        b = b.with_token(token);
    }
    Ok(b)
}
fn object_path(key: &str) -> Result<ObjectPath> {
    if key.is_empty() || key.starts_with('/') || key.split('/').any(|p| p == ".." || p == ".") {
        return Err(Error::Invalid("invalid object key".into()));
    }
    ObjectPath::parse(key).map_err(|_| Error::Invalid("invalid object key".into()))
}
fn output_key(prefix: &str, key: &str) -> Result<ObjectPath> {
    object_path(format!("{}/{}", prefix.trim_matches('/'), key).trim_start_matches('/'))
}
pub fn validate_artifact_key(key: &str) -> Result<()> {
    let Some(hash) = key.strip_suffix(".gif") else {
        return Err(Error::NotFound);
    };
    if hash.len() != 64 || !hash.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::NotFound);
    }
    Ok(())
}
async fn write_chunk(
    output: &mut tokio::fs::File,
    hash: &mut Sha256,
    size: &mut u64,
    bytes: &[u8],
    max: u64,
) -> Result<()> {
    *size = size
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| Error::Invalid("input too large".into()))?;
    if *size > max {
        return Err(Error::Invalid("input exceeds max_input_bytes".into()));
    }
    hash.update(bytes);
    output.write_all(bytes).await?;
    Ok(())
}
fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.octets()[0] == 0
                || ip.octets()[0] >= 240
                || (ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
                || (ip.octets()[0] == 198 && (18..=19).contains(&ip.octets()[1])))
        }
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                public_ip(IpAddr::V4(v4))
            } else {
                !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !ip.is_multicast()
                    && (ip.segments()[0] & 0xfe00) != 0xfc00
                    && (ip.segments()[0] & 0xffc0) != 0xfe80
                    && (ip.segments()[0] & 0xe000) == 0x2000
            }
        }
    }
}

#[cfg(test)]
mod http_input_tests {
    use super::*;

    async fn stalled_source(send_headers: bool) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/video?signature=private-fixture",
            listener.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0; 1024];
                let count = socket.read(&mut buffer).await.unwrap();
                if count == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|value| value == b"\r\n\r\n") {
                    break;
                }
            }
            if send_headers {
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\nx",
                    )
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        (url, server)
    }

    #[tokio::test]
    async fn request_and_body_timeouts_preserve_the_timeout_classification() {
        for send_headers in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = Config {
                output: Output::Local {
                    root: dir.path().join("out"),
                },
                ..Config::default()
            };
            config.execution.io_timeout_seconds = 1;
            config.inputs.http_hosts = vec!["127.0.0.1".into()];
            config.inputs.allow_http = true;
            config.inputs.allow_private_ips = true;
            let storage = Storage::new(Arc::new(config)).await.unwrap();
            let (url, server) = stalled_source(send_headers).await;
            // Exercise reqwest's own timeout directly, without the runner's
            // outer deadline, so neither header nor body errors can be masked.
            let result = tokio::time::timeout(
                Duration::from_secs(4),
                storage.stage(&Input::Http { url }, &dir.path().join("input")),
            )
            .await;
            server.abort();
            let _ = server.await;
            assert!(
                matches!(result, Ok(Err(Error::Timeout))),
                "headers_sent={send_headers}: {result:?}"
            );
        }
    }
}

#[cfg(test)]
mod credential_tests {
    use super::*;
    #[test]
    fn web_identity_and_ecs_sources_do_not_override_storage_configuration() {
        let c = S3Config {
            bucket: "explicit-bucket".into(),
            region: "eu-west-1".into(),
            endpoint: Some("https://storage.example".into()),
            ..Default::default()
        };
        let env = BTreeMap::from([
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/tokens/identity"),
            ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test"),
            ("AWS_ROLE_SESSION_NAME", "session-test"),
            ("AWS_ENDPOINT_URL_STS", "https://sts.example"),
            (
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "/v2/credentials/test",
            ),
            ("AWS_BUCKET", "ignored-bucket"),
            ("AWS_DEFAULT_REGION", "ignored-region"),
            ("AWS_ENDPOINT", "http://ignored.example"),
            ("AWS_ALLOW_HTTP", "true"),
        ]);
        let b = s3_builder(&c, |key| env.get(key).map(|v| v.to_string())).unwrap();
        for (key, expected) in [
            (AmazonS3ConfigKey::WebIdentityTokenFile, "/tokens/identity"),
            (
                AmazonS3ConfigKey::RoleArn,
                "arn:aws:iam::123456789012:role/test",
            ),
            (AmazonS3ConfigKey::RoleSessionName, "session-test"),
            (
                AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
                "/v2/credentials/test",
            ),
            (AmazonS3ConfigKey::Bucket, "explicit-bucket"),
            (AmazonS3ConfigKey::Region, "eu-west-1"),
            (AmazonS3ConfigKey::Endpoint, "https://storage.example"),
        ] {
            assert_eq!(b.get_config_value(&key).as_deref(), Some(expected));
        }
    }
    #[tokio::test]
    async fn container_identity_is_fetched_and_used_to_sign() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "test-identity-token").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}/credentials", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            loop {
                let mut chunk = [0; 1024];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if bytes.windows(4).any(|s| s == b"\r\n\r\n") {
                    break;
                }
                assert!(bytes.len() < 8192);
            }
            let request = String::from_utf8(bytes).unwrap().to_ascii_lowercase();
            assert!(request.starts_with("get /credentials "));
            assert!(request.contains("authorization: test-identity-token"));
            let body = r#"{"AccessKeyId":"container-access","SecretAccessKey":"test-secret","Token":"test-session","Expiration":"2099-01-01T00:00:00Z"}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        });
        let c = S3Config {
            bucket: "test-bucket".into(),
            endpoint: Some("https://storage.example".into()),
            ..Default::default()
        };
        let b = s3_builder(&c, |key| match key {
            "AWS_CONTAINER_CREDENTIALS_FULL_URI" => Some(uri.clone()),
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE" => Some(token.to_string_lossy().into_owned()),
            _ => None,
        })
        .unwrap()
        .build()
        .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            b.signed_url(
                http::Method::GET,
                &ObjectPath::from("test.gif"),
                Duration::from_secs(60),
            ),
        )
        .await;
        if result.is_err() {
            server.abort();
        }
        let url = result.unwrap().unwrap();
        assert!(url.query_pairs().any(
            |(key, value)| key == "X-Amz-Credential" && value.starts_with("container-access/")
        ));
        assert!(
            url.query_pairs()
                .any(|(key, value)| key == "X-Amz-Security-Token" && value == "test-session")
        );
        server.await.unwrap();
    }
}
