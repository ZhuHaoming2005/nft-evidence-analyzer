use std::borrow::Cow;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::{Client, Config, IsolationLevel, NoTls};
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

type AnyError = Box<dyn Error + Send + Sync>;
type Result<T> = std::result::Result<T, AnyError>;

const SNAPSHOT_COLUMNS: [&str; 11] = [
    "chain",
    "contract_address",
    "token_id",
    "token_uri",
    "image_uri",
    "name",
    "symbol",
    "metadata_json",
    "token_uri_norm",
    "image_uri_norm",
    "name_norm",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Chain {
    Ethereum,
    Base,
    Polygon,
    Solana,
}

impl Chain {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::Base => "base",
            Self::Polygon => "polygon",
            Self::Solana => "solana",
        }
    }

    fn is_solana(self) -> bool {
        self == Self::Solana
    }

    fn table(self) -> String {
        format!("nft_assets_{}", self.as_str())
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "nft-snapshot-export",
    version,
    about = "Export one NFT PostgreSQL table to an analysis/dedup-compatible Parquet snapshot"
)]
struct Args {
    /// Chain whose nft_assets_<chain> table is exported.
    #[arg(long, value_enum)]
    chain: Chain,

    /// Destination Parquet file. Existing files require --force.
    #[arg(long)]
    output: PathBuf,

    /// Rows buffered per Arrow write.
    #[arg(long, default_value_t = 100_000, value_parser = parse_positive_usize)]
    fetch_size: usize,

    /// Inclusive first_seen_block lower bound (EVM only).
    #[arg(long)]
    start_block: Option<i64>,

    /// Inclusive first_seen_block upper bound (EVM only).
    #[arg(long)]
    end_block: Option<i64>,

    /// Replace an existing output only after the new snapshot is complete.
    #[arg(long)]
    force: bool,

    /// PostgreSQL connection string. Prefer DATABASE_URL to avoid shell history exposure.
    #[arg(long, hide = true)]
    database_url: Option<String>,
}

fn parse_positive_usize(raw: &str) -> std::result::Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|error| format!("invalid positive integer: {error}"))?;
    if value == 0 {
        return Err("value must be greater than zero".to_owned());
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BlockRange {
    start: Option<i64>,
    end: Option<i64>,
}

impl BlockRange {
    fn new(start: Option<i64>, end: Option<i64>, chain: Chain) -> Result<Self> {
        if start.is_some_and(|value| value < 0) || end.is_some_and(|value| value < 0) {
            return Err("snapshot block bounds must be non-negative".into());
        }
        if matches!((start, end), (Some(start), Some(end)) if start > end) {
            return Err("snapshot start block must not exceed end block".into());
        }
        if chain.is_solana() && (start.is_some() || end.is_some()) {
            return Err("block filtering is not available for the Solana snapshot".into());
        }
        Ok(Self { start, end })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SnapshotRow {
    contract_address: String,
    token_id: String,
    token_uri: String,
    image_uri: String,
    name: String,
    symbol: String,
    metadata_json: String,
}

fn snapshot_schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        SNAPSHOT_COLUMNS
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ))
}

fn trailing_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            r"\s*#\s*[0-9a-fA-FxX]+\s*$",
            r"\s*#\s*\d+\s*$",
            r"\s*-\s*\d+\s*$",
            r"\s*:\s*\d+\s*$",
            r"\s*\(\s*\d+\s*\)\s*$",
            r"\s*\[\s*\d+\s*\]\s*$",
            r"\s*/\s*\d+\s*$",
            r"\s+No\.?\s*\d+\s*$",
            r"\s+nr\.?\s*\d+\s*$",
            r"\s+\d{1,12}\s*$",
        ]
        .into_iter()
        .map(|pattern| Regex::new(pattern).expect("static name regex"))
        .collect()
    })
}

fn whitespace_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"\s+").expect("static whitespace regex"))
}

fn ipfs_http_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)^https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)").expect("static IPFS regex")
    })
}

fn arweave_http_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)^https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)")
            .expect("static Arweave regex")
    })
}

fn normalize_name(raw: &str) -> String {
    let mut value = raw.nfkc().collect::<String>().trim().to_owned();
    for _ in 0..20 {
        let mut changed = false;
        for pattern in trailing_patterns() {
            let updated = pattern.replace(&value, "").trim().to_owned();
            if updated != value {
                value = updated;
                changed = true;
                break;
            }
        }
        if !changed {
            break;
        }
    }
    whitespace_regex()
        .replace_all(&value, " ")
        .trim()
        .to_lowercase()
}

fn decentralized_path(raw: &str, prefix_len: usize, prefix: &str) -> Option<String> {
    let value = raw[prefix_len..]
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .trim_matches('/');
    (!value.is_empty()).then(|| format!("{prefix}:{value}"))
}

fn normalize_url(raw: &str) -> String {
    let value = raw.trim();
    if value.is_empty() {
        return String::new();
    }
    let lowered = value.to_lowercase();
    if matches!(
        lowered.as_str(),
        "nano" | "null" | "none" | "undefined" | "n/a" | "na" | "-" | "." | "false" | "true" | "0"
    ) || lowered.starts_with("data:")
    {
        return String::new();
    }
    if lowered.starts_with("ipfs://") {
        let mut tail = &value[7..];
        if tail
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ipfs/"))
        {
            tail = &tail[5..];
        }
        return decentralized_path(tail, 0, "ipfs").unwrap_or_default();
    }
    if lowered.starts_with("ar://") {
        return decentralized_path(value, 5, "ar").unwrap_or_default();
    }
    if let Some(captures) = ipfs_http_regex().captures(value) {
        let path = captures.get(1).map(|part| part.as_str()).unwrap_or("");
        return decentralized_path(path, 0, "ipfs").unwrap_or_default();
    }
    if let Some(captures) = arweave_http_regex().captures(value) {
        let path = captures.get(1).map(|part| part.as_str()).unwrap_or("");
        return decentralized_path(path, 0, "ar").unwrap_or_default();
    }
    lowered.trim_end_matches('/').to_owned()
}

fn snapshot_batch(chain: Chain, rows: &[SnapshotRow]) -> Result<RecordBatch> {
    let chain_name = chain.as_str();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            chain_name,
            rows.len(),
        ))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| {
            let address = row.contract_address.trim();
            if chain.is_solana() {
                Cow::Borrowed(address)
            } else {
                Cow::Owned(address.to_lowercase())
            }
        }))),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.token_id.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.token_uri.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.image_uri.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.name.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.symbol.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.metadata_json.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| normalize_url(&row.token_uri)),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| normalize_url(&row.image_uri)),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| normalize_name(&row.name)),
        )),
    ];
    RecordBatch::try_new(snapshot_schema(), columns).map_err(Into::into)
}

fn writer(path: &Path, fetch_size: usize) -> Result<ArrowWriter<File>> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(fetch_size)
        .build();
    ArrowWriter::try_new(File::create(path)?, snapshot_schema(), Some(properties))
        .map_err(Into::into)
}

#[cfg(test)]
fn write_rows(path: &Path, chain: Chain, rows: &[SnapshotRow]) -> Result<()> {
    let mut output = writer(path, rows.len().max(1))?;
    if !rows.is_empty() {
        output.write(&snapshot_batch(chain, rows)?)?;
    }
    output.close()?;
    Ok(())
}

#[derive(Debug)]
struct SnapshotQuery {
    sql: String,
    params: Vec<i64>,
}

fn build_query(table: &str, metadata_column: &str, range: BlockRange) -> SnapshotQuery {
    let mut predicates = Vec::new();
    let mut params = Vec::new();
    if let Some(start) = range.start {
        params.push(start);
        predicates.push(format!("first_seen_block >= ${}", params.len()));
    }
    if let Some(end) = range.end {
        params.push(end);
        predicates.push(format!("first_seen_block <= ${}", params.len()));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    SnapshotQuery {
        sql: format!(
            "SELECT contract_address, token_id::text, COALESCE(token_uri, ''), \
             COALESCE(image_uri, ''), COALESCE(name, ''), COALESCE(symbol, ''), \
             COALESCE({metadata_column}::text, '') \
             FROM {table}{where_clause} ORDER BY id"
        ),
        params,
    }
}

fn database_config(explicit_url: Option<&str>) -> Result<Config> {
    let url = explicit_url
        .map(str::to_owned)
        .or_else(|| env::var("DATABASE_URL").ok());
    if let Some(url) = url {
        return Config::from_str(&url).map_err(Into::into);
    }
    let mut config = Config::new();
    config
        .host(&env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_owned()))
        .port(
            env::var("DB_PORT")
                .unwrap_or_else(|_| "5432".to_owned())
                .parse()?,
        )
        .dbname(&env::var("DB_NAME").unwrap_or_else(|_| "nft_data".to_owned()))
        .user(&env::var("DB_USER").unwrap_or_else(|_| "postgres".to_owned()))
        .connect_timeout(std::time::Duration::from_secs(
            env::var("DB_CONNECT_TIMEOUT")
                .unwrap_or_else(|_| "10".to_owned())
                .parse()?,
        ));
    if let Ok(password) = env::var("DB_PASS") {
        config.password(password);
    }
    Ok(config)
}

fn metadata_column(transaction: &mut postgres::Transaction<'_>, table: &str) -> Result<String> {
    let row = transaction.query_opt(
        "SELECT attname FROM pg_catalog.pg_attribute \
         WHERE attrelid = pg_catalog.to_regclass($1) \
         AND attnum > 0 AND NOT attisdropped \
         AND attname IN ('raw_metadata', 'metadata') \
         ORDER BY CASE WHEN attname = 'raw_metadata' THEN 0 ELSE 1 END LIMIT 1",
        &[&table],
    )?;
    row.map(|value| value.get::<_, String>(0))
        .ok_or_else(|| format!("table {table} has neither metadata nor raw_metadata column").into())
}

fn export_database_snapshot(
    client: &mut Client,
    chain: Chain,
    output_path: &Path,
    fetch_size: usize,
    range: BlockRange,
) -> Result<u64> {
    let table = chain.table();
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()?;
    let metadata = metadata_column(&mut transaction, &table)?;
    let query = build_query(&table, &metadata, range);
    let mut output = writer(output_path, fetch_size)?;
    let mut total = 0_u64;
    {
        let params = query
            .params
            .iter()
            .map(|value| value as &(dyn ToSql + Sync));
        let mut database_rows = transaction.query_raw(&query.sql, params)?;
        let mut buffer = Vec::with_capacity(fetch_size);
        while let Some(row) = database_rows.next()? {
            buffer.push(SnapshotRow {
                contract_address: row.get(0),
                token_id: row.get(1),
                token_uri: row.get(2),
                image_uri: row.get(3),
                name: row.get(4),
                symbol: row.get(5),
                metadata_json: row.get(6),
            });
            if buffer.len() == fetch_size {
                output.write(&snapshot_batch(chain, &buffer)?)?;
                total += buffer.len() as u64;
                eprintln!("exported {total} rows");
                buffer.clear();
            }
        }
        if !buffer.is_empty() {
            output.write(&snapshot_batch(chain, &buffer)?)?;
            total += buffer.len() as u64;
        }
    }
    output.close()?;
    transaction.commit()?;
    Ok(total)
}

fn sibling_path(path: &Path, label: &str) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or("output must have a valid file name")?;
    Ok(path.with_file_name(format!(".{file_name}.{label}.{}", std::process::id())))
}

fn publish(temp: &Path, output: &Path, force: bool) -> Result<()> {
    if !output.exists() {
        fs::rename(temp, output)?;
        return Ok(());
    }
    if !force {
        return Err(format!(
            "{} already exists; pass --force to replace it",
            output.display()
        )
        .into());
    }
    let backup = sibling_path(output, "previous")?;
    if backup.exists() {
        fs::remove_file(&backup)?;
    }
    fs::rename(output, &backup)?;
    if let Err(error) = fs::rename(temp, output) {
        let _ = fs::rename(&backup, output);
        return Err(error.into());
    }
    fs::remove_file(backup)?;
    Ok(())
}

fn run(args: Args) -> Result<()> {
    let range = BlockRange::new(args.start_block, args.end_block, args.chain)?;
    if let Some(parent) = args
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    if args.output.exists() && !args.force {
        return Err(format!(
            "{} already exists; pass --force to replace it",
            args.output.display()
        )
        .into());
    }
    let temp = sibling_path(&args.output, "partial")?;
    if temp.exists() {
        fs::remove_file(&temp)?;
    }
    let started = Instant::now();
    let result = (|| {
        let config = database_config(args.database_url.as_deref())?;
        let mut client = config.connect(NoTls)?;
        let rows =
            export_database_snapshot(&mut client, args.chain, &temp, args.fetch_size, range)?;
        publish(&temp, &args.output, args.force)?;
        eprintln!(
            "completed chain={} rows={} output={} elapsed={:.1}s",
            args.chain.as_str(),
            rows,
            args.output.display(),
            started.elapsed().as_secs_f64()
        );
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

fn main() -> Result<()> {
    run(Args::parse())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn normalization_matches_snapshot_contract() {
        assert_eq!(normalize_name("  Pokémon #0007 "), "pokémon");
        assert_eq!(
            normalize_url("ipfs://ipfs/QmExample/path/?x=1"),
            "ipfs:QmExample/path"
        );
        assert_eq!(
            normalize_url("https://gateway.example/ipfs/QmExample/path#part"),
            "ipfs:QmExample/path"
        );
        assert_eq!(normalize_url("DATA:image/png;base64,AAAA"), "");
        assert_eq!(
            normalize_url("HTTPS://EXAMPLE.COM/NFT/"),
            "https://example.com/nft"
        );
    }

    #[test]
    fn block_ranges_are_validated() {
        assert!(BlockRange::new(Some(-1), None, Chain::Ethereum).is_err());
        assert!(BlockRange::new(Some(20), Some(10), Chain::Base).is_err());
        assert!(BlockRange::new(Some(1), None, Chain::Solana).is_err());
        assert_eq!(
            BlockRange::new(Some(10), Some(20), Chain::Polygon).unwrap(),
            BlockRange {
                start: Some(10),
                end: Some(20)
            }
        );
    }

    #[test]
    fn query_uses_safe_table_and_inclusive_bounds() {
        let query = build_query(
            &Chain::Ethereum.table(),
            "metadata",
            BlockRange {
                start: Some(10),
                end: Some(20),
            },
        );
        assert!(query.sql.contains("FROM nft_assets_ethereum"));
        assert!(query.sql.contains("first_seen_block >= $1"));
        assert!(query.sql.contains("first_seen_block <= $2"));
        assert_eq!(query.params, vec![10, 20]);
    }

    #[test]
    #[ignore = "requires NFT_EXPORT_TEST_DATABASE_URL pointing to a test PostgreSQL database"]
    fn metadata_column_follows_search_path() {
        let url = env::var("NFT_EXPORT_TEST_DATABASE_URL")
            .expect("set NFT_EXPORT_TEST_DATABASE_URL to a test PostgreSQL database");
        let mut client = Client::connect(&url, NoTls).unwrap();
        let mut transaction = client.transaction().unwrap();
        transaction
            .batch_execute(
                "CREATE TEMP TABLE nft_export_metadata_lookup \
                 (metadata text, raw_metadata text) ON COMMIT DROP; \
                 SET LOCAL search_path = pg_catalog, pg_temp;",
            )
            .unwrap();
        let schema: String = transaction
            .query_one("SELECT current_schema()::text", &[])
            .unwrap()
            .get(0);
        assert_eq!(schema, "pg_catalog");
        assert_eq!(
            metadata_column(&mut transaction, "nft_export_metadata_lookup").unwrap(),
            "raw_metadata"
        );
        transaction
            .batch_execute("ALTER TABLE nft_export_metadata_lookup DROP COLUMN raw_metadata")
            .unwrap();
        assert_eq!(
            metadata_column(&mut transaction, "nft_export_metadata_lookup").unwrap(),
            "metadata"
        );
        transaction
            .batch_execute("ALTER TABLE nft_export_metadata_lookup DROP COLUMN metadata")
            .unwrap();
        assert!(metadata_column(&mut transaction, "nft_export_metadata_lookup").is_err());
        transaction.rollback().unwrap();
    }

    #[test]
    fn parquet_has_full_consumer_compatible_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ethereum.parquet");
        write_rows(
            &path,
            Chain::Ethereum,
            &[SnapshotRow {
                contract_address: "0xAbC".into(),
                token_id: "0007".into(),
                token_uri: "ipfs://QmToken".into(),
                image_uri: "https://gateway.example/ipfs/QmImage".into(),
                name: "Example #7".into(),
                symbol: "EX".into(),
                metadata_json: r#"{"name":"Example #7"}"#.into(),
            }],
        )
        .unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        let names = builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, SNAPSHOT_COLUMNS);
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let strings = |index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
        };
        assert_eq!(strings(0), "ethereum");
        assert_eq!(strings(1), "0xabc");
        assert_eq!(strings(8), "ipfs:QmToken");
        assert_eq!(strings(9), "ipfs:QmImage");
        assert_eq!(strings(10), "example");
        assert!(!batch.column(7).is_null(0));
    }

    #[test]
    fn publish_refuses_or_replaces_existing_output() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("snapshot.parquet");
        let first = dir.path().join("first");
        fs::write(&output, b"old").unwrap();
        fs::write(&first, b"new").unwrap();
        assert!(publish(&first, &output, false).is_err());
        assert_eq!(fs::read(&output).unwrap(), b"old");

        let second = dir.path().join("second");
        fs::write(&second, b"new").unwrap();
        publish(&second, &output, true).unwrap();
        assert_eq!(fs::read(&output).unwrap(), b"new");
    }
}
