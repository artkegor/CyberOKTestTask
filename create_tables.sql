CREATE DATABASE IF NOT EXISTS scans;

-- Основная таблица сканов
CREATE TABLE IF NOT EXISTS scans.scan_results (
    scan_id String,
    ip String,
    port UInt16,
    protocol String,
    ssl_tls UInt8,
    used_probes Map(String, String),
    scan_tries UInt8,
    sended_probes UInt8,
    banners Map(String, String),
    timestamp UInt32,
    total_time_spent String,
    hex_banners Map(String, String),
    banners_hashes Map(String, Tuple(md5 String, sha256 String, simhash UInt64)),
    products_count UInt16,
    product_services Array(String)
) ENGINE = MergeTree()
ORDER BY (ip, port);

-- Таблица с продуктами
CREATE TABLE IF NOT EXISTS scans.products (
    scan_id String,
    probe String,
    service String,
    regex String,
    softmatch UInt8,
    vendorproductname Nullable(String),
    info Nullable(String),
    os Nullable(String),
    devicetype Nullable(String),
    hostname Nullable(String),
    cpe Array(String)
) ENGINE = MergeTree()
ORDER BY (scan_id, probe);