use assert_cmd::prelude::*;
use kvs::{KvStore, Result};
use predicates::ord::eq;
use predicates::str::{contains, is_empty, PredicateStrExt};
use std::process::Command;
use tempfile::TempDir;
use walkdir::WalkDir;

use tokio_uring;
use tokio;

// `kvs` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin("kvs").unwrap().assert().failure();
}

// `kvs -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs get <KEY>` should print "Key not found" for a non-existent key and exit with zero.
#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());
}

// `kvs rm <KEY>` should print "Key not found" for an empty database and exit with non-zero code.
#[test]
fn cli_rm_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stdout(eq("Key not found").trim());
}

// `kvs set <KEY> <VALUE>` should print nothing and exit with zero.
#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    dbg!(temp_dir.path());

    tokio_uring::start(async move {
        let mut store = KvStore::open(temp_dir.path()).await.unwrap();
        store.set("key1".into(), "value1".into()).await.unwrap();
        store.set("key2".into(), "value2".into()).await.unwrap();
        drop(store);

        Command::cargo_bin("kvs")
            .unwrap()
            .args(&["get", "key1"])
            .current_dir(&temp_dir)
            .assert()
            .success()
            .stdout(eq("value1").trim());

        Command::cargo_bin("kvs")
            .unwrap()
            .args(&["get", "key2"])
            .current_dir(&temp_dir)
            .assert()
            .success()
            .stdout(eq("value2").trim());

    });

    Ok(())
}

// `kvs rm <KEY>` should print nothing and exit with zero.
#[tokio::test]
async fn cli_rm_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path()).await?;
    store.set("key1".into(), "value1".into()).await?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());

    Ok(())
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}

// Should get previously stored value.
#[tokio::test]
async fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).await?;

    store.set("key1".into(), "value1".into()).await?;
    store.set("key2".into(), "value2".into()).await?;

    assert_eq!(store.get("key1".as_bytes()).await?, Some("value1".into()));
    assert_eq!(store.get("key2".as_bytes()).await?, Some("value2".into()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path()).await?;
    assert_eq!(store.get("key1".as_bytes()).await?, Some("value1".into()));
    assert_eq!(store.get("key2".as_bytes()).await?, Some("value2".into()));

    Ok(())
}

// Should overwrite existent value.
#[tokio::test]
async fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).await?;

    store.set("key1".into(), "value1".into()).await?;
    assert_eq!(store.get("key1".as_bytes()).await?, Some("value1".into()));
    store.set("key1".into(), "value2".into()).await?;
    assert_eq!(store.get("key1".as_bytes()).await?, Some("value2".into()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path()).await?;
    assert_eq!(store.get("key1".as_bytes()).await?, Some("value2".into()));
    store.set("key1".into(), "value3".into()).await?;
    assert_eq!(store.get("key1".as_bytes()).await?, Some("value3".into()));

    Ok(())
}

// Should get `None` when getting a non-existent key.
#[tokio::test]
async fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).await?;

    store.set("key1".into(), "value1".into()).await?;
    assert_eq!(store.get("key2".as_bytes()).await?, None);

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path()).await?;
    assert_eq!(store.get("key2".as_bytes()).await?, None);

    Ok(())
}

#[tokio::test]
async fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).await?;
    assert!(store.remove("key1".as_bytes()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).await?;
    store.set("key1".into(), "value1".into()).await?;
    assert!(store.remove("key1".as_bytes()).await.is_ok());
    assert_eq!(store.get("key1".as_bytes()).await?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[tokio::test]
#[ignore]
async fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).await?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut current_size = dir_size();
    for iter in 0..1000 {
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(key.into(), value.into()).await?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered.

        drop(store);
        // reopen and check content.
        let mut store = KvStore::open(temp_dir.path()).await?;
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key.as_bytes()).await?, Some(format!("{}", iter).into()));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}
