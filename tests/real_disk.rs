use std::path::PathBuf;

use quorumdb::{Db, Options, RealFs, SyncMode};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("quorumdb-test-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn survives_reopen_with_flushes_and_compaction() {
    let dir = temp_dir("reopen");
    let opts = Options {
        sync: SyncMode::Manual,
        memtable_bytes: 2048,
        compact_at: 3,
        ..Options::default()
    };
    {
        let mut db = Db::open(RealFs::open(&dir).unwrap(), opts.clone()).unwrap();
        for i in 0..2000u32 {
            db.put(
                format!("key{:05}", i % 700).as_bytes(),
                format!("value-{i}").as_bytes(),
            )
            .unwrap();
            if i % 5 == 0 {
                db.delete(format!("key{:05}", (i * 7) % 700).as_bytes())
                    .unwrap();
            }
        }
        db.sync().unwrap();
        assert!(db.stats().flushes > 0 && db.stats().compactions > 0);
    }
    let expected: Vec<_> = {
        let db = Db::open(RealFs::open(&dir).unwrap(), opts.clone()).unwrap();
        db.scan()
    };
    let db = Db::open(RealFs::open(&dir).unwrap(), opts).unwrap();
    assert_eq!(db.scan(), expected);
    assert_eq!(db.get(b"key00699").as_deref(), Some(&b"value-1399"[..]));
    let _ = std::fs::remove_dir_all(&dir);
}
