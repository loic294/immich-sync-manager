//! Guarded direct writes to the Immich v3.4.1 database.
//!
//! Immich has no API to change the path or owner of an external-library asset, so these few
//! statements do it. Every write:
//! - runs in a transaction that locks the row (`FOR UPDATE`),
//! - checks the row still has the path/owner we planned against (else nothing changes),
//! - refuses to create a second asset with the same path.
//!
//! Ids are handled as text and cast in SQL, so this module does not depend on a UUID type.

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{PgConnection, Row};
use thiserror::Error;

use ism_core::checksum::immich_path_checksum;

#[derive(Debug, Error)]
pub enum PgError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("asset {0} changed since it was planned")]
    Stale(String),
    #[error("another Immich asset already uses {0}")]
    PathTaken(String),
}

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(url)
        .await
}

/// Name of the newest applied Immich migration.
pub async fn latest_migration(pool: &PgPool) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT name FROM kysely_migrations ORDER BY name DESC LIMIT 1")
        .fetch_optional(pool)
        .await
}

#[derive(Debug, Clone)]
pub struct AssetRow {
    pub id: String,
    pub owner_id: String,
    pub library_id: Option<String>,
    pub original_path: String,
    pub live_photo_video_id: Option<String>,
    pub stack_id: Option<String>,
    pub duplicate_id: Option<String>,
    pub trashed: bool,
}

const ASSET_COLUMNS: &str = r#"id::text AS id, "ownerId"::text AS owner_id, "libraryId"::text AS library_id,
    "originalPath" AS original_path,
    "livePhotoVideoId"::text AS live_photo_video_id, "stackId"::text AS stack_id,
    "duplicateId"::text AS duplicate_id, ("deletedAt" IS NOT NULL) AS trashed"#;

fn asset_row(row: &sqlx::postgres::PgRow) -> Result<AssetRow, sqlx::Error> {
    Ok(AssetRow {
        id: row.try_get("id")?,
        owner_id: row.try_get("owner_id")?,
        library_id: row.try_get("library_id")?,
        original_path: row.try_get("original_path")?,
        live_photo_video_id: row.try_get("live_photo_video_id")?,
        stack_id: row.try_get("stack_id")?,
        duplicate_id: row.try_get("duplicate_id")?,
        trashed: row.try_get("trashed")?,
    })
}

pub async fn asset_by_path(
    conn: &mut PgConnection,
    path: &str,
) -> Result<Option<AssetRow>, sqlx::Error> {
    let sql = format!(r#"SELECT {ASSET_COLUMNS} FROM asset WHERE "originalPath" = $1 LIMIT 1"#);
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(path)
        .fetch_optional(&mut *conn)
        .await?;
    row.as_ref().map(asset_row).transpose()
}

pub async fn asset_by_id(
    conn: &mut PgConnection,
    id: &str,
) -> Result<Option<AssetRow>, sqlx::Error> {
    let sql = format!(r#"SELECT {ASSET_COLUMNS} FROM asset WHERE id = $1::uuid"#);
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;
    row.as_ref().map(asset_row).transpose()
}

fn like_prefix(prefix: &str) -> String {
    let escaped = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("{}/%", escaped.trim_end_matches('/'))
}

/// Assets whose original lies below `folder` (an absolute Immich path).
pub async fn assets_under(
    conn: &mut PgConnection,
    folder: &str,
) -> Result<Vec<AssetRow>, sqlx::Error> {
    let sql =
        format!(r#"SELECT {ASSET_COLUMNS} FROM asset WHERE "originalPath" LIKE $1 ESCAPE '\'"#);
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(like_prefix(folder))
        .fetch_all(&mut *conn)
        .await?;
    rows.iter().map(asset_row).collect()
}

#[derive(Debug, Clone)]
pub struct SidecarRow {
    pub id: String,
    pub asset_id: String,
    pub path: String,
}

pub async fn sidecar_by_path(
    conn: &mut PgConnection,
    path: &str,
) -> Result<Option<SidecarRow>, sqlx::Error> {
    let row = sqlx::query(
        r#"SELECT id::text AS id, "assetId"::text AS asset_id, path FROM asset_file
           WHERE type = 'sidecar' AND path = $1 LIMIT 1"#,
    )
    .bind(path)
    .fetch_optional(&mut *conn)
    .await?;
    row.map(|r| {
        Ok(SidecarRow {
            id: r.try_get("id")?,
            asset_id: r.try_get("asset_id")?,
            path: r.try_get("path")?,
        })
    })
    .transpose()
}

pub async fn sidecars_under(
    conn: &mut PgConnection,
    folder: &str,
) -> Result<Vec<SidecarRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"SELECT id::text AS id, "assetId"::text AS asset_id, path FROM asset_file
           WHERE type = 'sidecar' AND path LIKE $1 ESCAPE '\'"#,
    )
    .bind(like_prefix(folder))
    .fetch_all(&mut *conn)
    .await?;
    rows.iter()
        .map(|r| {
            Ok(SidecarRow {
                id: r.try_get("id")?,
                asset_id: r.try_get("asset_id")?,
                path: r.try_get("path")?,
            })
        })
        .collect()
}

pub fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Moves one asset's original path. Path-derived checksums follow the new path.
pub async fn move_asset(
    conn: &mut PgConnection,
    asset_id: &str,
    expected_path: &str,
    new_path: &str,
) -> Result<(), PgError> {
    let locked: Option<(String, String)> = sqlx::query_as(
        r#"SELECT "originalPath", "checksumAlgorithm"::text FROM asset WHERE id = $1::uuid FOR UPDATE"#,
    )
    .bind(asset_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((current, algorithm)) = locked else {
        return Err(PgError::Stale(asset_id.to_owned()));
    };
    if current != expected_path {
        return Err(PgError::Stale(asset_id.to_owned()));
    }
    let taken: Option<i32> = sqlx::query_scalar(
        r#"SELECT 1 FROM asset WHERE "originalPath" = $1 AND id <> $2::uuid LIMIT 1"#,
    )
    .bind(new_path)
    .bind(asset_id)
    .fetch_optional(&mut *conn)
    .await?;
    if taken.is_some() {
        return Err(PgError::PathTaken(new_path.to_owned()));
    }
    let checksum = (algorithm == "sha1-path").then(|| immich_path_checksum(new_path));
    sqlx::query(
        r#"UPDATE asset SET "originalPath" = $2, "originalFileName" = $3,
           checksum = COALESCE($4, checksum) WHERE id = $1::uuid"#,
    )
    .bind(asset_id)
    .bind(new_path)
    .bind(file_name(new_path))
    .bind(checksum)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn move_sidecar(
    conn: &mut PgConnection,
    file_id: &str,
    expected_path: &str,
    new_path: &str,
) -> Result<(), PgError> {
    let result =
        sqlx::query(r#"UPDATE asset_file SET path = $3 WHERE id = $1::uuid AND path = $2"#)
            .bind(file_id)
            .bind(expected_path)
            .bind(new_path)
            .execute(&mut *conn)
            .await?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PgError::Stale(file_id.to_owned()))
    }
}

/// Tables referencing `asset.id` whose rows do not depend on who owns the asset.
const OWNER_NEUTRAL_TABLES: &[&str] = &[
    "asset_file",
    "asset_exif",
    "asset_job_status",
    "smart_search",
    "asset_metadata",
    "asset_ocr",
    "ocr_search",
    "asset_edit",
];

/// Reasons why moving `asset` to another owner could lose or leak associations. Empty means the
/// transfer is provably safe. Any table referencing assets that is not known to be owner-neutral
/// blocks the transfer if it has rows for this asset (fail closed on unknown schema).
pub async fn transfer_blockers(
    conn: &mut PgConnection,
    asset: &AssetRow,
) -> Result<Vec<String>, sqlx::Error> {
    let mut blockers = Vec::new();
    if asset.stack_id.is_some() {
        blockers.push("asset is part of a stack".to_owned());
    }
    if asset.duplicate_id.is_some() {
        blockers.push("asset is in a duplicate group".to_owned());
    }
    if asset.live_photo_video_id.is_some() {
        blockers.push("asset is a live photo with a linked video".to_owned());
    }
    let references: Vec<(String, String)> = sqlx::query_as(
        r#"SELECT cl.relname::text, att.attname::text
           FROM pg_constraint con
           JOIN pg_class cl ON cl.oid = con.conrelid
           JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
           WHERE con.contype = 'f' AND con.confrelid = 'asset'::regclass"#,
    )
    .fetch_all(&mut *conn)
    .await?;
    for (table, column) in references {
        if OWNER_NEUTRAL_TABLES.contains(&table.as_str()) {
            continue;
        }
        let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let mut sql = format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE {} = $1::uuid",
            quote(&table),
            quote(&column)
        );
        if table == "asset_face" {
            sql.push_str(r#" AND "personId" IS NOT NULL"#);
        }
        sql.push(')');
        let exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(&asset.id)
            .fetch_one(&mut *conn)
            .await?;
        if exists {
            blockers.push(match table.as_str() {
                "asset_face" => "asset has faces assigned to people".to_owned(),
                "album_asset" => "asset is in an album".to_owned(),
                "tag_asset" => "asset has tags".to_owned(),
                "memory_asset" => "asset is in a memory".to_owned(),
                "shared_link_asset" => "asset is in a shared link".to_owned(),
                "asset" => "another asset links to this asset".to_owned(),
                other => format!("asset is referenced by {other}.{column}"),
            });
        }
    }
    Ok(blockers)
}

/// Gives an asset to another library and, when they differ, another user. The previous owner gets
/// an audit entry so their clients drop the asset on the next sync.
pub async fn transfer_owner(
    conn: &mut PgConnection,
    asset_id: &str,
    from_owner: &str,
    to_owner: &str,
    to_library: &str,
) -> Result<(), PgError> {
    let result = sqlx::query(
        r#"UPDATE asset SET "ownerId" = $3::uuid, "libraryId" = $4::uuid
           WHERE id = $1::uuid AND "ownerId" = $2::uuid"#,
    )
    .bind(asset_id)
    .bind(from_owner)
    .bind(to_owner)
    .bind(to_library)
    .execute(&mut *conn)
    .await?;
    if result.rows_affected() != 1 {
        return Err(PgError::Stale(asset_id.to_owned()));
    }
    if from_owner == to_owner {
        return Ok(());
    }
    sqlx::query(r#"INSERT INTO asset_audit ("assetId", "ownerId") VALUES ($1::uuid, $2::uuid)"#)
        .bind(asset_id)
        .bind(from_owner)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_prefixes_escape_wildcards() {
        assert_eq!(like_prefix("/external/a_b%"), "/external/a\\_b\\%/%");
        assert_eq!(like_prefix("/external/x/"), "/external/x/%");
        assert_eq!(file_name("/a/b/c.jpg"), "c.jpg");
    }
}
