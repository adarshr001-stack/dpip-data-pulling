use anyhow::{Result, Context};
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use tokio_postgres::NoTls;

const CREATE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS file_transfers (
    id SERIAL PRIMARY KEY,
    sftp_file_name VARCHAR(1024) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    storage_file_name VARCHAR(1024),
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_file_status ON file_transfers(status);
CREATE INDEX IF NOT EXISTS idx_sftp_file_name ON file_transfers(sftp_file_name);
"#;

/// Database configuration
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub max_connections: usize,
}

/// Create a connection pool
pub async fn create_pool(config: &DbConfig) -> Result<Pool> {
    let mut pg_config = PoolConfig::new();
    pg_config.host = Some(config.host.clone());
    pg_config.port = Some(config.port);
    pg_config.user = Some(config.user.clone());
    pg_config.password = Some(config.password.clone());
    pg_config.dbname = Some(config.dbname.clone());
    
    let pool = pg_config
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .context("Failed to create database pool")?;
    
    println!("✓ Database connection pool created");
    Ok(pool)
}

/// Initialize database schema
pub async fn init_schema(pool: &Pool) -> Result<()> {
    let client = pool.get().await.context("Failed to get database client")?;
    
    client
        .batch_execute(CREATE_TABLE_SQL)
        .await
        .context("Failed to create database schema")?;
    
    println!("✓ Database schema initialized");
    Ok(())
}

/// Insert files as pending if they don't already exist
pub async fn insert_pending_files(pool: &Pool, file_paths: &[String]) -> Result<usize> {
    let client = pool.get().await.context("Failed to get database client")?;
    
    let mut inserted = 0;
    for file_path in file_paths {
        let result = client
            .execute(
                "INSERT INTO file_transfers (sftp_file_name, status, storage_file_name) 
                 VALUES ($1, 'pending', $1) 
                 ON CONFLICT (sftp_file_name) DO NOTHING",
                &[&file_path],
            )
            .await
            .context("Failed to insert file")?;
        
        inserted += result as usize;
    }
    
    println!("✓ Inserted {} new files as pending", inserted);
    Ok(inserted)
}

/// Reset failed files to pending status
pub async fn reset_failed_to_pending(pool: &Pool) -> Result<usize> {
    let client = pool.get().await.context("Failed to get database client")?;
    
    let count = client
        .execute(
            "UPDATE file_transfers SET status = 'pending', error = NULL, updated_at = NOW() WHERE status = 'failed' OR status = 'running'",
            &[],
        )
        .await
        .context("Failed to reset failed files")?;
    
    println!("✓ Reset {} failed files to pending", count);
    Ok(count as usize)
}

/// Claim next pending file atomically (changes status from pending to running)
/// Uses SELECT FOR UPDATE SKIP LOCKED to prevent race conditions between workers/processes
pub async fn claim_next_file(pool: &Pool) -> Result<Option<String>> {
    let mut client = pool.get().await.context("Failed to get database client")?;
    
    let transaction = client
        .transaction()
        .await
        .context("Failed to start transaction")?;
    
    // Find and lock a pending file
    let rows = transaction
        .query(
            "SELECT sftp_file_name FROM file_transfers 
             WHERE status = 'pending' OR status = 'failed'
             ORDER BY id 
             LIMIT 1 
             FOR UPDATE SKIP LOCKED",
            &[],
        )
        .await
        .context("Failed to select pending file")?;
    
    if rows.is_empty() {
        transaction.commit().await?;
        return Ok(None);
    }
    
    let file_path: String = rows[0].get(0);
    
    // Update status to running
    transaction
        .execute(
            "UPDATE file_transfers 
             SET status = 'running', updated_at = NOW() 
             WHERE sftp_file_name = $1",
            &[&file_path],
        )
        .await
        .context("Failed to update file status to running")?;
    
    transaction.commit().await?;
    
    Ok(Some(file_path))
}

/// Mark file as completed
pub async fn mark_completed(pool: &Pool, sftp_file_name: &str, storage_file_name: &str) -> Result<()> {
    let client = pool.get().await.context("Failed to get database client")?;
    
    client
        .execute(
            "UPDATE file_transfers 
             SET status = 'completed', storage_file_name = $1, updated_at = NOW() 
             WHERE sftp_file_name = $2",
            &[&storage_file_name, &sftp_file_name],
        )
        .await
        .context("Failed to mark file as completed")?;
    
    Ok(())
}

/// Mark file as failed with error message
pub async fn mark_failed(pool: &Pool, sftp_file_name: &str, error_msg: &str) -> Result<()> {
    let client = pool.get().await.context("Failed to get database client")?;
    
    client
        .execute(
            "UPDATE file_transfers 
             SET status = 'failed', error = $1, updated_at = NOW() 
             WHERE sftp_file_name = $2",
            &[&error_msg, &sftp_file_name],
        )
        .await
        .context("Failed to mark file as failed")?;
    
    Ok(())
}

/// Get statistics of file transfers
pub async fn get_stats(pool: &Pool) -> Result<(usize, usize, usize, usize)> {
    let client = pool.get().await.context("Failed to get database client")?;
    
    let row = client
        .query_one(
            "SELECT 
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'running') as running,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed
             FROM file_transfers",
            &[],
        )
        .await
        .context("Failed to get statistics")?;
    
    let pending: i64 = row.get(0);
    let running: i64 = row.get(1);
    let completed: i64 = row.get(2);
    let failed: i64 = row.get(3);
    
    Ok((pending as usize, running as usize, completed as usize, failed as usize))
}
